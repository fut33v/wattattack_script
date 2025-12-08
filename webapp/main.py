"""FastAPI backend for the Крутилка admin portal."""
from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Depends, FastAPI, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer
from repositories import instructors_repository, message_repository, schedule_repository
from starlette.middleware.sessions import SessionMiddleware

from .config import get_settings
from .dependencies import (
    get_current_user,
    is_admin_user,
    require_admin,
)
from .routes.activities import router as activities_router
from .routes.bikes import router as bikes_router
from .routes.admins import router as admins_router
from .routes.core import api_router as core_router, public_router as public_core_router
from .routes.clients import router as clients_router
from .routes.messaging import ensure_uploads_dir, router as messaging_router, UPLOADS_DIR as MESSAGING_UPLOADS_DIR
from .routes.client_links import router as client_links_router
from .routes.vk_client_links import router as vk_client_links_router
from .routes.intervals_links import router as intervals_links_router
from .routes.trainers import router as trainers_router
from .routes.races import router as races_router, public_router as public_races_router
from .routes.strava import router as strava_router
from .routes.leaderboard import router as leaderboard_router
from .routes.public_leaderboard import router as public_leaderboard_router
from .routes.sync import router as sync_router
from .routes.schedule import router as schedule_router, public_router as public_schedule_router
from .routes.schedule_slots import router as schedule_slots_router
from .routes.backup import router as backup_router
from .routes.stats import router as stats_router
from .routes.wattattack_accounts import router as wattattack_accounts_router
from .routes.groups import router as groups_router
from .routes.pulse import router as pulse_router


BASE_DIR = Path(__file__).parent
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"
TEMPLATES_DIR = BASE_DIR / "templates"
log = logging.getLogger(__name__)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _build_base_url(request: Request) -> str:
    settings = get_settings()
    base = settings.public_url or settings.base_url or str(request.base_url)
    return base.rstrip("/")


def _store_uploaded_image(image_upload: UploadFile, *, request: Request, image_bytes: bytes) -> str:
    """Persist uploaded image and return absolute URL for Telegram."""
    target_dir = UPLOADS_DIR / "messaging"
    target_dir.mkdir(parents=True, exist_ok=True)

    original_suffix = Path(image_upload.filename or "").suffix.lower()
    safe_suffix = original_suffix if original_suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp"} else ".jpg"
    filename = f"{uuid4().hex}{safe_suffix}"
    destination = target_dir / filename
    destination.write_bytes(image_bytes)

    public_url = f"{_build_base_url(request)}/uploads/messaging/{filename}"
    log.info("messaging: stored image %s (%s bytes) -> %s", destination, len(image_bytes), public_url)
    return public_url


api = APIRouter(prefix="/api", tags=["api"])
api.include_router(activities_router)
api.include_router(clients_router)
api.include_router(sync_router)
api.include_router(leaderboard_router)
api.include_router(bikes_router)
api.include_router(admins_router)
api.include_router(core_router)
api.include_router(client_links_router, dependencies=[Depends(require_admin)])
api.include_router(vk_client_links_router, dependencies=[Depends(require_admin)])
api.include_router(intervals_links_router, dependencies=[Depends(require_admin)])
api.include_router(races_router)
api.include_router(trainers_router)
api.include_router(messaging_router)
api.include_router(schedule_router)
api.include_router(schedule_slots_router)
api.include_router(strava_router)
api.include_router(backup_router)
api.include_router(stats_router)
api.include_router(wattattack_accounts_router)
api.include_router(groups_router)
api.include_router(pulse_router)


def _apply_dev_favicon(html: str, *, enabled: bool) -> str:
    if not enabled:
        return html
    return html.replace("/app/logo.png", "/img/logo_black.png")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(title="Крутилка Admin API")
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret_key,
        same_site="lax",
        https_only=False,
    )
    app.include_router(api)
    app.include_router(public_schedule_router)
    app.include_router(public_races_router)
    app.include_router(public_core_router)
    app.include_router(public_leaderboard_router)

    @app.on_event("startup")
    def _startup_seed_instructors() -> None:
        """Ensure instructor directory exists before first request."""
        try:
            instructors_repository.ensure_instructors_table()
            message_repository.ensure_user_messages_table()
            ensure_uploads_dir()
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("Failed to ensure instructors table on startup: %s", exc)

    @app.get("/")
    def root():
        return RedirectResponse(url="/app", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    dist_root = FRONTEND_DIST.resolve()
    assets_dir = dist_root / "assets"
    if assets_dir.exists():
        app.mount("/app/assets", StaticFiles(directory=str(assets_dir), html=False), name="frontend-assets")

    images_dir = BASE_DIR.parent / "img"
    dev_favicon_path = images_dir / "logo_black.png"
    dev_favicon_enabled = False
    if images_dir.exists():
        app.mount("/img", StaticFiles(directory=str(images_dir), html=False), name="static-images")
    if settings.dev_build:
        if dev_favicon_path.exists():
            dev_favicon_enabled = True
        else:
            log.warning("DEV_BUILD is set but %s is missing; using default favicon", dev_favicon_path)

    if MESSAGING_UPLOADS_DIR.exists():
        app.mount("/uploads", StaticFiles(directory=str(MESSAGING_UPLOADS_DIR), html=False), name="uploads")
    try:
        fit_dir = schedule_repository.ensure_fit_files_dir()
        if fit_dir.exists():
            app.mount("/fitfiles", StaticFiles(directory=str(fit_dir), html=False), name="fitfiles")
    except Exception:
        log.exception("Failed to mount FIT files directory")

    index_file = dist_root / "index.html"

    @lru_cache()
    def _index_html() -> str | None:
        if not index_file.exists():
            return None
        try:
            return index_file.read_text(encoding="utf-8")
        except Exception:  # pylint: disable=broad-except
            return None

    def _spa_response_for(request: Request) -> Response:
        html = _index_html()
        if html is None:
            return JSONResponse(
                {
                    "detail": "Frontend is not built yet. Run npm install && npm run build inside webapp/frontend.",
                },
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        html = _apply_dev_favicon(html, enabled=dev_favicon_enabled)
        return HTMLResponse(html)

    @app.get("/app", response_class=HTMLResponse)
    def serve_spa_root(request: Request):
        user = get_current_user(request)
        if not user:
            return _spa_response_for(request)

        if is_admin_user(user):
            return _spa_response_for(request)

        context = {
            "request": request,
        }
        return templates.TemplateResponse("non_admin_placeholder.html", context)

    @app.get("/app/{path:path}")
    def serve_spa_asset(request: Request, path: str):
        user = get_current_user(request)
        if not user:
            target = (dist_root / path).resolve()
            if dist_root in target.parents or target == dist_root:
                if target.is_file():
                    return FileResponse(target)
            return _spa_response_for(request)

        if not is_admin_user(user):
            context = {
                "request": request,
            }
            return templates.TemplateResponse("non_admin_placeholder.html", context)

        target = (dist_root / path).resolve()
        if dist_root in target.parents or target == dist_root:
            if target.is_file():
                return FileResponse(target)
        return _spa_response_for(request)

    return app


app = create_app()
