"""FastAPI backend for the Крутилка admin portal."""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import psycopg2
import requests
from straver_client import StraverClient
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Form,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer
from repositories import (
    admin_repository,
    bikes_repository,
    client_link_repository,
    client_repository,
    intervals_link_repository,
    instructors_repository,
    layout_repository,
    message_repository,
    race_repository,
    trainers_repository,
    schedule_repository,
)
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import PlainTextResponse

from .auth import TelegramAuthError, verify_telegram_payload
from .config import get_settings
from .dependencies import (
    SESSION_KEY_USER,
    get_current_user,
    is_admin_user,
    require_admin,
    require_user,
)
from .routes.activities import router as activities_router
from .routes.clients import router as clients_router
from .routes.messaging import ensure_uploads_dir, router as messaging_router, UPLOADS_DIR as MESSAGING_UPLOADS_DIR
from .routes.client_links import router as client_links_router
from .routes.vk_client_links import router as vk_client_links_router
from .routes.intervals_links import router as intervals_links_router
from .routes.races import router as races_router, _format_race_date_label
from .routes.leaderboard import router as leaderboard_router
from .routes.public_leaderboard import router as public_leaderboard_router
from .routes.sync import router as sync_router
from .routes.schedule import router as schedule_router, public_router as public_schedule_router
from .routes.schedule_slots import router as schedule_slots_router


BASE_DIR = Path(__file__).parent
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"
TEMPLATES_DIR = BASE_DIR / "templates"
log = logging.getLogger(__name__)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _safe_next(next_param: Optional[str]) -> str:
    if not next_param:
        return "/app"
    if next_param.startswith("/"):
        return next_param
    return "/app"


def _json_success(payload: dict) -> JSONResponse:
    return JSONResponse(payload)


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
api.include_router(client_links_router, dependencies=[Depends(require_admin)])
api.include_router(vk_client_links_router, dependencies=[Depends(require_admin)])
api.include_router(intervals_links_router, dependencies=[Depends(require_admin)])
api.include_router(races_router)
api.include_router(messaging_router)
api.include_router(schedule_router)
api.include_router(schedule_slots_router)

@api.get("/config")
def api_config():
    settings = get_settings()
    return {
        "loginBotUsername": settings.telegram_login_bot_username,
        "clientsPageSize": settings.clients_page_size,
        "baseUrl": settings.base_url,
    }


@api.get("/session")
def api_session(user=Depends(require_user)):
    return {
        "user": jsonable_encoder(user.to_dict()),
        "isAdmin": is_admin_user(user),
    }


@api.get("/summary")
def api_summary(user=Depends(require_user)):
    return {
        "clients": client_repository.count_clients(),
        "bikes": bikes_repository.bikes_count(),
        "trainers": trainers_repository.trainers_count(),
        "admins": len(admin_repository.list_admins()),
        "links": len(client_link_repository.list_links()),
    }




@api.get("/bikes")
def api_bikes(user=Depends(require_user)):
    rows = bikes_repository.list_bikes()
    return _json_success({"items": jsonable_encoder(rows)})


@api.post("/bikes")
async def api_create_bike(request: Request, user=Depends(require_admin)):
    payload = await request.json()

    title = payload.get("title")
    if not isinstance(title, str) or not title.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Title is required")

    def _clean_str(value: object, *, allowed: set[str] | None = None) -> str | None:
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                return None
            if allowed is not None and trimmed not in allowed:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid value")
            return trimmed
        if value is None:
            return None
        if allowed is not None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid value")
        return None

    def _parse_height(value: object, field: str) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            normalized = stripped.replace(",", ".")
            try:
                return float(normalized)
            except ValueError as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}") from exc
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")

    try:
        record = bikes_repository.create_bike(
            title=title.strip(),
            owner=_clean_str(payload.get("owner")),
            size_label=_clean_str(payload.get("size_label")),
            frame_size_cm=_clean_str(payload.get("frame_size_cm")),
            height_min_cm=_parse_height(payload.get("height_min_cm"), "height_min_cm"),
            height_max_cm=_parse_height(payload.get("height_max_cm"), "height_max_cm"),
            gears=_clean_str(payload.get("gears")),
            axle_type=_clean_str(payload.get("axle_type"), allowed={"ЭКС", "ОСЬ"}),
            cassette=_clean_str(payload.get("cassette"), allowed={"7", "8", "9", "10", "11", "12"}),
        )
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Bike with this title already exists") from exc

    return {"item": jsonable_encoder(record)}


@api.patch("/bikes/{bike_id}")
async def api_update_bike(bike_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    updates: dict[str, object] = {}

    for key in ("height_min_cm", "height_max_cm"):
        if key in payload:
            value = payload[key]
            updates[key] = value

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    bikes_repository.update_bike_fields(bike_id, **updates)
    record = bikes_repository.get_bike(bike_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Bike not found")
    return {"item": jsonable_encoder(record)}


@api.get("/trainers")
def api_trainers(user=Depends(require_user)):
    rows = trainers_repository.list_trainers()
    return _json_success({"items": jsonable_encoder(rows)})


@api.patch("/trainers/{trainer_id}")
async def api_update_trainer(trainer_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    updates: dict[str, object] = {}
    for key in ("title", "display_name", "owner", "axle_types", "cassette"):
        if key in payload:
            updates[key] = payload[key]

    bike_assignment_handled = False
    if "bike_id" in payload:
        bike_assignment_handled = True
        bike_value = payload["bike_id"]
        if isinstance(bike_value, str):
            bike_value = bike_value.strip()
        if bike_value in (None, "", "null"):
            layout_repository.ensure_layout_table()
            layout_repository.clear_bike_assignment_for_stand(trainer_id)
        else:
            try:
                bike_id = int(bike_value)
            except (TypeError, ValueError):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid bike_id")
            bike_record = bikes_repository.get_bike(bike_id)
            if not bike_record:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Bike not found")
            layout_repository.ensure_layout_table()
            layout_repository.set_bike_assignment(trainer_id, bike_id, assigned_by=getattr(user, "id", None))

    for key, value in list(updates.items()):
        if isinstance(value, str):
            trimmed = value.strip()
            updates[key] = trimmed or None
        elif value in ("", None):
            updates[key] = None

    if not updates and not bike_assignment_handled:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    if updates:
        trainers_repository.update_trainer_fields(trainer_id, **updates)

    record = trainers_repository.get_trainer(trainer_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Trainer not found")
    return {"item": jsonable_encoder(record)}


@api.get("/instructors")
def api_instructors(user=Depends(require_user)):
    rows = instructors_repository.list_instructors()
    return _json_success({"items": jsonable_encoder(rows)})


@api.post("/instructors")
async def api_create_instructor(request: Request, user=Depends(require_admin)):
    payload = await request.json()
    name = payload.get("full_name") or payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "full_name is required")
    try:
        record = instructors_repository.create_instructor(name.strip())
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to create instructor") from exc
    return {"item": jsonable_encoder(record)}


@api.delete("/instructors/{instructor_id}")
def api_delete_instructor(instructor_id: int, user=Depends(require_admin)):
    deleted = instructors_repository.delete_instructor(instructor_id)
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Instructor not found")
    return {"status": "ok"}


@api.get("/admins")
def api_admins(user=Depends(require_admin)):
    rows = admin_repository.list_admins()
    return _json_success({"items": jsonable_encoder(rows)})


@api.post("/admins")
async def api_add_admin(request: Request, user=Depends(require_admin)):
    payload = await request.json()
    tg_id = payload.get("tg_id")
    username = payload.get("username")
    display_name = payload.get("display_name")

    if tg_id is None and not username:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Username or tg_id required")

    if tg_id is not None:
        try:
            tg_id = int(tg_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid tg_id") from exc
    if isinstance(username, str):
        username = username.strip() or None
    if isinstance(display_name, str):
        display_name = display_name.strip() or None

    _, record = admin_repository.add_admin(
        tg_id=tg_id,
        username=username,
        display_name=display_name,
    )
    return {"item": jsonable_encoder(record)}


@api.delete("/admins/{admin_id}")
def api_remove_admin(
    admin_id: int,
    tg_id: Optional[int] = None,
    username: Optional[str] = None,
    user=Depends(require_admin),
):
    if tg_id is None and (username is None or username == ""):
        # fallback: find admin by id to determine identifiers
        for row in admin_repository.list_admins():
            if row.get("id") == admin_id:
                tg_id = row.get("tg_id")
                username = row.get("username")
                break
    admin_repository.remove_admin(tg_id=tg_id, username=username)
    return {"status": "ok"}


@api.get("/strava/authorize")
def strava_authorize(state: str = None, user=Depends(require_admin)):
    """Generate Strava authorization URL for a client."""
    try:
        straver = StraverClient()
        if not straver.is_configured():
            raise RuntimeError("STRAVER_BASE_URL/STRAVER_INTERNAL_SECRET are not configured")
        auth_url = straver.build_authorize_url(state=state or "")
        return RedirectResponse(url=auth_url)
    except Exception as exc:
        log.exception("Failed to generate Strava authorization URL")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to generate Strava authorization URL") from exc


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

    @app.get("/race/{slug}", response_class=HTMLResponse)
    def public_race_page(slug: str, request: Request):
        context = {"request": request}
        race = race_repository.get_race_by_slug(slug)
        if not race:
            context["error"] = "Гонка не найдена или ещё не опубликована."
            return templates.TemplateResponse("public_race.html", context)

        race_date_raw = race.get("race_date")
        if isinstance(race_date_raw, date):
            race_date = race_date_raw
        elif isinstance(race_date_raw, str):
            try:
                race_date = datetime.strptime(race_date_raw, "%Y-%m-%d").date()
            except ValueError:
                race_date = None
        else:
            race_date = None

        registrations = race_repository.list_registrations(race["id"])
        participants: list[dict] = []
        pending_count = 0
        clusters_meta = race.get("clusters") or []
        cluster_times: dict[str, dict[str, Optional[str]]] = {}
        cluster_order_keys: list[tuple] = []
        for cluster in clusters_meta:
            label = (cluster.get("label") or cluster.get("code") or "").strip()
            start_time = (cluster.get("start_time") or "").strip() or None
            end_time = (cluster.get("end_time") or "").strip() or None
            if label:
                cluster_times[label] = {
                    "start_time": start_time,
                    "end_time": end_time,
                }
                try:
                    start_key = datetime.strptime(start_time, "%H:%M").time() if start_time else None
                except ValueError:
                    start_key = None
                cluster_order_keys.append((start_key is None, start_key, label.lower(), label))

        ordered_cluster_labels = [item[3] for item in sorted(cluster_order_keys, key=lambda entry: entry)]

        client_stand_map: dict[int, str] = {}
        if race_date:
            week = schedule_repository.get_week_by_start(race_date)
            if week:
                slots = schedule_repository.list_slots_with_reservations(week["id"])
                trainers = trainers_repository.list_trainers()
                trainers_map = {row["id"]: row for row in trainers if isinstance(row.get("id"), int)}

                for slot in slots:
                    slot_date = slot.get("slot_date")
                    if slot_date and str(slot_date) != race_date.isoformat():
                        continue
                    if slot.get("session_kind") != "race":
                        continue
                    for res in slot.get("reservations") or []:
                        if res.get("status") != "booked":
                            continue
                        client_id = res.get("client_id")
                        if not isinstance(client_id, int):
                            continue
                        label_parts: list[str] = []
                        stand_id = res.get("stand_id")
                        trainer = trainers_map.get(stand_id)
                        stand_code = (res.get("stand_code") or "").strip()
                        if trainer:
                            code = (trainer.get("code") or "").strip()
                            if code:
                                label_parts.append(code)
                        if not label_parts and stand_code:
                            label_parts.append(stand_code)
                        if not label_parts and stand_id is not None:
                            label_parts.append(f"Станок {stand_id}")
                        if label_parts:
                            client_stand_map[client_id] = " · ".join(label_parts)

        for entry in registrations:
            status_value = (entry.get("status") or "").lower()
            stand_label = None
            client_id = entry.get("client_id")
            if isinstance(client_id, int):
                stand_label = client_stand_map.get(client_id)
            payload = {
                "name": entry.get("client_name")
                or entry.get("tg_full_name")
                or (f"@{entry.get('tg_username')}" if entry.get("tg_username") else f"ID {entry.get('client_id')}"),
                "cluster": entry.get("cluster_label"),
                "notes": entry.get("notes"),
                "is_pending": False,
                "race_mode": entry.get("race_mode"),
                "stand_label": stand_label,
            }
            submitted = entry.get("payment_submitted_at")
            if hasattr(submitted, "isoformat"):
                payload["submitted"] = submitted.isoformat()
            elif submitted:
                payload["submitted"] = str(submitted)
            if status_value == race_repository.RACE_STATUS_APPROVED:
                participants.append(payload)
            elif status_value == race_repository.RACE_STATUS_PENDING:
                payload["is_pending"] = True
                pending_count += 1
                participants.append(payload)

        price_value = race.get("price_rub")
        price_label = (
            f"{int(price_value):,}".replace(",", " ") if isinstance(price_value, (int, float)) else None
        )

        description_raw = (race.get("description") or "").strip()
        if description_raw:
            description_formatted = "<br>".join(description_raw.splitlines())
        else:
            description_formatted = None

        grouped_participants_offline: list[dict] = []
        grouped_participants_online: list[dict] = []
        groups_map_offline: dict[str, list] = {}
        groups_map_online: dict[str, list] = {}
        unassigned_label = "Кластер не назначен"
        for item in participants:
            mode_key = (item.get("race_mode") or "").strip().lower()
            target_map = groups_map_offline if mode_key != "online" else groups_map_online
            label = (item.get("cluster") or "").strip() or unassigned_label
            target_map.setdefault(label, []).append(item)

        for label in ordered_cluster_labels:
            if label in groups_map_offline:
                members = groups_map_offline.pop(label)
                members.sort(key=lambda x: ((x.get("stand_label") or "станокzzz").lower(), x.get("name") or ""))
                grouped_participants_offline.append({"label": label, "participants": members})
        for label in sorted(groups_map_offline.keys()):
            members = groups_map_offline[label]
            members.sort(key=lambda x: ((x.get("stand_label") or "станокzzz").lower(), x.get("name") or ""))
            grouped_participants_offline.append({"label": label, "participants": members})

        for label in ordered_cluster_labels:
            if label in groups_map_online:
                members = groups_map_online.pop(label)
                members.sort(key=lambda x: (x.get("name") or ""))
                grouped_participants_online.append({"label": label, "participants": members})
        for label in sorted(groups_map_online.keys()):
            members = groups_map_online[label]
            members.sort(key=lambda x: (x.get("name") or ""))
            grouped_participants_online.append({"label": label, "participants": members})

        race_payload = {
            "title": race.get("title"),
            "date_label": _format_race_date_label(race.get("race_date")),
            "price_label": price_label,
            "sbp_phone": race.get("sbp_phone"),
            "payment_instructions": race.get("payment_instructions"),
            "notes": race.get("notes"),
            "description": description_formatted,
            "slug": race.get("slug"),
            "is_active": race.get("is_active"),
            "clusters": race.get("clusters") or [],
        }

        share_url = f"{request.url.scheme}://{request.url.netloc}/race/{race_payload['slug']}"

        context.update(
            {
                "race": race_payload,
                "participants": participants,
                "participant_groups_offline": grouped_participants_offline,
                "participant_groups_online": grouped_participants_online,
                "participants_count": len(participants),
                "pending_count": pending_count,
                "share_url": share_url,
                "cluster_times": cluster_times,
            }
        )
        return templates.TemplateResponse("public_race.html", context)

    @app.get("/race")
    def race_default_redirect():
        return RedirectResponse(url="/schedule", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    @app.get("/auth/telegram")
    async def telegram_auth(request: Request, next: Optional[str] = None):
        settings = get_settings()
        allowed_keys = {
            "id",
            "first_name",
            "last_name",
            "username",
            "photo_url",
            "auth_date",
            "hash",
        }
        payload = {key: value for key, value in request.query_params.items() if key in allowed_keys}

        try:
            user = verify_telegram_payload(
                payload,
                bot_token=settings.telegram_login_bot_token,
            )
        except TelegramAuthError as exc:
            target = _safe_next(next)
            separator = "&" if "?" in target else "?"
            target = f"{target}{separator}error={quote(str(exc))}"
            return RedirectResponse(url=target, status_code=status.HTTP_303_SEE_OTHER)

        request.session[SESSION_KEY_USER] = user.to_dict()
        target = _safe_next(next)
        return RedirectResponse(url=target, status_code=status.HTTP_303_SEE_OTHER)

    @app.get("/strava/callback")
    async def strava_oauth_callback(request: Request, code: str = None, error: str = None, state: str = None):
        """Forward Strava OAuth callback to the Straver service."""
        try:
            straver = StraverClient()
            if not straver.base_url:
                raise RuntimeError("STRAVER_BASE_URL is not configured")
            forward_url = f"{straver.base_url}/strava/callback"
            if request.url.query:
                forward_url = f"{forward_url}?{request.url.query}"
            return RedirectResponse(url=forward_url)
        except Exception as exc:
            log.exception("Failed to forward Strava callback")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Strava callback is now handled by Straver: {exc}",
            ) from exc

    @app.get("/api/health")
    def api_health():
        return {"status": "ok"}

    @app.get("/logout")
    def logout(request: Request):
        request.session.pop(SESSION_KEY_USER, None)
        return RedirectResponse(url="/app", status_code=status.HTTP_303_SEE_OTHER)

    @app.get("/api/logout")
    def api_logout(request: Request):
        request.session.pop(SESSION_KEY_USER, None)
        return {"status": "ok"}

    dist_root = FRONTEND_DIST.resolve()
    assets_dir = dist_root / "assets"
    if assets_dir.exists():
        app.mount("/app/assets", StaticFiles(directory=str(assets_dir), html=False), name="frontend-assets")

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
