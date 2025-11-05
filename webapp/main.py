"""FastAPI backend for the Крутилка admin portal."""
from __future__ import annotations

import math
from functools import lru_cache
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import psycopg2
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    HTTPException,
    Request,
    status,
)
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from repositories import (
    admin_repository,
    bikes_repository,
    client_link_repository,
    client_repository,
    layout_repository,
    trainers_repository,
)

from .auth import TelegramAuthError, verify_telegram_payload
from .config import get_settings
from .dependencies import (
    SESSION_KEY_USER,
    get_current_user,
    is_admin_user,
    require_admin,
    require_user,
)


BASE_DIR = Path(__file__).parent
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"


def _safe_next(next_param: Optional[str]) -> str:
    if not next_param:
        return "/app"
    if next_param.startswith("/"):
        return next_param
    return "/app"


def _json_success(payload: dict) -> JSONResponse:
    return JSONResponse(payload)


api = APIRouter(prefix="/api", tags=["api"])


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


@api.get("/clients")
def api_clients(
    page: int = 1,
    search: str | None = None,
    sort: str | None = None,
    direction: str = "asc",
    user=Depends(require_user),
):
    settings = get_settings()
    page = max(page, 1)
    limit = settings.clients_page_size
    offset = (page - 1) * limit
    term = search.strip() if isinstance(search, str) else None

    rows = client_repository.list_clients(
        limit=limit,
        offset=offset,
        search=term,
        sort=sort,
        direction=direction,
    )
    total = client_repository.count_clients(search=term)
    total_pages = max(math.ceil(total / limit), 1)

    return _json_success(
        {
            "items": jsonable_encoder(rows),
            "pagination": {
                "page": page,
                "pageSize": limit,
                "total": total,
                "totalPages": total_pages,
            },
        }
    )


@api.get("/clients/{client_id}")
def api_get_client(client_id: int, user=Depends(require_user)):
    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"item": jsonable_encoder(record)}


@api.patch("/clients/{client_id}")
async def api_update_client(client_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()

    record = client_repository.get_client(client_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    updates: dict[str, object] = {}

    def _clean_text(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            return trimmed or None
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid text value")

    def _clean_numeric(value: object, field: str) -> float | None:
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

    if "first_name" in payload:
        updates["first_name"] = _clean_text(payload["first_name"])
    if "last_name" in payload:
        updates["last_name"] = _clean_text(payload["last_name"])
    if "weight" in payload:
        updates["weight"] = _clean_numeric(payload["weight"], "weight")
    if "height" in payload:
        updates["height"] = _clean_numeric(payload["height"], "height")
    if "ftp" in payload:
        updates["ftp"] = _clean_numeric(payload["ftp"], "ftp")
    if "favorite_bike" in payload:
        updates["favorite_bike"] = _clean_text(payload["favorite_bike"])
    if "pedals" in payload:
        updates["pedals"] = _clean_text(payload["pedals"])
    if "goal" in payload:
        updates["goal"] = _clean_text(payload["goal"])
    if "gender" in payload:
        gender_value = _clean_text(payload["gender"])
        updates["gender"] = gender_value.lower() if isinstance(gender_value, str) else gender_value
    if "saddle_height" in payload:
        updates["saddle_height"] = _clean_text(payload["saddle_height"])

    if "first_name" in updates or "last_name" in updates:
        first = updates.get("first_name", record.get("first_name"))
        last = updates.get("last_name", record.get("last_name"))
        full_name = " ".join(part for part in [first, last] if part) if (first or last) else None
        updates["full_name"] = full_name

    # remove keys that remain unchanged compared to existing values
    for key in list(updates.keys()):
        if key == "full_name":
            continue
        if updates[key] == record.get(key):
            updates.pop(key)
    if "full_name" in updates and updates["full_name"] == record.get("full_name"):
        updates.pop("full_name")

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    try:
        client_repository.update_client_fields(client_id, **updates)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Another client already uses this full name"
        ) from exc
    updated = client_repository.get_client(client_id)
    if not updated:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"item": jsonable_encoder(updated)}


@api.post("/clients")
async def api_create_client(request: Request, user=Depends(require_admin)):
    payload = await request.json()

    def _clean_text(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            return trimmed or None
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid text value")

    def _clean_numeric(value: object, field: str) -> float | None:
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

    first_name = _clean_text(payload.get("first_name"))
    last_name = _clean_text(payload.get("last_name"))
    if not first_name and not last_name:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Имя или фамилия обязательны")

    weight = _clean_numeric(payload.get("weight"), "weight")
    height = _clean_numeric(payload.get("height"), "height")
    ftp = _clean_numeric(payload.get("ftp"), "ftp")
    pedals = _clean_text(payload.get("pedals"))
    goal = _clean_text(payload.get("goal"))
    favorite_bike = _clean_text(payload.get("favorite_bike"))
    saddle_height = _clean_text(payload.get("saddle_height"))
    gender = _clean_text(payload.get("gender"))
    if isinstance(gender, str):
        gender = gender.lower()

    try:
        record = client_repository.create_client(
            first_name=first_name,
            last_name=last_name,
            gender=gender,
            weight=weight,
            height=height,
            ftp=ftp,
            pedals=pedals,
            goal=goal,
            favorite_bike=favorite_bike,
            saddle_height=saddle_height,
        )
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "Клиент с таким именем уже существует") from exc

    created = client_repository.get_client(record["id"])
    return {"item": jsonable_encoder(created or record)}


@api.delete("/clients/{client_id}")
def api_delete_client(client_id: int, user=Depends(require_admin)):
    # Remove linked Telegram accounts if present (FK may handle cascade but ensure consistency)
    client_link_repository.remove_link(client_id=client_id)

    deleted = client_repository.delete_client(client_id)
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")
    return {"ok": True}


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


@api.get("/client-links")
def api_client_links(user=Depends(require_admin)):
    rows = client_link_repository.list_links()
    return _json_success({"items": jsonable_encoder(rows)})


@api.patch("/client-links/{client_id}")
async def api_update_client_link(client_id: int, request: Request, user=Depends(require_admin)):
    payload = await request.json()
    tg_user_id = payload.get("tg_user_id")
    if tg_user_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_id required")

    try:
        tg_user_id_int = int(tg_user_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid tg_user_id") from exc

    tg_username = payload.get("tg_username")
    tg_full_name = payload.get("tg_full_name")
    if isinstance(tg_username, str):
        tg_username = tg_username.strip() or None
    if isinstance(tg_full_name, str):
        tg_full_name = tg_full_name.strip() or None

    record = client_link_repository.link_user_to_client(
        tg_user_id=tg_user_id_int,
        client_id=client_id,
        tg_username=tg_username,
        tg_full_name=tg_full_name,
    )
    return {"item": jsonable_encoder(record)}


@api.delete("/client-links/{client_id}")
def api_delete_client_link(client_id: int, user=Depends(require_admin)):
    client_link_repository.remove_link(client_id=client_id)
    return {"status": "ok"}


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

    @app.get("/")
    def root():
        return RedirectResponse(url="/app", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    @app.get("/logout")
    def logout(request: Request):
        request.session.pop(SESSION_KEY_USER, None)
        return RedirectResponse(url="/app", status_code=status.HTTP_303_SEE_OTHER)

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

    @app.get("/api/health")
    def api_health():
        return {"status": "ok"}

    @app.get("/api/logout")
    def api_logout(request: Request):
        request.session.pop(SESSION_KEY_USER, None)
        return {"status": "ok"}

    if FRONTEND_DIST.exists():
        dist_root = FRONTEND_DIST.resolve()
        assets_dir = dist_root / "assets"
        if assets_dir.exists():
            app.mount("/app/assets", StaticFiles(directory=str(assets_dir), html=False), name="frontend-assets")

        index_file = dist_root / "index.html"

        @lru_cache()
        def _index_html() -> str:
            if not index_file.exists():
                raise RuntimeError("Frontend index.html is missing. Run npm install && npm run build inside webapp/frontend.")
            return index_file.read_text(encoding="utf-8")

        @app.get("/app", response_class=HTMLResponse)
        def serve_spa_root() -> HTMLResponse:
            return HTMLResponse(_index_html())

        @app.get("/app/{path:path}")
        def serve_spa_asset(path: str):
            target = (dist_root / path).resolve()
            if dist_root in target.parents or target == dist_root:
                if target.is_file():
                    return FileResponse(target)
            return HTMLResponse(_index_html())
    else:
        @app.get("/app")
        def spa_placeholder():
            return {
                "detail": "Frontend is not built yet. Run npm install && npm run build inside webapp/frontend.",
            }

    return app


app = create_app()
