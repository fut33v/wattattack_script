"""Messaging endpoints (broadcast, history)."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse

from repositories import client_link_repository, message_repository, race_repository

from ..config import get_settings
from ..dependencies import require_admin

log = logging.getLogger(__name__)

router = APIRouter(prefix="/messages", tags=["messages"], dependencies=[Depends(require_admin)])

BASE_DIR = Path(__file__).resolve().parent.parent
UPLOADS_DIR = BASE_DIR / "uploads"


def ensure_uploads_dir() -> None:
    """Create uploads directory if missing."""
    try:
        UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # pylint: disable=broad-except
        log.warning("messaging: failed to create uploads dir: %s", exc)


def _json_success(payload: dict) -> JSONResponse:
    return JSONResponse(payload)


def _build_base_url(request: Request) -> str:
    settings = get_settings()
    base = settings.public_url or settings.base_url or str(request.base_url)
    return base.rstrip("/")


async def _parse_broadcast_payload(request: Request) -> tuple[dict, UploadFile | None]:
    content_type = request.headers.get("content-type", "").lower()
    image_upload: UploadFile | None = None

    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        payload = dict(form)
        candidate = form.get("image")
        if isinstance(candidate, UploadFile) or hasattr(candidate, "filename"):
            image_upload = candidate  # type: ignore[assignment]
    else:
        try:
            payload = await request.json()
        except Exception:
            form = await request.form()
            payload = dict(form)
            candidate = form.get("image")
            if isinstance(candidate, UploadFile) or hasattr(candidate, "filename"):
                image_upload = candidate  # type: ignore[assignment]

    return payload, image_upload


def _parse_id_list(raw_value: object, field_name: str) -> set[int] | None:
    if raw_value is None:
        return None

    values: list[object]
    if isinstance(raw_value, list):
        values = list(raw_value)
    elif isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return set()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                values = parsed
            else:
                values = [parsed]
        except Exception:
            values = [part for part in text.split(",") if part]
    else:
        values = [raw_value]

    result: set[int] = set()
    for value in values:
        try:
            int_value = int(value)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"{field_name} must contain integers") from exc
        if int_value <= 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"{field_name} must be positive integers")
        result.add(int_value)

    return result


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


@router.post("/broadcast")
async def api_broadcast_message(request: Request):
    """Broadcast a message to linked Telegram users."""
    try:
        log.info(
            "broadcast: incoming content-type=%s length=%s",
            request.headers.get("content-type"),
            request.headers.get("content-length"),
        )
        payload, image_upload = await _parse_broadcast_payload(request)
        log.debug("broadcast: payload keys=%s", list(payload.keys()))
        message_text = payload.get("message")
        send_at = payload.get("sendAt") or payload.get("send_at")  # ISO datetime string or None for immediate
        client_ids_raw = payload.get("clientIds") or payload.get("client_ids")
        race_id_raw = payload.get("raceId") or payload.get("race_id")
        image_url = payload.get("imageUrl") or payload.get("image_url")

        if (not message_text or not isinstance(message_text, str) or len(message_text.strip()) == 0) and not (
            image_upload or image_url
        ):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Message text or image is required")

        if message_text and not isinstance(message_text, str):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Message text must be a string")

        client_id_filter: set[int] | None = None
        if client_ids_raw is not None:
            client_id_filter = _parse_id_list(client_ids_raw, "clientIds")
            if client_id_filter is not None and len(client_id_filter) == 0:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "clientIds cannot be empty")

        race_client_ids: set[int] | None = None
        if race_id_raw is not None:
            try:
                race_id = int(race_id_raw)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "raceId must be an integer") from exc
            if race_id <= 0:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "raceId must be positive")

            race = race_repository.get_race(race_id)
            if not race:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

            registrations = race_repository.list_registrations(race_id)
            allowed_statuses = {
                race_repository.RACE_STATUS_APPROVED,
                race_repository.RACE_STATUS_PENDING,
            }
            race_client_ids = {
                int(reg["client_id"])
                for reg in registrations or []
                if reg.get("client_id") and str(reg.get("status") or "").lower() in allowed_statuses
            }

            if not race_client_ids:
                return {"sent": 0, "message": "Не найдены участники выбранной гонки"}

        if send_at is not None:
            log.warning("Scheduled messaging not yet implemented, sending immediately")

        try:
            links = client_link_repository.list_links()
        except Exception as exc:  # pylint: disable=broad-except
            log.exception("Failed to fetch client links")
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch client links") from exc

        if not links:
            return {"sent": 0, "message": "Нет подключённых пользователей"}

        if client_id_filter is not None:
            links = [link for link in links if int(link.get("client_id") or 0) in client_id_filter]
            if not links:
                return {"sent": 0, "message": "Нет получателей среди выбранных клиентов"}

        if race_client_ids is not None:
            links = [link for link in links if int(link.get("client_id") or 0) in race_client_ids]
            if not links:
                return {"sent": 0, "message": "Нет получателей среди участников гонки"}

        settings = get_settings()
        bot_token = settings.krutilkavn_bot_token
        if not bot_token:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "KRUTILKAVN_BOT_TOKEN not configured")

        image_bytes: bytes | None = None
        image_filename = "image.jpg"
        if image_upload is not None:
            try:
                image_bytes = await image_upload.read()
                if image_upload.filename:
                    image_filename = image_upload.filename
                if not image_bytes:
                    raise HTTPException(status.HTTP_400_BAD_REQUEST, "Uploaded image is empty")
            except Exception as exc:  # pylint: disable=broad-except
                log.exception("Failed to read uploaded image")
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Failed to read uploaded image") from exc

            try:
                image_url = _store_uploaded_image(image_upload, request=request, image_bytes=image_bytes)
            except HTTPException:
                raise
            except Exception as exc:  # pylint: disable=broad-except
                log.exception("Failed to store uploaded image")
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to store uploaded image") from exc

        sent_count = 0
        failed_count = 0
        total_recipients = len(links)

        for link in links:
            tg_user_id = link.get("tg_user_id")
            if not tg_user_id:
                continue

            try:
                import requests

                if image_bytes is not None or image_url:
                    photo_url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
                    caption = message_text.strip() if isinstance(message_text, str) else ""
                    caption_allowed = bool(caption) and len(caption) <= 1024

                    photo_sent = False
                    last_status = None
                    last_text = None

                    if image_url:
                        payload = {
                            "chat_id": str(tg_user_id),
                            "photo": image_url,
                        }
                        if caption_allowed:
                            payload["caption"] = caption
                            payload["parse_mode"] = "HTML"
                        response = requests.post(photo_url, data=payload, timeout=20)
                        last_status = response.status_code
                        last_text = response.text
                        if response.status_code == 200:
                            photo_sent = True

                    if not photo_sent and image_bytes is not None:
                        files = {"photo": (image_filename or "image.jpg", image_bytes)}
                        data = {"chat_id": str(tg_user_id)}
                        if caption_allowed:
                            data["caption"] = caption
                            data["parse_mode"] = "HTML"
                        response = requests.post(photo_url, data=data, files=files, timeout=30)
                        last_status = response.status_code
                        last_text = response.text
                        if response.status_code == 200:
                            photo_sent = True

                    if not photo_sent:
                        log.warning(
                            "Failed to send photo to user %s: %s %s",
                            tg_user_id,
                            last_status,
                            last_text,
                        )
                        failed_count += 1
                        continue

                    sent_count += 1

                    if caption and not caption_allowed:
                        follow_data = {
                            "chat_id": str(tg_user_id),
                            "text": caption,
                            "parse_mode": "HTML",
                            "disable_web_page_preview": True,
                        }
                        response = requests.post(
                            f"https://api.telegram.org/bot{bot_token}/sendMessage", json=follow_data, timeout=10
                        )
                        if response.status_code != 200:
                            log.warning(
                                "Failed to send caption message to user %s: %s %s",
                                tg_user_id,
                                response.status_code,
                                response.text,
                            )
                            failed_count += 1
                    continue

                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                data = {
                    "chat_id": str(tg_user_id),
                    "text": message_text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }

                response = requests.post(url, json=data, timeout=10)

                if response.status_code == 200:
                    sent_count += 1
                else:
                    log.warning(
                        "Failed to send message to user %s: %s %s",
                        tg_user_id,
                        response.status_code,
                        response.text,
                    )
                    failed_count += 1

            except Exception:
                log.exception("Failed to send message to user %s", tg_user_id)
                failed_count += 1

        return {
            "sent": sent_count,
            "failed": failed_count,
            "total": total_recipients,
            "message": f"Отправлено {sent_count} из {total_recipients} пользователей, ошибок: {failed_count}",
        }

    except HTTPException:
        raise
    except Exception as exc:
        log.exception("Failed to broadcast message")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to broadcast message") from exc


@router.get("")
def api_list_messages(page: int = 1, page_size: int = 50):
    """Return paginated list of user messages."""
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 50

    offset = (page - 1) * page_size
    try:
        messages = message_repository.list_user_messages(limit=page_size, offset=offset)
        total = message_repository.get_user_message_count()

        pagination = {
            "page": page,
            "pageSize": page_size,
            "total": total,
            "totalPages": (total + page_size - 1) // page_size,
        }

        return _json_success({"items": jsonable_encoder(messages), "pagination": pagination})
    except Exception:
        log.exception("Failed to fetch user messages")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch messages") from None
