"""Messaging endpoints (broadcast, history)."""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse

from repositories import client_link_repository, message_repository, race_repository, schedule_repository

from ..config import get_settings
from ..dependencies import require_admin

log = logging.getLogger(__name__)

router = APIRouter(prefix="/messages", tags=["messages"], dependencies=[Depends(require_admin)])

BASE_DIR = Path(__file__).resolve().parent.parent
UPLOADS_DIR = BASE_DIR / "uploads"

FilterClientSet = Tuple[Set[int], str]


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


def _collect_clients_with_bookings_on_date(target_date: date) -> FilterClientSet:
    """Return client IDs that have bookings on a specific date."""

    reservations = schedule_repository.list_reservations_by_date(target_date)
    booked_ids = {int(res["client_id"]) for res in reservations or [] if res.get("client_id")}
    note = f"Бронь на {target_date.isoformat()}"
    if not booked_ids:
        return set(), "Нет броней в расписании"
    return booked_ids, note


def _collect_clients_without_bookings(target_date: date) -> FilterClientSet:
    """Return client IDs that have bookings on a specific date (legacy exclude helper)."""

    booked_ids, note = _collect_clients_with_bookings_on_date(target_date)
    return booked_ids, f"Исключены клиенты с {note.lower()}"


def _collect_clients_with_bookings_for_slot(slot_id: int) -> FilterClientSet:
    """Return client IDs that have bookings for a specific slot."""

    slot = schedule_repository.get_slot_with_reservations(slot_id)
    if not slot:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Slot not found")

    reservations = slot.get("reservations") or []
    booked_ids = {int(res["client_id"]) for res in reservations if res.get("client_id")}

    label_parts = [str(slot.get("slot_date") or "")]
    start_time = slot.get("start_time")
    end_time = slot.get("end_time")
    if start_time and end_time:
        label_parts.append(f"{start_time}-{end_time}")
    slot_label = " ".join(part for part in label_parts if part).strip() or "слот"
    if slot.get("label"):
        slot_label = f"{slot_label} · {slot['label']}"

    note = f"Бронь в слоте {slot_label}"
    return booked_ids, note


@router.get("/booking-filters")
def api_booking_filters(filter_date: Optional[date] = None, slot_id: Optional[int] = None):
    """Return client IDs grouped by booking selectors (today/tomorrow/date/slot)."""

    today = date.today()
    tomorrow = today + timedelta(days=1)
    today_ids, _ = _collect_clients_without_bookings(today)
    tomorrow_ids, _ = _collect_clients_without_bookings(tomorrow)

    payload: dict[str, object] = {
        "todayIds": sorted(today_ids),
        "tomorrowIds": sorted(tomorrow_ids),
    }

    if filter_date is not None:
        date_ids, _ = _collect_clients_with_bookings_on_date(filter_date)
        payload.update(
            {
                "dateIds": sorted(date_ids),
                "dateLabel": filter_date.isoformat(),
            }
        )

    if slot_id is not None:
        slot_ids, note = _collect_clients_with_bookings_for_slot(slot_id)
        payload.update(
            {
                "slotIds": sorted(slot_ids),
                "slotLabel": note,
            }
        )

    return payload


@router.get("/booking-slots")
def api_booking_slots(limit: int = 80):
    """Return upcoming slots for booking-based messaging filters."""

    limit = max(1, min(limit, 200))
    slots = schedule_repository.list_upcoming_slots(limit=limit)

    def _serialize_slot(slot: dict[str, Any]) -> dict[str, object]:
        slot_date = slot.get("slot_date")
        start_time = slot.get("start_time")
        end_time = slot.get("end_time")
        return {
            "id": slot.get("id"),
            "slot_date": slot_date.isoformat() if hasattr(slot_date, "isoformat") else slot_date,
            "start_time": str(start_time) if start_time is not None else None,
            "end_time": str(end_time) if end_time is not None else None,
            "label": slot.get("label"),
            "week_start_date": (
                slot.get("week_start_date").isoformat()
                if hasattr(slot.get("week_start_date"), "isoformat")
                else slot.get("week_start_date")
            ),
            "instructor_name": slot.get("instructor_name"),
        }

    return {"items": [_serialize_slot(slot) for slot in slots]}


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
        raw_message = (
            payload.get("message")
            or payload.get("text")
            or payload.get("caption")
        )
        if isinstance(raw_message, str):
            message_text = raw_message.strip()
        elif raw_message is None:
            message_text = ""
        else:
            # Gracefully coerce non-str payloads (e.g., form field objects) to string
            message_text = str(raw_message).strip()

        if len(message_text) == 0:
            # Fallback: try to read raw body (e.g., text/plain without JSON wrapper)
            try:
                body_bytes = await request.body()
                if body_bytes:
                    message_text = body_bytes.decode(errors="ignore").strip()
            except Exception:  # pylint: disable=broad-except
                message_text = message_text
        send_at = payload.get("sendAt") or payload.get("send_at")  # ISO datetime string or None for immediate
        client_ids_raw = payload.get("clientIds") or payload.get("client_ids")
        race_id_raw = payload.get("raceId") or payload.get("race_id")
        filter_no_booking_today = payload.get("filterNoBookingToday")
        filter_no_booking_tomorrow = payload.get("filterNoBookingTomorrow")
        filter_has_booking_today = payload.get("filterHasBookingToday")
        filter_has_booking_tomorrow = payload.get("filterHasBookingTomorrow")
        filter_booking_date_raw = payload.get("filterBookingDate")
        filter_slot_id_raw = payload.get("filterSlotId") or payload.get("slotId")
        image_url = payload.get("imageUrl") or payload.get("image_url")
        log.info(
            "broadcast: parsed message_len=%s image_upload=%s image_url=%s",
            len(message_text),
            bool(image_upload),
            bool(image_url),
        )

        if len(message_text.strip()) == 0 and not (image_upload or image_url):
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "Добавьте текст сообщения или изображение",
            )

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

        inclusion_ids: set[int] | None = None

        if filter_has_booking_today:
            today_ids, _ = _collect_clients_with_bookings_on_date(date.today())
            inclusion_ids = today_ids if inclusion_ids is None else inclusion_ids | today_ids

        if filter_has_booking_tomorrow:
            tomorrow_ids, _ = _collect_clients_with_bookings_on_date(date.today() + timedelta(days=1))
            inclusion_ids = tomorrow_ids if inclusion_ids is None else inclusion_ids | tomorrow_ids

        if filter_booking_date_raw:
            try:
                parsed_date = date.fromisoformat(str(filter_booking_date_raw))
            except Exception as exc:  # pylint: disable=broad-except
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "filterBookingDate must be YYYY-MM-DD") from exc
            date_ids, _ = _collect_clients_with_bookings_on_date(parsed_date)
            inclusion_ids = date_ids if inclusion_ids is None else inclusion_ids | date_ids

        if filter_slot_id_raw:
            try:
                slot_id = int(filter_slot_id_raw)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "filterSlotId must be an integer") from exc
            slot_ids, _ = _collect_clients_with_bookings_for_slot(slot_id)
            inclusion_ids = slot_ids if inclusion_ids is None else inclusion_ids | slot_ids

        if inclusion_ids is not None:
            links = [link for link in links if int(link.get("client_id") or 0) in inclusion_ids]
            if not links:
                return {"sent": 0, "message": "Нет получателей с бронью по выбранным условиям"}

        # Exclude clients who already have bookings on the selected dates
        exclusion_ids: set[int] = set()
        exclusion_notes: list[str] = []

        if filter_no_booking_today:
            today_ids, note = _collect_clients_without_bookings(date.today())
            exclusion_ids.update(today_ids)
            exclusion_notes.append(note)

        if filter_no_booking_tomorrow:
            tomorrow_ids, note = _collect_clients_without_bookings(date.today() + timedelta(days=1))
            exclusion_ids.update(tomorrow_ids)
            exclusion_notes.append(note)

        if exclusion_ids:
            links = [link for link in links if int(link.get("client_id") or 0) not in exclusion_ids]
            if not links:
                return {"sent": 0, "message": "Нет получателей без броней на выбранные дни"}

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
