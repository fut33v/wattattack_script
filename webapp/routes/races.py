"""Race management API routes."""
from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import requests
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from repositories import (
    bikes_repository,
    client_repository,
    layout_repository,
    race_repository,
    schedule_repository,
    trainers_repository,
)

from ..config import get_settings
from ..dependencies import require_admin
from ..utils.parsing import (
    parse_cluster_time as _parse_cluster_time,
    parse_iso_date as _parse_iso_date,
    parse_iso_time as _parse_iso_time,
    parse_positive_int as _parse_positive_int,
    to_float as _to_float,
)

router = APIRouter(prefix="/races", tags=["races"], dependencies=[Depends(require_admin)])
log = logging.getLogger(__name__)

RACE_REGISTRATION_STATUSES = {
    race_repository.RACE_STATUS_PENDING,
    race_repository.RACE_STATUS_APPROVED,
    race_repository.RACE_STATUS_REJECTED,
}
RACE_REGISTRATION_MODES = {"offline", "online"}


def _json_success(payload: dict) -> JSONResponse:
    return JSONResponse(payload)


def _send_telegram_message(chat_id: int, text: str, *, parse_mode: str | None = None) -> bool:
    settings = get_settings()
    bot_token = settings.krutilkavn_bot_token
    if not bot_token:
        log.warning("KRUTILKAVN_BOT_TOKEN not configured; cannot send Telegram message")
        return False

    payload = {
        "chat_id": str(chat_id),
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json=payload,
            timeout=10,
        )
    except Exception:
        log.exception("Failed to send Telegram message to %s", chat_id)
        return False

    if response.status_code != 200:
        log.warning(
            "Telegram API error for chat %s: %s %s",
            chat_id,
            response.status_code,
            response.text,
        )
        return False
    return True


def _match_favorite_bike_id(favorite_raw: Optional[str], bikes_map: Dict[int, Dict[str, Any]]) -> Optional[int]:
    if not favorite_raw:
        return None
    needle = favorite_raw.strip().lower()
    if not needle:
        return None

    exact_matches: List[int] = []
    partial_matches: List[int] = []

    for bike_id, bike in bikes_map.items():
        title = (bike.get("title") or "").strip().lower()
        owner = (bike.get("owner") or "").strip().lower()
        if title == needle or owner == needle:
            exact_matches.append(bike_id)
        elif needle in title or (owner and needle in owner):
            partial_matches.append(bike_id)

    if exact_matches:
        return exact_matches[0]
    if partial_matches:
        return partial_matches[0]
    return None


def _score_bike_height(bike: Dict[str, Any], client_height: Optional[float]) -> float:
    """Lower is better; penalize out-of-range bikes heavily."""

    if client_height is None:
        return 120.0

    min_h = _to_float(bike.get("height_min_cm"))
    max_h = _to_float(bike.get("height_max_cm"))

    if min_h is not None and max_h is not None:
        if min_h <= client_height <= max_h:
            midpoint = (min_h + max_h) / 2
            return abs(client_height - midpoint)
        if client_height < min_h:
            return 200.0 + (min_h - client_height)
        return 200.0 + (client_height - max_h)

    if min_h is not None:
        if client_height >= min_h:
            return client_height - min_h
        return 200.0 + (min_h - client_height)

    if max_h is not None:
        if client_height <= max_h:
            return max_h - client_height
        return 200.0 + (client_height - max_h)

    return 150.0


def _parse_clusters_payload(value: object) -> list[dict[str, str] | str]:
    if value is None:
        return []
    if isinstance(value, list):
        clusters: list[dict[str, str] | str] = []
        for entry in value:
            if isinstance(entry, str):
                label = entry.strip()
                if label:
                    clusters.append(label)
            elif isinstance(entry, dict):
                label = str(entry.get("label") or entry.get("title") or "").strip()
                if label:
                    cluster: dict[str, str] = {"label": label}
                    code = entry.get("code")
                    if isinstance(code, str) and code.strip():
                        cluster["code"] = code.strip()
                    start_time = _parse_cluster_time("start_time", entry.get("start_time") or entry.get("startTime"))
                    end_time = _parse_cluster_time("end_time", entry.get("end_time") or entry.get("endTime"))
                    if start_time:
                        cluster["start_time"] = start_time
                    if end_time:
                        cluster["end_time"] = end_time
                    if start_time and end_time:
                        try:
                            start_dt = datetime.strptime(start_time, "%H:%M").time()
                            end_dt = datetime.strptime(end_time, "%H:%M").time()
                        except ValueError:
                            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid cluster time format") from None
                        if end_dt <= start_dt:
                            raise HTTPException(
                                status.HTTP_400_BAD_REQUEST,
                                f"end_time must be after start_time for cluster '{label}'",
                            )
                    clusters.append(cluster)
        return clusters
    if isinstance(value, str):
        tokens = [token.strip() for token in value.replace(",", "\n").splitlines()]
        return [token for token in tokens if token]
    raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid clusters payload")


def _format_race_date_label(value: object) -> Optional[str]:
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                parsed = datetime.strptime(value, fmt).date()
                return parsed.strftime("%d.%m.%Y")
            except ValueError:
                continue
        return value.strip() or None
    return None


def _format_registration_status_message(status_value: str) -> Optional[str]:
    status_value = (status_value or "").lower()
    if status_value == race_repository.RACE_STATUS_APPROVED:
        return "✅ Оплата подтверждена! Ждём вас на старте."
    if status_value == race_repository.RACE_STATUS_REJECTED:
        return "❌ Оплату не удалось подтвердить. Свяжитесь с администратором для уточнения."
    if status_value == race_repository.RACE_STATUS_PENDING:
        return "⌛ Оплата ожидает проверки. Мы сообщим дополнительно после подтверждения."
    return None


def _notify_registration_update(old_record: dict, new_record: dict, race: dict) -> None:
    tg_user_id = new_record.get("tg_user_id")
    if not tg_user_id:
        return

    old_status = (old_record.get("status") or "").lower()
    new_status = (new_record.get("status") or "").lower()
    status_changed = old_status != new_status

    old_cluster = (old_record.get("cluster_label") or "").strip()
    new_cluster = (new_record.get("cluster_label") or "").strip()
    cluster_changed = old_cluster != new_cluster

    if not status_changed and not cluster_changed:
        return

    header_parts = [f"🏁 {race.get('title') or 'Гонка'}"]
    race_date_text = _format_race_date_label(race.get("race_date"))
    if race_date_text:
        header_parts.append(f"({race_date_text})")

    lines = [" ".join(part for part in header_parts if part)]
    if status_changed:
        status_message = _format_registration_status_message(new_status)
        if status_message:
            lines.append(status_message)
    if cluster_changed:
        if new_cluster:
            lines.append(f"📌 Вам назначен кластер {new_cluster}.")
        else:
            lines.append("📌 Кластер пока не назначен. Мы сообщим, как только обновим информацию.")

    if len(lines) > 1:
        success = _send_telegram_message(int(tg_user_id), "\n".join(lines))
        if not success:
            log.warning("Failed to notify user %s about race registration update", tg_user_id)


def _serialize_race_registration(record: dict) -> dict:
    serialized = dict(record)
    for field in ("payment_submitted_at", "created_at", "updated_at"):
        value = serialized.get(field)
        if hasattr(value, "isoformat"):
            serialized[field] = value.isoformat()
    if "bike_id" in serialized and "bikeId" not in serialized:
        serialized["bikeId"] = serialized.get("bike_id")
    if "bike_title" in serialized and "bikeTitle" not in serialized:
        serialized["bikeTitle"] = serialized.get("bike_title")
    if "bike_owner" in serialized and "bikeOwner" not in serialized:
        serialized["bikeOwner"] = serialized.get("bike_owner")
    if "client_height" in serialized and "clientHeight" not in serialized:
        serialized["clientHeight"] = serialized.get("client_height")
    if "client_weight" in serialized and "clientWeight" not in serialized:
        serialized["clientWeight"] = serialized.get("client_weight")
    if "client_ftp" in serialized and "clientFtp" not in serialized:
        serialized["clientFtp"] = serialized.get("client_ftp")
    if "client_pedals" in serialized and "clientPedals" not in serialized:
        serialized["clientPedals"] = serialized.get("client_pedals")
    if "cluster_start_time" in serialized and "clusterStartTime" not in serialized:
        serialized["clusterStartTime"] = serialized.get("cluster_start_time")
    if "stand_order" in serialized and "standOrder" not in serialized:
        serialized["standOrder"] = serialized.get("stand_order")
    return serialized


def _serialize_race(record: dict, *, include_registrations: bool = False) -> dict:
    serialized = dict(record)
    race_date = serialized.get("race_date")
    if isinstance(race_date, date):
        serialized["race_date"] = race_date.isoformat()
    for field in ("created_at", "updated_at"):
        value = serialized.get(field)
        if hasattr(value, "isoformat"):
            serialized[field] = value.isoformat()
    slug_value = serialized.get("slug")
    if slug_value is not None:
        serialized["slug"] = str(slug_value).strip()

    clusters = serialized.get("clusters") or []
    if isinstance(clusters, list):
        normalized_clusters = []
        for entry in clusters:
            if not isinstance(entry, dict):
                continue
            label = (entry.get("label") or "").strip()
            code = (entry.get("code") or "").strip() or None
            if label:
                cluster_payload: dict[str, str | None] = {"label": label, "code": code}
                start_time = (entry.get("start_time") or "").strip()
                end_time = (entry.get("end_time") or "").strip()
                if start_time:
                    cluster_payload["start_time"] = start_time
                if end_time:
                    cluster_payload["end_time"] = end_time
                normalized_clusters.append(cluster_payload)
        serialized["clusters"] = normalized_clusters
    for key in ("pending_count", "approved_count"):
        value = serialized.get(key)
        if value is not None:
            try:
                serialized[key] = int(value)
            except (TypeError, ValueError):
                pass

    if include_registrations:
        registrations = serialized.pop("registrations", None)
        if registrations is None and serialized.get("id") is not None:
            registrations = race_repository.list_registrations(serialized["id"])
        serialized["registrations"] = [_serialize_race_registration(row) for row in registrations or []]
    return serialized


@router.get("")
def api_list_races():
    rows = race_repository.list_races()
    return _json_success({"items": [_serialize_race(row) for row in rows]})


@router.post("")
async def api_create_race(request: Request):
    payload = await request.json()
    title = payload.get("title")
    if not isinstance(title, str) or not title.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "title is required")

    race_date = _parse_iso_date("race_date", payload.get("race_date"))
    price_rub = _parse_positive_int("price_rub", payload.get("price_rub"))

    sbp_phone = payload.get("sbp_phone")
    if not isinstance(sbp_phone, str) or not sbp_phone.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "sbp_phone is required")

    payment_instructions = payload.get("payment_instructions")
    if payment_instructions is not None:
        if not isinstance(payment_instructions, str):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "payment_instructions must be string")
        payment_instructions = payment_instructions.strip() or None

    notes = payload.get("notes")
    if notes is not None:
        if not isinstance(notes, str):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "notes must be string")
        notes = notes.strip() or None

    description = payload.get("description")
    if description is not None:
        if not isinstance(description, str):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "description must be string")
        description = description.strip() or None

    clusters = _parse_clusters_payload(payload.get("clusters"))

    is_active_value = payload.get("is_active", True)
    if isinstance(is_active_value, bool):
        is_active = is_active_value
    elif is_active_value in (0, 1):
        is_active = bool(is_active_value)
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "is_active must be boolean")

    slug_value = payload.get("slug")
    if slug_value is not None:
        if not isinstance(slug_value, str):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "slug must be a string")
        slug_value = slug_value.strip() or None

    record = race_repository.create_race(
        title=title.strip(),
        race_date=race_date,
        price_rub=price_rub,
        sbp_phone=sbp_phone.strip(),
        payment_instructions=payment_instructions,
        clusters=clusters,
        notes=notes,
        description=description,
        is_active=is_active,
        slug=slug_value,
    )
    return {"item": _serialize_race(record)}


@router.get("/{race_id}")
def api_get_race(race_id: int):
    record = race_repository.get_race(race_id)
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")
    record["registrations"] = race_repository.list_registrations(race_id)
    return {"item": _serialize_race(record, include_registrations=True)}


@router.post("/{race_id}/schedule/slots")
def api_create_race_slots(race_id: int):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    race_date = race.get("race_date")
    if isinstance(race_date, str):
        try:
            race_date = datetime.strptime(race_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid race_date") from exc
    if not isinstance(race_date, date):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Race date is missing")

    clusters = list(race.get("clusters") or [])
    existing_codes = {str(entry.get("code") or entry.get("label") or "").strip() for entry in clusters}
    registrations_raw = race_repository.list_registrations(race_id)
    for reg in registrations_raw:
        code = (reg.get("cluster_code") or reg.get("cluster_label") or "").strip()
        if code and code not in existing_codes:
            clusters.append({"code": code, "label": code})
            existing_codes.add(code)
    if not clusters:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Race has no clusters")

    week = schedule_repository.get_or_create_week(week_start_date=race_date, title=race.get("title"))

    created_ids: list[int] = []
    skipped: list[str] = []
    duplicates: list[str] = []
    errors: list[str] = []

    for entry in clusters:
        if not isinstance(entry, dict):
            continue
        label = (entry.get("label") or entry.get("code") or "").strip() or "Кластер"

        start_time_value = entry.get("start_time") or entry.get("start")
        end_time_value = entry.get("end_time") or entry.get("end")
        if not start_time_value or not end_time_value:
            skipped.append(label)
            continue

        try:
            start_time_str = _parse_cluster_time("start_time", start_time_value)
            end_time_str = _parse_cluster_time("end_time", end_time_value)
        except HTTPException:
            errors.append(label)
            continue

        if not start_time_str or not end_time_str:
            skipped.append(label)
            continue

        try:
            start_time = datetime.strptime(start_time_str, "%H:%M").time()
            end_time = datetime.strptime(end_time_str, "%H:%M").time()
        except ValueError:
            errors.append(label)
            continue

        if end_time <= start_time:
            errors.append(label)
            continue

        try:
            slot = schedule_repository.create_slot(
                week_id=week["id"],
                slot_date=race_date,
                start_time=start_time,
                end_time=end_time,
                label=f"Гонка · {label}",
                session_kind="race",
                notes=f"Гонка {race.get('title') or ''}".strip() or None,
            )
            if slot.get("id"):
                created_ids.append(int(slot["id"]))
        except psycopg2.errors.UniqueViolation:
            duplicates.append(label)
        except Exception:
            log.exception("Failed to create schedule slot for race %s cluster %s", race_id, label)
            errors.append(label)

    race_date_label = race_date.isoformat()
    week_start = week.get("week_start_date")
    week_start_label = week_start.isoformat() if hasattr(week_start, "isoformat") else str(week_start)

    return {
        "created": len(created_ids),
        "slot_ids": created_ids,
        "race_date": race_date_label,
        "week_id": week.get("id"),
        "week_start_date": week_start_label,
        "skipped_missing_time": skipped,
        "duplicates": duplicates,
        "errors": errors,
    }


@router.post("/{race_id}/schedule/seat")
def api_seat_race_participants(race_id: int):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    race_date = race.get("race_date")
    if isinstance(race_date, str):
        try:
            race_date = datetime.strptime(race_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid race_date") from exc
    if not isinstance(race_date, date):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Race date is missing")

    clusters = list(race.get("clusters") or [])
    registrations_raw = race_repository.list_registrations(race_id)
    existing_codes = {str(entry.get("code") or entry.get("label") or "").strip() for entry in clusters}
    for reg in registrations_raw:
        code = (reg.get("cluster_code") or reg.get("cluster_label") or "").strip()
        if code and code not in existing_codes:
            clusters.append({"code": code, "label": code})
            existing_codes.add(code)
    if not clusters:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Race has no clusters")

    week = schedule_repository.get_week_by_start(race_date)
    if not week:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Create slots for the race first")

    def _normalize_date(value: object) -> Optional[date]:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return None
        return None

    def _normalize_time(value: object) -> Optional[time]:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:
                    return datetime.strptime(value, fmt).time()
                except ValueError:
                    continue
        return None

    slots = schedule_repository.list_slots_with_reservations(week["id"])
    race_slots: dict[int, dict] = {}
    slot_used: Dict[int, set[int]] = {}

    for slot in slots:
        slot_date = _normalize_date(slot.get("slot_date"))
        if slot_date != race_date:
            continue
        if slot.get("session_kind") != "race":
            continue
        if slot.get("id"):
            schedule_repository.clear_reservations_for_slot(slot["id"])
            schedule_repository.ensure_slot_capacity(slot["id"])
            refreshed = schedule_repository.get_slot_with_reservations(slot["id"]) or slot
            race_slots[slot["id"]] = refreshed

    if not race_slots:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No race slots found for this date")

    bikes = bikes_repository.list_bikes()
    bikes_map: Dict[int, Dict[str, Any]] = {row["id"]: row for row in bikes if isinstance(row.get("id"), int)}
    trainers = trainers_repository.list_trainers()
    trainers_map: Dict[int, Dict[str, Any]] = {row["id"]: row for row in trainers if isinstance(row.get("id"), int)}
    layout_rows = layout_repository.list_layout_details()
    layout_map: Dict[int, Dict[str, Any]] = {
        row["stand_id"]: row for row in layout_rows if isinstance(row.get("stand_id"), int)
    }

    def _stand_position_key(stand_id: int) -> int:
        if stand_id in layout_map and layout_map[stand_id].get("stand_position") is not None:
            return int(layout_map[stand_id]["stand_position"])
        trainer_row = trainers_map.get(stand_id)
        if trainer_row and trainer_row.get("position") is not None:
            try:
                return int(trainer_row["position"])
            except (TypeError, ValueError):
                return 9999
        return 9999

    slot_candidates: Dict[str, dict] = {}
    for cluster in clusters:
        code = (cluster.get("code") or cluster.get("label") or "").strip()
        label = (cluster.get("label") or cluster.get("code") or "").strip() or "Кластер"
        start = _normalize_time(cluster.get("start_time") or cluster.get("start"))
        matched_slot: Optional[dict] = None
        if start:
            for slot in race_slots.values():
                slot_start = _normalize_time(slot.get("start_time"))
                if slot_start and slot_start == start:
                    matched_slot = slot
                    break
        if matched_slot is None and label:
            for slot in race_slots.values():
                slot_label = (slot.get("label") or "").lower()
                if slot_label and label.lower() in slot_label:
                    matched_slot = slot
                    break
        if matched_slot:
            slot_candidates[code] = matched_slot
        else:
            slot_candidates[code] = None

    registrations = registrations_raw
    clients_cache: Dict[int, Dict[str, Any]] = {}

    def _client_label(reg: Dict[str, Any]) -> str:
        label = (reg.get("client_name") or "").strip()
        if label:
            return label
        client_id = reg.get("client_id")
        return f"ID {client_id}" if client_id is not None else "Участник"

    cluster_results: Dict[str, Dict[str, Any]] = {}
    missing_slots: List[str] = []
    unplaced_clients: List[str] = []
    placed_total = 0
    total_candidates = 0
    skipped_online = 0
    skipped_missing_cluster = 0
    unknown_cluster = 0
    already_assigned = 0
    slot_ids_used: set[int] = set()
    assigned_reservation_ids: set[int] = set()
    blocked_count = 0

    for reg in registrations:
        status_value = (reg.get("status") or "").lower()
        if status_value != race_repository.RACE_STATUS_APPROVED:
            continue
        race_mode = (reg.get("race_mode") or "").strip().lower()
        if race_mode == "online":
            skipped_online += 1
            continue
        cluster_code = reg.get("cluster_code") or reg.get("cluster_label")
        if not cluster_code:
            skipped_missing_cluster += 1
            continue

        cluster_entry = next(
            (entry for entry in clusters if cluster_code == entry.get("code") or cluster_code == entry.get("label")),
            {"code": cluster_code, "label": cluster_code},
        )

        code = (cluster_entry.get("code") or cluster_entry.get("label") or "").strip()
        label = (cluster_entry.get("label") or cluster_entry.get("code") or "").strip() or cluster_code
        slot = slot_candidates.get(code) or slot_candidates.get(label)
        total_candidates += 1
        start_time_value = _normalize_time(cluster_entry.get("start_time") or cluster_entry.get("start"))

        stats = cluster_results.setdefault(
            code,
            {
                "cluster": label,
                "code": code,
                "slot_id": slot.get("id") if slot else None,
                "slot_label": slot.get("label") if isinstance(slot, dict) else None,
                "start_time": start_time_value.strftime("%H:%M") if start_time_value else None,
                "requested": 0,
                "placed": 0,
                "already": 0,
                "unplaced": [],
            },
        )
        stats["requested"] += 1

        if slot is None:
            if label not in missing_slots:
                missing_slots.append(label)
            stats["unplaced"].append(_client_label(reg))
            unplaced_clients.append(_client_label(reg))
            continue

        reservations = slot.get("reservations") or []
        client_id = reg.get("client_id")
        if client_id is not None:
            existing_res = next((res for res in reservations if res.get("client_id") == client_id), None)
            if existing_res:
                stats["placed"] += 1
                stats["already"] += 1
                already_assigned += 1
                placed_total += 1
                slot_ids_used.add(slot["id"])
                continue

        if client_id not in clients_cache and isinstance(client_id, int):
            clients_cache[client_id] = client_repository.get_client(client_id) or {}
        client_record = clients_cache.get(client_id or -1, {})
        client_height = _to_float(client_record.get("height"))
        favorite_bike_id = _match_favorite_bike_id(client_record.get("favorite_bike"), bikes_map)
        bring_own_bike = bool(reg.get("bring_own_bike"))

        available: List[Dict[str, Any]] = []
        used_res = slot_used.setdefault(slot["id"], set())
        for reservation in reservations:
            if reservation.get("status") != "available":
                continue
            if reservation.get("id") in used_res:
                continue
            stand_id = reservation.get("stand_id")
            if stand_id is None:
                continue
            available.append(
                {
                    "reservation_id": reservation["id"],
                    "stand_id": stand_id,
                    "bike_id": layout_map.get(stand_id, {}).get("bike_id")
                    or trainers_map.get(stand_id, {}).get("bike_id"),
                    "position_key": _stand_position_key(stand_id),
                }
            )

        if not available:
            stats["unplaced"].append(_client_label(reg))
            unplaced_clients.append(_client_label(reg))
            continue

        candidates: List[Tuple[Dict[str, Any], float]] = []
        for entry in available:
            bike_row = bikes_map.get(entry["bike_id"])
            if bring_own_bike:
                candidates.append((entry, entry["position_key"]))
            elif favorite_bike_id and entry["bike_id"] == favorite_bike_id:
                candidates.append((entry, -5.0 + entry["position_key"]))
            elif bike_row:
                candidates.append((entry, _score_bike_height(bike_row, client_height) + entry["position_key"]))
            else:
                candidates.append((entry, 50.0 + entry["position_key"]))

        chosen_entry, _ = sorted(candidates, key=lambda pair: pair[1])[0]
        reservation_id = chosen_entry["reservation_id"]
        stand_id = chosen_entry["stand_id"]
        used_res.add(reservation_id)
        slot_ids_used.add(slot["id"])
        assigned_reservation_ids.add(reservation_id)

        try:
            updated_res = schedule_repository.update_reservation(
                reservation_id,
                client_id=reg.get("client_id"),
                client_name=reg.get("client_name"),
                notes=f"Гонка {race.get('title') or ''}".strip() or None,
            )
            if updated_res:
                stats["placed"] += 1
                placed_total += 1
                blocked_count += int(updated_res.get("is_blocked") or 0)
        except Exception:
            log.exception("Failed to assign reservation %s for race %s", reservation_id, race_id)
            stats["unplaced"].append(_client_label(reg))
            unplaced_clients.append(_client_label(reg))
            continue

        trainers_repository.set_default_bike(stand_id, chosen_entry.get("bike_id") or None)

    return {
        "clusters": list(cluster_results.values()),
        "placed": placed_total,
        "candidates": total_candidates,
        "missing_slots": missing_slots,
        "unplaced": unplaced_clients,
        "skipped_online": skipped_online,
        "skipped_missing_cluster": skipped_missing_cluster,
        "unknown_cluster": unknown_cluster,
        "already_assigned": already_assigned,
        "slot_ids": list(slot_ids_used),
        "assigned_reservation_ids": list(assigned_reservation_ids),
        "blocked": blocked_count,
    }


@router.post("/{race_id}/registrations")
async def api_create_race_registration(race_id: int, request: Request):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    payload = await request.json()
    client_id = payload.get("client_id")
    if client_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "client_id required")
    try:
        client_id = int(client_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid client_id") from exc

    client = client_repository.get_client(client_id)
    if not client:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Client not found")

    status_value = (payload.get("status") or race_repository.RACE_STATUS_PENDING).lower()
    if status_value not in RACE_REGISTRATION_STATUSES:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid status {status_value}")

    cluster_code = payload.get("cluster_code")
    if cluster_code is not None:
        cluster_code = str(cluster_code).strip() or None

    cluster_label = payload.get("cluster_label")
    if cluster_label is not None:
        cluster_label = str(cluster_label).strip() or None

    race_mode = payload.get("race_mode")
    if race_mode is not None:
        race_mode = str(race_mode).strip() or None
        if race_mode and race_mode.lower() not in {"online", "offline"}:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid race_mode")

    payload_notes = payload.get("notes")
    if payload_notes is not None:
        if not isinstance(payload_notes, str):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "notes must be string")
        payload_notes = payload_notes.strip() or None

    bring_own_bike = payload.get("bringOwnBike")
    if isinstance(bring_own_bike, str):
        bring_own_bike = bring_own_bike.strip().lower() in {"1", "true", "yes", "on"}
    else:
        bring_own_bike = bool(bring_own_bike)

    bike_id_raw = payload.get("bikeId")
    bike_id: Optional[int] = None
    if bike_id_raw is not None:
        try:
            bike_id = int(bike_id_raw)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid bikeId") from exc

    axle_type = payload.get("axle_type")
    if axle_type is not None:
        axle_type = str(axle_type).strip() or None

    gears_label = payload.get("gears_label")
    if gears_label is not None:
        gears_label = str(gears_label).strip() or None

    try:
        record = race_repository.create_registration(
            race_id=race_id,
            client_id=client_id,
            client_name=payload.get("client_name") or client.get("full_name"),
            tg_user_id=client.get("tg_user_id"),
            tg_username=client.get("tg_username"),
            tg_full_name=client.get("tg_full_name"),
            status=status_value,
            cluster_code=cluster_code,
            cluster_label=cluster_label,
            notes=payload_notes,
            race_mode=race_mode,
            bring_own_bike=bring_own_bike,
            bike_id=bike_id,
            axle_type=axle_type,
            gears_label=gears_label,
        )
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Registration already exists") from exc

    return {"item": _serialize_race_registration(record)}


@router.patch("/{race_id}")
async def api_update_race(race_id: int, request: Request):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    payload = await request.json()
    updates = {}
    if "title" in payload:
        title = payload.get("title")
        if not isinstance(title, str) or not title.strip():
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "title is required")
        updates["title"] = title.strip()

    if "race_date" in payload:
        updates["race_date"] = _parse_iso_date("race_date", payload.get("race_date"))

    if "price_rub" in payload:
        updates["price_rub"] = _parse_positive_int("price_rub", payload.get("price_rub"))

    if "sbp_phone" in payload:
        sbp_phone = payload.get("sbp_phone")
        if not isinstance(sbp_phone, str) or not sbp_phone.strip():
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "sbp_phone is required")
        updates["sbp_phone"] = sbp_phone.strip()

    if "payment_instructions" in payload:
        payment_instructions = payload.get("payment_instructions")
        if payment_instructions is not None:
            if not isinstance(payment_instructions, str):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "payment_instructions must be string")
            payment_instructions = payment_instructions.strip() or None
        updates["payment_instructions"] = payment_instructions

    if "notes" in payload:
        notes = payload.get("notes")
        if notes is not None:
            if not isinstance(notes, str):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "notes must be string")
            notes = notes.strip() or None
        updates["notes"] = notes

    if "description" in payload:
        description = payload.get("description")
        if description is not None:
            if not isinstance(description, str):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "description must be string")
            description = description.strip() or None
        updates["description"] = description

    if "is_active" in payload:
        is_active_value = payload.get("is_active")
        if isinstance(is_active_value, bool):
            updates["is_active"] = is_active_value
        elif is_active_value in (0, 1):
            updates["is_active"] = bool(is_active_value)
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "is_active must be boolean")

    if "slug" in payload:
        slug_value = payload.get("slug")
        if slug_value is not None:
            if not isinstance(slug_value, str):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "slug must be a string")
            slug_value = slug_value.strip() or None
        updates["slug"] = slug_value

    if "clusters" in payload:
        updates["clusters"] = _parse_clusters_payload(payload.get("clusters"))

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    try:
        record = race_repository.update_race(race_id, **updates)
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Slug already exists") from exc

    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    return {"item": _serialize_race(record)}


@router.delete("/{race_id}")
def api_delete_race(race_id: int):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    deleted = race_repository.delete_race(race_id)
    if not deleted:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to delete race")
    return {"status": "ok"}


@router.patch("/{race_id}/registrations/{registration_id}")
async def api_update_race_registration(race_id: int, registration_id: int, request: Request):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    payload = await request.json()
    updates = {}

    existing_record = race_repository.get_registration_by_id(registration_id)
    if not existing_record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Registration not found")

    if "status" in payload:
        status_value = str(payload.get("status") or "").lower().strip()
        if status_value not in RACE_REGISTRATION_STATUSES:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid status {status_value}")
        updates["status"] = status_value

    if "clusterCode" in payload or "clusterLabel" in payload:
        cluster_code = payload.get("clusterCode")
        cluster_label = payload.get("clusterLabel")
        if cluster_code is not None:
            cluster_code = str(cluster_code).strip() or None
        if cluster_label is not None:
            cluster_label = str(cluster_label).strip() or None
        updates["cluster_code"] = cluster_code
        updates["cluster_label"] = cluster_label

    if "notes" in payload:
        notes = payload.get("notes")
        if notes is not None:
            if not isinstance(notes, str):
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "notes must be string")
            notes = notes.strip() or None
        updates["notes"] = notes

    if "raceMode" in payload:
        race_mode = payload.get("raceMode")
        if race_mode is not None:
            race_mode = str(race_mode).strip() or None
            if race_mode and race_mode.lower() not in {"online", "offline"}:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid raceMode")
            updates["race_mode"] = race_mode

    if "bikeId" in payload:
        bike_id_raw = payload.get("bikeId")
        if bike_id_raw is None or bike_id_raw == "":
            updates["bike_id"] = None
        else:
            try:
                updates["bike_id"] = int(bike_id_raw)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid bikeId") from exc
        updates.setdefault("bring_own_bike", False)

    if "bringOwnBike" in payload:
        bring_raw = payload.get("bringOwnBike")
        if isinstance(bring_raw, str):
            bring_value = bring_raw.strip().lower() in {"1", "true", "yes", "on"}
        else:
            bring_value = bool(bring_raw)
        updates["bring_own_bike"] = bring_value
        if bring_value:
            updates["bike_id"] = None

    if not updates:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Nothing to update")

    record = race_repository.update_registration(
        registration_id,
        status=updates.get("status"),
        cluster_code=updates.get("cluster_code"),
        cluster_label=updates.get("cluster_label"),
        notes=updates.get("notes"),
        race_mode=updates.get("race_mode"),
        bring_own_bike=updates.get("bring_own_bike"),
        bike_id=updates.get("bike_id"),
    )
    if not record:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Registration not found")

    detailed = race_repository.get_registration_by_id(registration_id) or record

    try:
        _notify_registration_update(existing_record, detailed, race)
    except Exception:
        log.exception("Failed to send Telegram notification for registration %s", registration_id)

    return {"item": _serialize_race_registration(detailed)}


@router.get("/{race_id}/summary")
def api_race_summary(race_id: int):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    registrations = race_repository.list_registrations(race_id)
    client_cache: dict[int, dict] = {}
    for reg in registrations:
        client_id = reg.get("client_id")
        if isinstance(client_id, int) and client_id not in client_cache:
            client_cache[client_id] = client_repository.get_client(client_id) or {}
        client_row = client_cache.get(client_id or -1, {})
        if client_row:
            reg["client_height"] = client_row.get("height")
            reg["client_weight"] = client_row.get("weight")
            reg["client_ftp"] = client_row.get("ftp")
            reg["client_pedals"] = client_row.get("pedals")

    cluster_time_map: dict[str, str] = {}
    for cluster in race.get("clusters") or []:
        code = (cluster.get("code") or cluster.get("label") or "").strip().lower()
        start = (cluster.get("start_time") or "").strip()
        if code and start:
            cluster_time_map[code] = start

    stand_labels: dict[int, dict] = {}
    stand_order_map: dict[int, int] = {}
    race_date_raw = race.get("race_date")
    race_date: Optional[date] = None
    if isinstance(race_date_raw, str):
        try:
            race_date = datetime.strptime(race_date_raw, "%Y-%m-%d").date()
        except ValueError:
            race_date = None
    elif isinstance(race_date_raw, date):
        race_date = race_date_raw

    if race_date:
        week = schedule_repository.get_week_by_start(race_date)
        if week:
            slots = schedule_repository.list_slots_with_reservations(week["id"])
            trainers = trainers_repository.list_trainers()
            trainers_map = {row["id"]: row for row in trainers if isinstance(row.get("id"), int)}
            sorted_trainers = sorted(
                trainers,
                key=lambda row: (
                    _to_float(row.get("code")) is None,
                    _to_float(row.get("code")) if _to_float(row.get("code")) is not None else row.get("code") or "",
                    row.get("id") or 0,
                ),
            )
            for idx, trainer in enumerate(sorted_trainers):
                if isinstance(trainer.get("id"), int):
                    stand_order_map[trainer["id"]] = idx
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
                    stand_id = res.get("stand_id")
                    label_parts: list[str] = []
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
                    stand_labels[client_id] = {
                        "label": " · ".join(label_parts) if label_parts else None,
                        "stand_id": stand_id,
                        "stand_code": stand_code or None,
                        "stand_order": stand_order_map.get(stand_id),
                    }

    bikes = bikes_repository.list_bikes()

    registrations_payload = []
    for reg in registrations:
        client_id = reg.get("client_id")
        stand_meta = stand_labels.get(client_id) if isinstance(client_id, int) else None
        cluster_key = (reg.get("cluster_code") or reg.get("cluster_label") or "").strip().lower()
        payload = _serialize_race_registration(reg)
        if stand_meta:
            payload["stand_label"] = stand_meta.get("label")
            payload["stand_id"] = stand_meta.get("stand_id")
            payload["stand_code"] = stand_meta.get("stand_code")
            payload["stand_order"] = stand_meta.get("stand_order")
        if cluster_key in cluster_time_map:
            payload["cluster_start_time"] = cluster_time_map[cluster_key]
        registrations_payload.append(payload)

    return {
        "race": _serialize_race(race),
        "registrations": registrations_payload,
        "bikes": jsonable_encoder(bikes),
    }


@router.delete("/{race_id}/registrations/{registration_id}")
def api_delete_race_registration(race_id: int, registration_id: int):
    race = race_repository.get_race(race_id)
    if not race:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Race not found")

    deleted = race_repository.delete_registration(race_id, registration_id)
    if not deleted:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Registration not found")

    return {"status": "ok"}
