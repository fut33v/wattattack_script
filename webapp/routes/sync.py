from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from fastapi import APIRouter, Body, Depends, File, HTTPException, UploadFile, status
from zoneinfo import ZoneInfo

from scheduler.notifier import load_accounts  # type: ignore
from scheduler.notifier_client import (  # type: ignore
    parse_activity_start_dt,
    resolve_scheduled_client,
    extract_athlete_name,
    format_strava_activity_description,
)
from wattattack_activities import WattAttackClient, DEFAULT_BASE_URL
from repositories import client_link_repository, schedule_repository, client_repository, intervals_link_repository
from scheduler import intervals_sync
from straver_client import StraverClient
from ..dependencies import require_admin

router = APIRouter(prefix="/sync", tags=["sync"], dependencies=[Depends(require_admin)])
log = logging.getLogger(__name__)
LOCAL_TZ = ZoneInfo(os.environ.get("WATTATTACK_LOCAL_TZ", "Europe/Moscow"))
STRAVER_HTTP_TIMEOUT = float(os.environ.get("STRAVER_HTTP_TIMEOUT", "15"))


class SyncState:
    MAX_LOG_LINES = 400

    def __init__(self) -> None:
        self.running = False
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.accounts_total: int = 0
        self.accounts_done: int = 0
        self.current_account: Optional[str] = None
        self.log: list[str] = []
        self.summary: Dict[str, Dict[str, int]] = {}
        self.processed = 0
        self.updated = 0
        self.fit_downloaded = 0
        self.error: Optional[str] = None
        self._lock = threading.Lock()

    def start(self, total_accounts: int) -> None:
        with self._lock:
            self.running = True
            self.started_at = datetime.utcnow()
            self.finished_at = None
            self.accounts_total = total_accounts
            self.accounts_done = 0
            self.current_account = None
            self.log = ["Старт синхронизации…"]
            self.summary = {}
            self.processed = 0
            self.updated = 0
            self.fit_downloaded = 0
            self.error = None

    def append_log(self, message: str) -> None:
        with self._lock:
            self.log.append(message)
            # Keep only the tail to avoid unbounded logs
            if len(self.log) > self.MAX_LOG_LINES:
                overflow = len(self.log) - self.MAX_LOG_LINES
                if overflow > 0:
                    self.log = self.log[overflow:]

    def to_dict(self) -> Dict:
        with self._lock:
            return {
                "running": self.running,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "accounts_total": self.accounts_total,
                "accounts_done": self.accounts_done,
                "current_account": self.current_account,
                "log": list(self.log),
                "summary": dict(self.summary),
                "processed": self.processed,
                "updated": self.updated,
                "fit_downloaded": self.fit_downloaded,
                "error": self.error,
            }

    def finish(self, error: Optional[str] = None) -> None:
        with self._lock:
            self.running = False
            self.finished_at = datetime.utcnow()
            self.error = error


SYNC_STATE = SyncState()


class StravaBackfillState:
    MAX_LOG_LINES = 400

    def __init__(self) -> None:
        self.running = False
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.users_total: int = 0
        self.users_done: int = 0
        self.current_user: Optional[str] = None
        self.log: list[str] = []
        self.summary: Dict[str, Dict[str, int]] = {}
        self.uploaded = 0
        self.skipped = 0
        self.error: Optional[str] = None
        self._lock = threading.Lock()

    def start(self, total_users: int) -> None:
        with self._lock:
            self.running = True
            self.started_at = datetime.utcnow()
            self.finished_at = None
            self.users_total = total_users
            self.users_done = 0
            self.current_user = None
            self.log = ["Старт загрузки в Strava…"]
            self.summary = {}
            self.uploaded = 0
            self.skipped = 0
            self.error = None

    def append_log(self, message: str) -> None:
        with self._lock:
            self.log.append(message)
            if len(self.log) > self.MAX_LOG_LINES:
                overflow = len(self.log) - self.MAX_LOG_LINES
                if overflow > 0:
                    self.log = self.log[overflow:]

    def to_dict(self) -> Dict:
        with self._lock:
            return {
                "running": self.running,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "users_total": self.users_total,
                "users_done": self.users_done,
                "current_user": self.current_user,
                "log": list(self.log),
                "summary": dict(self.summary),
                "uploaded": self.uploaded,
                "skipped": self.skipped,
                "error": self.error,
            }

    def finish(self, error: Optional[str] = None) -> None:
        with self._lock:
            self.running = False
            self.finished_at = datetime.utcnow()
            self.error = error


STRAVA_STATE = StravaBackfillState()
class IntervalsBackfillState:
    MAX_LOG_LINES = 400

    def __init__(self) -> None:
        self.running = False
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.users_total: int = 0
        self.users_done: int = 0
        self.current_user: Optional[str] = None
        self.log: list[str] = []
        self.summary: dict[str, dict] = {}
        self.uploaded: int = 0
        self.skipped: int = 0
        self.error: Optional[str] = None
        self._lock = threading.Lock()

    def start(self, total_users: int) -> None:
        with self._lock:
            self.running = True
            self.started_at = datetime.utcnow()
            self.finished_at = None
            self.users_total = total_users
            self.users_done = 0
            self.current_user = None
            self.log = ["Старт загрузки в Intervals…"]
            self.summary = {}
            self.uploaded = 0
            self.skipped = 0
            self.error = None

    def append_log(self, message: str) -> None:
        with self._lock:
            self.log.append(message)
            if len(self.log) > self.MAX_LOG_LINES:
                overflow = len(self.log) - self.MAX_LOG_LINES
                if overflow > 0:
                    self.log = self.log[overflow:]

    def to_dict(self) -> Dict:
        with self._lock:
            return {
                "running": self.running,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "finished_at": self.finished_at.isoformat() if self.finished_at else None,
                "users_total": self.users_total,
                "users_done": self.users_done,
                "current_user": self.current_user,
                "log": list(self.log),
                "summary": dict(self.summary),
                "uploaded": self.uploaded,
                "skipped": self.skipped,
                "error": self.error,
            }

    def finish(self, error: Optional[str] = None) -> None:
        with self._lock:
            self.running = False
            self.finished_at = datetime.utcnow()
            self.error = error


INTERVALS_STATE = IntervalsBackfillState()


def _fit_storage_path(account_id: str, activity_id: str) -> Path:
    base = schedule_repository.ensure_fit_files_dir()
    dest = base / account_id
    dest.mkdir(parents=True, exist_ok=True)
    return dest / f"{activity_id}.fit"


def _normalize_account_id_value(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = str(raw).strip()
    legacy_match = re.match(r"krutilkavn0*([1-9]\d*)", text, re.IGNORECASE)
    if legacy_match:
        num = int(legacy_match.group(1))
        return f"krutilka_{num:03d}"
    return text


def _ensure_legacy_slot(start_dt: datetime, athlete_name: str, scheduled_name: Optional[str]) -> Optional[Dict]:
    """
    Ensure there is a slot/reservation to attach legacy activity when no reservation match was found.

    Creates week/slot/reservation marked as legacy with the athlete name.
    """

    local_dt = start_dt.astimezone(LOCAL_TZ) if start_dt.tzinfo else start_dt
    slot_date = local_dt.date()
    duration = timedelta(seconds=90 * 60)
    end_dt = local_dt + duration
    label = scheduled_name or "Самокрутка"

    try:
        week = schedule_repository.get_or_create_week(week_start_date=slot_date, title="Legacy импорт")
    except Exception as exc:  # noqa: BLE001
        log.warning("Legacy import: failed to get/create week for %s: %s", slot_date, exc)
        return None

    slot = None
    try:
        slot = schedule_repository.create_slot(
            week_id=week["id"],
            slot_date=slot_date,
            start_time=local_dt.time(),
            end_time=end_dt.time(),
            label=label,
            session_kind="legacy",
            notes="Создано при импорте history_legacy.json",
        )
    except Exception as exc:  # noqa: BLE001
        log.info("Legacy import: slot create conflict, try reuse: %s", exc)
        try:
            with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
                cur.execute(
                    """
                    SELECT * FROM schedule_slots
                    WHERE week_id = %s AND slot_date = %s AND start_time = %s
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (week["id"], slot_date, local_dt.time()),
                )
                slot = cur.fetchone()
        except Exception:
            slot = None

    if not slot:
        return None

    try:
        reservation = schedule_repository.create_reservation(
            slot_id=slot["id"],
            stand_id=None,
            stand_code=None,
            client_id=None,
            client_name=athlete_name,
            status="legacy",
            source="legacy_import",
            notes="Добавлено из history_legacy.json",
        )
        return reservation
    except Exception as exc:  # noqa: BLE001
        log.warning("Legacy import: failed to create reservation in slot %s: %s", slot.get("id"), exc)
        return None


def _flatten_message_text(message: Dict) -> str:
    raw = message.get("text")
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: List[str] = []
        for item in raw:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("text"):
                parts.append(str(item.get("text")))
        return "".join(parts)
    return ""


def _normalize_account_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = str(raw).strip()
    legacy_match = re.match(r"krutilkavn0*([1-9]\\d*)", text, re.IGNORECASE)
    if legacy_match:
        num = int(legacy_match.group(1))
        return f"krutilka_{num:03d}"
    return text


def _extract_bold_values(message: Dict) -> List[str]:
    values: List[str] = []
    for item in message.get("text_entities", []) or []:
        if isinstance(item, dict) and item.get("type") == "bold":
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                values.append(text.strip())
    return values


def _parse_duration_seconds(raw: str) -> Optional[int]:
    if not raw:
        return None
    hours = minutes = seconds = 0
    hours_match = re.search(r"(\d+)\s*ч", raw)
    minutes_match = re.search(r"(\d+)\s*м", raw)
    seconds_match = re.search(r"(\d+)\s*с", raw)
    if hours_match:
        hours = int(hours_match.group(1))
    if minutes_match:
        minutes = int(minutes_match.group(1))
    if seconds_match:
        seconds = int(seconds_match.group(1))
    if hours == minutes == seconds == 0:
        return None
    return hours * 3600 + minutes * 60 + seconds


def _parse_legacy_message(message: Dict) -> Optional[Dict]:
    file_name = message.get("file_name") or ""
    match = re.search(r"activity_(\d+)", str(file_name))
    if not match:
        return None

    activity_id = match.group(1)
    bold_values = _extract_bold_values(message)
    account_id = _normalize_account_id_value(bold_values[0] if bold_values else None)
    scheduled_name = bold_values[1] if len(bold_values) > 1 else None

    text_blob = _flatten_message_text(message)
    athlete_match = re.search(r"Атлет:\s*([^\n]+)", text_blob, flags=re.IGNORECASE)
    athlete_name = athlete_match.group(1).strip() if athlete_match else None

    date_match = re.search(r"Дата:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2})", text_blob)
    start_time: Optional[datetime] = None
    if date_match:
        try:
            start_time = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M")
            start_time = start_time.replace(tzinfo=LOCAL_TZ)
        except Exception:
            start_time = None

    def _parse_metric(label: str) -> Optional[float]:
        metric_match = re.search(rf"{label}:\s*([\d.,]+)", text_blob)
        if metric_match:
            try:
                return float(metric_match.group(1).replace(",", "."))
            except ValueError:
                return None
        return None

    distance_km = _parse_metric("Дистанция")
    elevation_gain = _parse_metric("Набор высоты")
    avg_power = _parse_metric("Средняя мощность")
    avg_cadence = _parse_metric("Средний каденс")
    avg_heartrate = _parse_metric("Средний пульс")

    elapsed_match = re.search(r"Время:\s*([^\n]+)", text_blob)
    elapsed_seconds = _parse_duration_seconds(elapsed_match.group(1)) if elapsed_match else None

    if not account_id:
        return None

    return {
        "account_id": account_id,
        "activity_id": activity_id,
        "scheduled_name": scheduled_name,
        "start_time": start_time,
        "athlete_name": athlete_name,
        "distance": distance_km * 1000 if distance_km is not None else None,
        "elapsed_time": elapsed_seconds,
        "elevation_gain": elevation_gain,
        "average_power": avg_power,
        "average_cadence": avg_cadence,
        "average_heartrate": avg_heartrate,
    }


def _parse_structured_entry(entry: Dict) -> Optional[Dict]:
    """Parse new structured JSON object with explicit fields."""
    if not isinstance(entry, dict):
        return None
    activity_id = entry.get("activity_id")
    account_id = _normalize_account_id_value(entry.get("account") or entry.get("account_id"))
    date_str = entry.get("date")
    athlete_name = entry.get("athlete")
    if not (activity_id and account_id and date_str):
        return None
    start_time: Optional[datetime] = None
    try:
        start_time = datetime.strptime(str(date_str), "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
    except Exception:
        start_time = None
    elapsed_seconds = entry.get("time_seconds")
    if not elapsed_seconds and entry.get("time"):
        elapsed_seconds = _parse_duration_seconds(str(entry["time"]))
    return {
        "account_id": str(account_id),
        "activity_id": str(activity_id),
        "scheduled_name": None,
        "start_time": start_time,
        "athlete_name": athlete_name,
        "distance": float(entry.get("distance")) * 1000 if entry.get("distance") is not None else None,
        "elapsed_time": int(elapsed_seconds) if elapsed_seconds is not None else None,
        "elevation_gain": float(entry.get("elevation_gain")) if entry.get("elevation_gain") is not None else None,
        "average_power": float(entry.get("avg_power")) if entry.get("avg_power") is not None else None,
        "average_cadence": float(entry.get("cadence")) if entry.get("cadence") is not None else None,
        "average_heartrate": float(entry.get("heartrate")) if entry.get("heartrate") is not None else None,
    }


def _import_legacy_payload(payload: Dict | List[Dict]) -> Dict[str, object]:
    schedule_repository.ensure_activity_ids_table()

    if isinstance(payload, list):
        items = payload
    else:
        messages = payload.get("messages")
        if not isinstance(messages, list):
            raise ValueError("payload must be a list or object with messages[]")
        items = messages

    processed = 0
    stored = 0
    matched = 0
    skipped = 0
    errors: List[str] = []
    log_lines: List[str] = []
    max_log = 400
    created_reservations = 0

    for message in items:
        if isinstance(payload, list):
            parsed = _parse_structured_entry(message)
        else:
            parsed = _parse_legacy_message(message) if isinstance(message, dict) else None
        if not parsed:
            skipped += 1
            log_lines.append("skip: не распознано (нет activity_id/account)")
            continue

        processed += 1
        client_id: Optional[int] = None
        scheduled_name = parsed.get("scheduled_name")
        account_id = parsed.get("account_id")

        start_time = parsed.get("start_time")
        athlete_name = parsed.get("athlete_name")
        match_row = None
        if start_time and athlete_name:
            try:
                match_row = schedule_repository.find_reservation_by_client_name(
                    start_time,
                    athlete_name,
                    statuses=("booked", "legacy", "pending", "waitlist"),
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{parsed['activity_id']}: match failed ({exc})")
                match_row = None

            if match_row and match_row.get("client_id"):
                client_id = match_row.get("client_id")
                matched += 1
                scheduled_name = scheduled_name or match_row.get("label") or match_row.get("session_kind")
                log_lines.append(
                    f"{parsed['activity_id']} {parsed['account_id']}: {athlete_name} → client #{client_id}"
                )
            else:
                # Create synthetic slot/reservation so запись попала в расписание
                reservation = _ensure_legacy_slot(start_time, athlete_name, scheduled_name)
                if reservation and athlete_name:
                    try:
                        matches = client_repository.search_clients(athlete_name, limit=10)
                    except Exception as exc:  # noqa: BLE001
                        matches = []
                        log_lines.append(f"{parsed['activity_id']} {parsed['account_id']}: поиск клиента упал ({exc})")
                    if matches:
                        client_id = matches[0].get("id")
                        try:
                            schedule_repository.update_reservation(reservation["id"], client_id=client_id, status="booked")
                            matched += 1
                            log_lines.append(
                                f"{parsed['activity_id']} {parsed['account_id']}: legacy бронь #{reservation.get('id')} привязана к client #{client_id}"
                            )
                        except Exception as exc:  # noqa: BLE001
                            log_lines.append(
                                f"{parsed['activity_id']} {parsed['account_id']}: не смогли привязать клиента к legacy брони ({exc})"
                            )
                if reservation:
                    created_reservations += 1
                    log_lines.append(
                        f"{parsed['activity_id']} {parsed['account_id']}: создан legacy-слот/бронирование #{reservation.get('id')} "
                        f"на {start_time.date()} {start_time.time()}"
                    )
                    match_row = reservation
                else:
                    log_lines.append(f"{parsed['activity_id']} {parsed['account_id']}: нет совпадения по имени")
        else:
            log_lines.append(f"{parsed['activity_id']} {parsed['account_id']}: нет даты/имени для сопоставления")

        try:
            saved = schedule_repository.record_seen_activity_id(
                str(parsed["account_id"]),
                str(parsed["activity_id"]),
                client_id=client_id,
                scheduled_name=athlete_name if not client_id else scheduled_name or athlete_name,
                start_time=start_time,
                profile_name=athlete_name,
                distance=parsed.get("distance"),
                elapsed_time=parsed.get("elapsed_time"),
                elevation_gain=parsed.get("elevation_gain"),
                average_power=parsed.get("average_power"),
                average_cadence=parsed.get("average_cadence"),
                average_heartrate=parsed.get("average_heartrate"),
            )
            if saved:
                stored += 1
                log_lines.append(
                    f"{parsed['activity_id']} {parsed['account_id']}: записано (distance={parsed.get('distance')}, elapsed={parsed.get('elapsed_time')})"
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{parsed['activity_id']}: {exc}")
            log_lines.append(f"{parsed['activity_id']} {parsed['account_id']}: ошибка записи ({exc})")

        if len(log_lines) > max_log:
            overflow = len(log_lines) - max_log
            if overflow > 0:
                log_lines = log_lines[overflow:]

    return {
        "processed": processed,
        "stored": stored,
        "matched": matched,
        "skipped": skipped,
        "errors": errors,
        "log": log_lines,
        "created_reservations": created_reservations,
    }


def _normalize_seen_activity_accounts() -> Dict[str, object]:
    """Rename legacy account_ids in seen_activity_ids and move FIT files."""

    schedule_repository.ensure_activity_ids_table()
    with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT account_id
            FROM seen_activity_ids
            WHERE account_id ~ '^krutilkavn[0-9]+'
            """
        )
        legacy_ids = [row["account_id"] for row in cur.fetchall()]

    changes: Dict[str, str] = {}
    for old_id in legacy_ids:
        new_id = _normalize_account_id_value(old_id) or old_id
        if new_id != old_id:
            changes[old_id] = new_id

    migrated = 0
    skipped_conflicts = 0
    moved_files = 0
    errors: List[str] = []

    for old_id, new_id in changes.items():
        try:
            with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
                cur.execute(
                    """
                    INSERT INTO seen_activity_ids (
                        account_id, activity_id, client_id, scheduled_name, start_time,
                        profile_name, sent_clientbot, sent_strava, sent_intervals,
                        distance, elapsed_time, elevation_gain, average_power,
                        average_cadence, average_heartrate, fit_path
                    )
                    SELECT
                        %(new_id)s AS account_id,
                        activity_id,
                        client_id,
                        scheduled_name,
                        start_time,
                        profile_name,
                        sent_clientbot,
                        sent_strava,
                        sent_intervals,
                        distance,
                        elapsed_time,
                        elevation_gain,
                        average_power,
                        average_cadence,
                        average_heartrate,
                        CASE
                            WHEN fit_path IS NOT NULL THEN REPLACE(fit_path, %(old_path)s, %(new_path)s)
                            ELSE fit_path
                        END AS fit_path
                    FROM seen_activity_ids
                    WHERE account_id = %(old_id)s
                    ON CONFLICT (account_id, activity_id) DO UPDATE SET
                        client_id = COALESCE(EXCLUDED.client_id, seen_activity_ids.client_id),
                        scheduled_name = COALESCE(EXCLUDED.scheduled_name, seen_activity_ids.scheduled_name),
                        start_time = COALESCE(EXCLUDED.start_time, seen_activity_ids.start_time),
                        profile_name = COALESCE(EXCLUDED.profile_name, seen_activity_ids.profile_name),
                        sent_clientbot = seen_activity_ids.sent_clientbot OR COALESCE(EXCLUDED.sent_clientbot, FALSE),
                        sent_strava = seen_activity_ids.sent_strava OR COALESCE(EXCLUDED.sent_strava, FALSE),
                        sent_intervals = seen_activity_ids.sent_intervals OR COALESCE(EXCLUDED.sent_intervals, FALSE),
                        distance = COALESCE(EXCLUDED.distance, seen_activity_ids.distance),
                        elapsed_time = COALESCE(EXCLUDED.elapsed_time, seen_activity_ids.elapsed_time),
                        elevation_gain = COALESCE(EXCLUDED.elevation_gain, seen_activity_ids.elevation_gain),
                        average_power = COALESCE(EXCLUDED.average_power, seen_activity_ids.average_power),
                        average_cadence = COALESCE(EXCLUDED.average_cadence, seen_activity_ids.average_cadence),
                        average_heartrate = COALESCE(EXCLUDED.average_heartrate, seen_activity_ids.average_heartrate),
                        fit_path = COALESCE(EXCLUDED.fit_path, seen_activity_ids.fit_path)
                    """,
                    {
                        "new_id": new_id,
                        "old_id": old_id,
                        "old_path": f"/fitfiles/{old_id}/",
                        "new_path": f"/fitfiles/{new_id}/",
                    },
                )
                cur.execute("DELETE FROM seen_activity_ids WHERE account_id = %s", (old_id,))
                migrated += cur.rowcount
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed to migrate account_id %s -> %s", old_id, new_id)
            errors.append(f"{old_id}: {exc}")
            continue

        # Move FIT files on disk
        base_dir = schedule_repository.ensure_fit_files_dir()
        src_dir = base_dir / old_id
        dst_dir = base_dir / new_id
        if src_dir.exists():
            dst_dir.mkdir(parents=True, exist_ok=True)
            for path in src_dir.iterdir():
                if not path.is_file():
                    continue
                dest = dst_dir / path.name
                if dest.exists():
                    skipped_conflicts += 1
                    continue
                try:
                    path.rename(dest)
                    moved_files += 1
                except Exception as exc:  # noqa: BLE001
                    log.warning("Failed to move FIT %s -> %s (%s)", path, dest, exc)
            try:
                src_dir.rmdir()
            except OSError:
                pass

    return {
        "legacy_accounts": legacy_ids,
        "updated": len(changes),
        "migrated_rows": migrated,
        "moved_files": moved_files,
        "conflicts": skipped_conflicts,
        "errors": errors,
    }


def _process_accounts(accounts_path: Path, timeout: float) -> None:
    schedule_repository.ensure_activity_ids_table()
    try:
        accounts = load_accounts(accounts_path)
    except Exception as exc:  # noqa: BLE001
        SYNC_STATE.finish(error=f"Failed to load accounts: {exc}")
        return

    SYNC_STATE.start(len(accounts))

    for account_id, account in accounts.items():
        SYNC_STATE.current_account = account_id
        account_processed = 0
        account_updated = 0
        account_errors = 0

        client = WattAttackClient(account.get("base_url", DEFAULT_BASE_URL))
        try:
            client.login(account["email"], account["password"], timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            SYNC_STATE.append_log(f"{account_id}: ошибка логина ({exc})")
            account_errors += 1
            SYNC_STATE.summary[account_id] = {
                "processed": account_processed,
                "updated": account_updated,
                "error": account_errors,
            }
            SYNC_STATE.accounts_done += 1
            continue

        try:
            profile = client.fetch_profile(timeout=timeout)
        except Exception:
            profile = {}
        profile_name = extract_athlete_name(profile) if profile else None

        try:
            activities, _ = client.fetch_activity_feed(limit=2000, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            SYNC_STATE.append_log(f"{account_id}: ошибка чтения ленты ({exc})")
            account_errors += 1
            SYNC_STATE.summary[account_id] = {
                "processed": account_processed,
                "updated": account_updated,
                "error": account_errors,
            }
            SYNC_STATE.accounts_done += 1
            continue

        SYNC_STATE.append_log(f"{account_id}: найдено {len(activities)} активностей")

        total_for_account = len(activities)

        for activity in activities:
            activity_id = str(activity.get("id") or "")
            if not activity_id:
                continue
            account_processed += 1
            SYNC_STATE.processed += 1
            SYNC_STATE.append_log(
                f"{account_id}: обрабатываем {activity_id} ({account_processed}/{total_for_account})"
            )

            start_dt = parse_activity_start_dt(activity)
            scheduled_match = resolve_scheduled_client(account, activity)
            scheduled_client_id: Optional[int] = None
            scheduled_name: Optional[str] = None
            if scheduled_match:
                scheduled_client_id = scheduled_match.get("client_id")
                scheduled_name = scheduled_match.get("client_name")

            fit_path: Optional[str] = None
            fit_id = activity.get("fitFileId")
            if fit_id:
                dest_file = _fit_storage_path(account_id, activity_id)
                if not dest_file.exists():
                    try:
                        client.download_fit_file(str(fit_id), dest_file, timeout=timeout)
                        SYNC_STATE.fit_downloaded += 1
                    except Exception:
                        dest_file.unlink(missing_ok=True)
                if dest_file.exists():
                    fit_path = f"/fitfiles/{account_id}/{activity_id}.fit"

            stored = schedule_repository.record_seen_activity_id(
                account_id,
                activity_id,
                client_id=scheduled_client_id,
                scheduled_name=scheduled_name,
                start_time=start_dt,
                profile_name=profile_name,
                distance=activity.get("distance"),
                elapsed_time=activity.get("elapsedTime"),
                elevation_gain=activity.get("totalElevationGain"),
                average_power=activity.get("averageWatts"),
                average_cadence=activity.get("averageCadence"),
                average_heartrate=activity.get("averageHeartrate"),
                fit_path=fit_path,
            )
            if stored:
                account_updated += 1
                SYNC_STATE.updated += 1

        SYNC_STATE.summary[account_id] = {
            "processed": account_processed,
            "updated": account_updated,
            "error": account_errors,
        }
        SYNC_STATE.accounts_done += 1

    SYNC_STATE.current_account = None
    SYNC_STATE.finish()


def _resolve_fit_file_path(activity_row: dict) -> Optional[Path]:
    """Return a local path to the archived FIT file if it exists."""
    base_dir = schedule_repository.ensure_fit_files_dir()
    candidates: List[Path] = []

    raw_fit_path = activity_row.get("fit_path")
    if isinstance(raw_fit_path, str) and raw_fit_path:
        trimmed = raw_fit_path.lstrip("/")
        if trimmed.startswith("fitfiles/"):
            trimmed = trimmed[len("fitfiles/") :]
        candidates.append(base_dir / trimmed)
    account_id = activity_row.get("account_id")
    activity_id = activity_row.get("activity_id")
    if account_id and activity_id:
        candidates.append(base_dir / str(account_id) / f"{activity_id}.fit")

    for candidate in candidates:
        path = candidate.with_suffix(".fit") if not candidate.suffix else candidate
        if path.exists():
            return path
    return None


def _build_strava_payload(activity_row: dict) -> tuple[str, str]:
    """Construct upload name/description for Strava backfill."""
    start_dt = activity_row.get("start_time") or activity_row.get("created_at")
    start_iso = start_dt.isoformat() if hasattr(start_dt, "isoformat") else None
    display_name = activity_row.get("scheduled_name") or activity_row.get("profile_name") or "Крутилка"
    name_parts = ["Крутилка"]
    if start_dt:
        try:
            name_parts.append(start_dt.strftime("%Y-%m-%d"))
        except Exception:  # noqa: BLE001
            name_parts.append(str(start_dt))
    if activity_row.get("scheduled_name"):
        name_parts.append(activity_row["scheduled_name"])
    upload_name = " · ".join(name_parts)

    activity_payload = {
        "name": display_name,
        "startTime": start_iso,
        "distance": activity_row.get("distance"),
        "elapsedTime": activity_row.get("elapsed_time"),
        "totalElevationGain": activity_row.get("elevation_gain"),
        "averageWatts": activity_row.get("average_power"),
        "averageCadence": activity_row.get("average_cadence"),
        "averageHeartrate": activity_row.get("average_heartrate"),
    }
    description = format_strava_activity_description(
        activity_payload,
        activity_row.get("account_id"),
        profile=None,
        scheduled_name=activity_row.get("scheduled_name"),
    )
    return upload_name, description


def _backfill_strava(tg_user_ids: Sequence[int], max_per_user: int) -> None:
    """Upload archived FIT files to Strava for selected Telegram users."""
    STRAVA_STATE.start(len(tg_user_ids))
    straver = StraverClient()
    if not straver.is_configured():
        STRAVA_STATE.finish(error="Straver client is not configured")
        return

    try:
        statuses = straver.connection_status(tg_user_ids)
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to fetch Strava statuses")
        STRAVA_STATE.finish(error=f"Не удалось получить статусы Strava: {exc}")
        return

    for tg_user_id in tg_user_ids:
        summary_key = str(tg_user_id)
        STRAVA_STATE.current_user = summary_key
        uploaded = 0
        skipped = 0

        try:
            link = client_link_repository.get_link_by_user(int(tg_user_id))
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed to load client link for %s", tg_user_id)
            STRAVA_STATE.append_log(f"{tg_user_id}: ошибка чтения связки ({exc})")
            STRAVA_STATE.summary[summary_key] = {"uploaded": uploaded, "skipped": skipped, "error": 1}
            STRAVA_STATE.users_done += 1
            continue

        if not link:
            STRAVA_STATE.append_log(f"{tg_user_id}: нет связанного клиента, пропускаем")
            STRAVA_STATE.summary[summary_key] = {"uploaded": uploaded, "skipped": skipped, "error": 1}
            STRAVA_STATE.users_done += 1
            continue

        status_row = statuses.get(int(tg_user_id)) or {}
        if not status_row.get("connected"):
            STRAVA_STATE.append_log(f"{tg_user_id}: Strava не подключена, пропускаем")
            STRAVA_STATE.summary[summary_key] = {"uploaded": uploaded, "skipped": skipped, "error": 1}
            STRAVA_STATE.users_done += 1
            continue

        activities = schedule_repository.list_strava_backfill_activities(
            link["client_id"],
            limit=max_per_user,
        )
        STRAVA_STATE.append_log(f"{tg_user_id}: найдено {len(activities)} активностей для загрузки")

        for activity in activities:
            file_path = _resolve_fit_file_path(activity)
            if not file_path:
                STRAVA_STATE.append_log(
                    f"{tg_user_id}: {activity.get('activity_id')} — нет FIT-файла, пропускаем"
                )
                skipped += 1
                STRAVA_STATE.skipped += 1
                continue

            upload_name, description = _build_strava_payload(activity)
            try:
                straver.upload_activity(
                    tg_user_id=int(tg_user_id),
                    file_path=file_path,
                    name=upload_name,
                    description=description,
                )
                account_id = activity.get("account_id")
                activity_id = activity.get("activity_id")
                if account_id and activity_id:
                    schedule_repository.record_seen_activity_id(
                        str(account_id),
                        str(activity_id),
                        sent_strava=True,
                    )
                uploaded += 1
                STRAVA_STATE.uploaded += 1
                STRAVA_STATE.append_log(f"{tg_user_id}: загружено {activity.get('activity_id')}")
            except Exception as exc:  # noqa: BLE001
                log.exception("Failed to upload activity %s for user %s", activity.get("activity_id"), tg_user_id)
                STRAVA_STATE.append_log(
                    f"{tg_user_id}: ошибка загрузки {activity.get('activity_id')} ({exc})"
                )
                skipped += 1
                STRAVA_STATE.skipped += 1

        STRAVA_STATE.summary[summary_key] = {
            "pending": len(activities),
            "uploaded": uploaded,
            "skipped": skipped,
        }
        STRAVA_STATE.users_done += 1

    STRAVA_STATE.current_user = None
    STRAVA_STATE.finish()


def _backfill_intervals(tg_user_ids: Sequence[int], max_per_user: int) -> None:
    """Upload archived FIT files to Intervals.icu for selected Telegram users."""
    INTERVALS_STATE.start(len(tg_user_ids))

    for tg_user_id in tg_user_ids:
        summary_key = str(tg_user_id)
        INTERVALS_STATE.current_user = summary_key
        uploaded = 0
        skipped = 0

        try:
            link = client_link_repository.get_link_by_user(int(tg_user_id))
        except Exception as exc:  # noqa: BLE001
            log.exception("Intervals: failed to load client link for %s", tg_user_id)
            INTERVALS_STATE.append_log(f"{tg_user_id}: ошибка чтения связки ({exc})")
            INTERVALS_STATE.summary[summary_key] = {"uploaded": uploaded, "skipped": skipped, "error": 1}
            INTERVALS_STATE.users_done += 1
            continue

        if not link:
            INTERVALS_STATE.append_log(f"{tg_user_id}: нет связанного клиента, пропускаем")
            INTERVALS_STATE.summary[summary_key] = {"uploaded": uploaded, "skipped": skipped, "error": 1}
            INTERVALS_STATE.users_done += 1
            continue

        intervals_link = intervals_link_repository.get_link(int(tg_user_id))
        if not intervals_link or not intervals_link.get("intervals_api_key"):
            INTERVALS_STATE.append_log(f"{tg_user_id}: Intervals не подключен, пропускаем")
            INTERVALS_STATE.summary[summary_key] = {"uploaded": uploaded, "skipped": skipped, "error": 1}
            INTERVALS_STATE.users_done += 1
            continue

        activities = schedule_repository.list_intervals_backfill_activities(
            link["client_id"],
            limit=max_per_user,
        )
        INTERVALS_STATE.append_log(f"{tg_user_id}: найдено {len(activities)} активностей для загрузки")

        for activity in activities:
            file_path = _resolve_fit_file_path(activity)
            if not file_path:
                INTERVALS_STATE.append_log(
                    f"{tg_user_id}: {activity.get('activity_id')} — нет FIT-файла, пропускаем"
                )
                skipped += 1
                INTERVALS_STATE.skipped += 1
                continue

            upload_name, description = _build_strava_payload(activity)
            try:
                intervals_sync.upload_activity(
                    tg_user_id=int(tg_user_id),
                    temp_file=file_path,
                    description=description,
                    activity_id=activity.get("activity_id"),
                    timeout=STRAVER_HTTP_TIMEOUT,
                    activity_name=upload_name,
                )
                account_id = activity.get("account_id")
                activity_id = activity.get("activity_id")
                if account_id and activity_id:
                    schedule_repository.record_seen_activity_id(
                        str(account_id),
                        str(activity_id),
                        sent_intervals=True,
                    )
                uploaded += 1
                INTERVALS_STATE.uploaded += 1
                INTERVALS_STATE.append_log(f"{tg_user_id}: загружено {activity.get('activity_id')}")
            except Exception as exc:  # noqa: BLE001
                log.exception("Failed to upload activity %s for user %s to Intervals", activity.get("activity_id"), tg_user_id)
                INTERVALS_STATE.append_log(
                    f"{tg_user_id}: ошибка загрузки {activity.get('activity_id')} ({exc})"
                )
                skipped += 1
                INTERVALS_STATE.skipped += 1

        INTERVALS_STATE.summary[summary_key] = {
            "pending": len(activities),
            "uploaded": uploaded,
            "skipped": skipped,
        }
        INTERVALS_STATE.users_done += 1

    INTERVALS_STATE.current_user = None
    INTERVALS_STATE.finish()


@router.post("/activities")
def api_sync_activities():
    """Kick off background sync of historical WattAttack activities."""
    if SYNC_STATE.running:
        raise HTTPException(status.HTTP_409_CONFLICT, "Sync already running")

    accounts_path = Path(os.environ.get("WATTATTACK_ACCOUNTS_FILE", "accounts.json"))
    timeout = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))

    thread = threading.Thread(target=_process_accounts, args=(accounts_path, timeout), daemon=True)
    thread.start()

    return {"status": "started"}


@router.get("/status")
def api_sync_status():
    """Return current sync progress/state."""
    return SYNC_STATE.to_dict()


@router.post("/status/clear")
def api_sync_clear_logs():
    """Clear sync logs and cached state."""
    try:
        SYNC_STATE.log = []
        STRAVA_STATE.log = []
        INTERVALS_STATE.log = []
        return {"status": "cleared"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to clear logs") from exc



@router.post("/legacy/import")
async def api_import_legacy_history(file: UploadFile = File(...)):
    """Upload Telegram export with historical FIT notifications and backfill schedule."""

    try:
        raw_bytes = await file.read()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Не удалось прочитать файл: {exc}") from exc

    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Файл должен быть JSON") from exc

    try:
        result = _import_legacy_payload(payload)
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to import legacy history")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось импортировать историю") from exc

    return result


@router.post("/legacy/normalize_accounts")
def api_normalize_legacy_accounts():
    """Rename legacy krutilkavn account_ids to krutilka_XXX and move FIT files."""
    try:
        result = _normalize_seen_activity_accounts()
        return result
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to normalize legacy accounts")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось нормализовать аккаунты") from exc


@router.get("/strava/candidates")
def api_strava_candidates():
    """List Telegram users with connected Strava accounts and pending activities."""
    try:
        links = client_link_repository.list_links()
        tg_user_ids = [row["tg_user_id"] for row in links if row.get("tg_user_id")]

        straver = StraverClient()
        statuses: Dict[int, Dict] = {}
        straver_configured = straver.is_configured()
        if straver_configured and tg_user_ids:
            try:
                statuses = straver.connection_status(tg_user_ids)
            except Exception:  # noqa: BLE001
                log.exception("Failed to fetch Strava connection statuses")

        stats = schedule_repository.list_strava_backfill_stats([row["client_id"] for row in links])
        stats_map = {int(row["client_id"]): row for row in stats if row.get("client_id") is not None}

        items = []
        for row in links:
            tg_user_id = row.get("tg_user_id")
            status_row = statuses.get(int(tg_user_id)) if tg_user_id else {}
            if not status_row or not status_row.get("connected"):
                continue

            stats_row = stats_map.get(row["client_id"], {}) if row.get("client_id") else {}
            items.append(
                {
                    "client_id": row.get("client_id"),
                    "client_name": row.get("client_name"),
                    "tg_user_id": tg_user_id,
                    "strava_athlete_name": status_row.get("athlete_name"),
                    "pending": int(stats_row.get("pending") or 0),
                    "with_fit": int(stats_row.get("with_fit") or 0),
                    "last_activity_at": stats_row.get("last_activity_at"),
                }
            )
        return {"items": items, "straver_configured": straver_configured}
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to list Strava candidates")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to list Strava candidates") from exc


@router.post("/strava/backfill")
def api_strava_backfill(payload: dict = Body(...)):
    """Kick off Strava backfill for selected Telegram users."""
    if STRAVA_STATE.running:
        raise HTTPException(status.HTTP_409_CONFLICT, "Strava backfill already running")

    tg_ids_raw = payload.get("tg_user_ids")
    if not isinstance(tg_ids_raw, list):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_ids must be a list")

    tg_user_ids: List[int] = []
    for value in tg_ids_raw:
        try:
            uid = int(value)
        except (TypeError, ValueError) as exc:  # noqa: BLE001
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_ids must contain integers") from exc
        if uid > 0:
            tg_user_ids.append(uid)

    if not tg_user_ids:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No valid tg_user_ids provided")

    max_per_user_raw = payload.get("max_per_user")
    try:
        max_per_user = int(max_per_user_raw) if max_per_user_raw is not None else 50
    except (TypeError, ValueError):
        max_per_user = 50
    max_per_user = max(1, min(max_per_user, 500))

    straver = StraverClient()
    if not straver.is_configured():
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Straver client is not configured")

    thread = threading.Thread(
        target=_backfill_strava,
        args=(tg_user_ids, max_per_user),
        daemon=True,
    )
    thread.start()

    return {"status": "started", "users": len(tg_user_ids)}


@router.get("/strava/status")
def api_strava_backfill_status():
    """Return current Strava backfill state."""
    return STRAVA_STATE.to_dict()


@router.get("/intervals/candidates")
def api_intervals_candidates():
    """Return Intervals.icu backfill candidates."""
    try:
        intervals_links = intervals_link_repository.list_links()
        client_links = client_link_repository.list_links()
        link_by_tg = {row["tg_user_id"]: row for row in client_links if row.get("tg_user_id")}
        stats = schedule_repository.list_intervals_backfill_stats(
            [row.get("client_id") for row in client_links if row.get("client_id")]
        )
        stats_map = {row["client_id"]: row for row in stats}

        items: list[dict] = []
        for link in intervals_links:
            tg_user_id = link.get("tg_user_id")
            cl = link_by_tg.get(tg_user_id) or {}
            stats_row = stats_map.get(cl.get("client_id")) if cl.get("client_id") else {}
            items.append(
                {
                    "client_id": cl.get("client_id"),
                    "client_name": cl.get("client_name"),
                    "tg_user_id": tg_user_id,
                    "pending": int(stats_row.get("pending") or 0),
                    "with_fit": int(stats_row.get("with_fit") or 0),
                    "last_activity_at": stats_row.get("last_activity_at"),
                }
            )
        return {"items": items}
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to list Intervals candidates")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to list Intervals candidates") from exc


@router.post("/intervals/backfill")
def api_intervals_backfill(payload: dict = Body(...)):
    """Kick off Intervals backfill for selected Telegram users."""
    if INTERVALS_STATE.running:
        raise HTTPException(status.HTTP_409_CONFLICT, "Intervals backfill already running")

    tg_ids_raw = payload.get("tg_user_ids")
    if not isinstance(tg_ids_raw, list):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_ids must be a list")

    tg_user_ids: List[int] = []
    for value in tg_ids_raw:
        try:
            uid = int(value)
        except (TypeError, ValueError) as exc:  # noqa: BLE001
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "tg_user_ids must contain integers") from exc
        if uid > 0:
            tg_user_ids.append(uid)

    if not tg_user_ids:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No valid tg_user_ids provided")

    max_per_user_raw = payload.get("max_per_user")
    try:
        max_per_user = int(max_per_user_raw) if max_per_user_raw is not None else 50
    except (TypeError, ValueError):
        max_per_user = 50
    max_per_user = max(1, min(max_per_user, 500))

    thread = threading.Thread(
        target=_backfill_intervals,
        args=(tg_user_ids, max_per_user),
        daemon=True,
    )
    thread.start()

    return {"status": "started", "users": len(tg_user_ids)}


@router.get("/intervals/status")
def api_intervals_backfill_status():
    """Return current Intervals backfill state."""
    return INTERVALS_STATE.to_dict()
