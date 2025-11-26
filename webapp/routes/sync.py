from __future__ import annotations

import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from fastapi import APIRouter, Body, Depends, HTTPException, status

from scheduler.notifier import load_accounts  # type: ignore
from scheduler.notifier_client import (  # type: ignore
    parse_activity_start_dt,
    resolve_scheduled_client,
    extract_athlete_name,
    format_strava_activity_description,
)
from wattattack_activities import WattAttackClient, DEFAULT_BASE_URL
from repositories import client_link_repository, schedule_repository
from straver_client import StraverClient
from ..dependencies import require_admin

router = APIRouter(prefix="/sync", tags=["sync"], dependencies=[Depends(require_admin)])
log = logging.getLogger(__name__)


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


def _fit_storage_path(account_id: str, activity_id: str) -> Path:
    base = schedule_repository.ensure_fit_files_dir()
    dest = base / account_id
    dest.mkdir(parents=True, exist_ok=True)
    return dest / f"{activity_id}.fit"


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
