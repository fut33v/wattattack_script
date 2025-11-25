from __future__ import annotations

import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, status

from scheduler.notifier import load_accounts  # type: ignore
from scheduler.notifier_client import (  # type: ignore
    parse_activity_start_dt,
    resolve_scheduled_client,
    extract_athlete_name,
)
from wattattack_activities import WattAttackClient, DEFAULT_BASE_URL
from repositories import schedule_repository
from ..dependencies import require_admin

router = APIRouter(prefix="/sync", tags=["sync"], dependencies=[Depends(require_admin)])


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
