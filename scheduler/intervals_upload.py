"""Download Intervals.icu workouts for the week and upload to WattAttack accounts."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import requests

from intervals_client import IntervalsClient
from repositories.intervals_link_repository import list_links
from repositories import intervals_uploaded_repository as uploaded_repo
from wattattack_workouts import (
    build_workout_payload,
    calculate_workout_metrics,
    parse_zwo_workout,
    zwo_to_chart_data,
)
from wattattack_activities import WattAttackClient

LOGGER = logging.getLogger(__name__)


def _decode_zwo(raw_bytes: bytes) -> str:
    text = None
    for encoding in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = raw_bytes.decode("utf-8", errors="ignore")
    return text


def _fetch_ftp(client: WattAttackClient, timeout: float) -> float | None:
    try:
        profile = client.fetch_profile(timeout=timeout)
        ftp_raw = profile.get("ftp") if isinstance(profile, dict) else None
        if ftp_raw in (None, "", "—"):
            return None
        ftp_value = float(ftp_raw)
        return ftp_value if ftp_value > 0 else None
    except Exception:  # noqa: BLE001
        return None


def _notify_user(bot_token: str, tg_user_id: int, text: str, timeout: float) -> None:
    if not bot_token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": tg_user_id, "text": text},
            timeout=timeout,
        )
    except Exception:
        LOGGER.exception("Failed to notify user %s about Intervals upload", tg_user_id)


def _upload_to_account(
    account: Dict[str, Any],
    workout: Dict[str, Any],
    chart_data: List[Dict[str, Any]],
    *,
    timeout: float,
) -> Tuple[bool, str]:
    def worker():
        client = WattAttackClient(account["base_url"])
        client.login(account["email"], account["password"], timeout=timeout)
        ftp = _fetch_ftp(client, timeout)
        metrics = calculate_workout_metrics(workout, ftp)
        payload = build_workout_payload(workout, chart_data, metrics)
        return client.upload_workout(payload, timeout=timeout)

    try:
        resp = worker()
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)

    if isinstance(resp, dict):
        msg = resp.get("message") or ""
        workout_obj = resp.get("workout")
        if workout_obj and isinstance(workout_obj, dict) and workout_obj.get("id"):
            wid = workout_obj["id"]
            msg = msg or "Загружено"
            msg = f"{msg} (ID {wid})"
        if not msg:
            msg = "Загружено"
    else:
        msg = "Загружено"
    return True, msg


def sync_intervals_workouts(
    *,
    accounts: Dict[str, Dict[str, Any]],
    bot_token: str,
    timeout: float,
) -> None:
    """Download planned workouts for next 7 days and upload to all WattAttack accounts."""
    links = list_links()
    if not links:
        LOGGER.debug("No Intervals.icu links found; skipping sync")
        return

    start_date = date.today()
    end_date = start_date + timedelta(days=7)
    oldest = start_date.isoformat()
    newest = end_date.isoformat()

    for link in links:
        tg_user_id = link.get("tg_user_id")
        api_key = link.get("intervals_api_key")
        athlete_id = link.get("intervals_athlete_id") or "0"
        if not tg_user_id or not api_key:
            continue

        try:
            client = IntervalsClient(api_key=api_key, athlete_id=athlete_id, timeout=timeout)
            events = client.fetch_events(
                category="WORKOUT",
                oldest=oldest,
                newest=newest,
                resolve=True,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to fetch Intervals events for user %s", tg_user_id)
            continue

        for ev in events:
            event_id = ev.get("id")
            if event_id is None:
                continue
            try:
                event_id_int = int(event_id)
            except (TypeError, ValueError):
                continue

            date_str = (ev.get("start_date_local") or "")[:10] or None
            name = ev.get("name") or "Без названия"
            per_account_status: Dict[str, bool] = {}
            event_had_new_records = False

            # Download ZWO once per event
            zwo_bytes = None
            try:
                if date_str:
                    zwo_bytes = client.download_event_zwo(oldest=date_str, newest=date_str)
                if not zwo_bytes:
                    workout_obj = ev.get("workout_doc") or ev
                    if workout_obj:
                        zwo_bytes = client.download_workout_as_zwo(workout_obj)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to download ZWO for event %s (user %s)", event_id, tg_user_id)
                continue

            if not zwo_bytes:
                LOGGER.warning("No ZWO data for event %s (user %s)", event_id, tg_user_id)
                continue

            try:
                workout_text = _decode_zwo(zwo_bytes)
                workout_obj = parse_zwo_workout(workout_text)
                chart_data = zwo_to_chart_data(workout_obj)
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                if "at least one segment" in msg.lower():
                    # Skip empty/placeholder workouts and mark as skipped to avoid retries
                    for account_id in accounts.keys():
                        uploaded_repo.record_upload(
                            tg_user_id=tg_user_id,
                            event_id=event_id_int,
                            account_id=account_id,
                            status="skipped_empty",
                            info=msg,
                        )
                    LOGGER.info("Skipped empty workout event %s for user %s (no segments)", event_id, tg_user_id)
                    continue
                LOGGER.exception("Failed to parse ZWO for event %s (user %s)", event_id, tg_user_id)
                continue

            for account_id, account in accounts.items():
                if uploaded_repo.is_uploaded(tg_user_id, event_id_int, account_id):
                    per_account_status[account_id] = True
                    continue
                success, info = _upload_to_account(account, workout_obj, chart_data, timeout=timeout)
                status = "success" if success else "error"
                # Treat duplicate errors as success to avoid retries
                if not success and "duplicate" in info.lower():
                    status = "duplicate"
                    success = True
                uploaded_repo.record_upload(
                    tg_user_id=tg_user_id,
                    event_id=event_id_int,
                    account_id=account_id,
                    status=status,
                    info=info,
                )
                event_had_new_records = True
                per_account_status[account_id] = success
                LOGGER.info(
                    "Intervals upload %s for user %s to account %s: %s",
                    event_id,
                    tg_user_id,
                    account_id,
                    "OK" if success else "FAIL",
                )

            if bot_token and accounts:
                # Avoid spamming the user if nothing new happened for this event
                if not event_had_new_records:
                    continue
                total_accounts = len(accounts)
                ok_count = sum(1 for v in per_account_status.values() if v)
                if ok_count == total_accounts:
                    text = f"✅ Загрузили \"{name}\" ({date_str or 'дата ?'}) во все аккаунты ({ok_count}/{total_accounts})."
                    _notify_user(bot_token, tg_user_id, text, timeout)
                else:
                    text = (
                        f"⚠️ Загрузка \"{name}\" ({date_str or 'дата ?'}): {ok_count}/{total_accounts} аккаунтов успешно."
                    )
                    _notify_user(bot_token, tg_user_id, text, timeout)
