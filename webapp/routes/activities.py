from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status

from repositories import client_link_repository, schedule_repository, intervals_link_repository, client_repository
from ..dependencies import require_admin
from scheduler import intervals_sync
from scheduler.notifier_client import (
    format_activity_meta,
    format_strava_activity_description,
    telegram_send_document,
    telegram_send_message,
)
from straver_client import StraverClient
from .sync import _build_strava_payload, _resolve_fit_file_path

log = logging.getLogger(__name__)

router = APIRouter(prefix="/activities", tags=["activities"], dependencies=[Depends(require_admin)])


def _serialize_activity_id(activity_record: dict) -> dict:
    serialized = dict(activity_record)
    created_at = serialized.get("created_at")
    if hasattr(created_at, "isoformat"):
        serialized["created_at"] = created_at.isoformat()
    start_time = serialized.get("start_time")
    if hasattr(start_time, "isoformat"):
        serialized["start_time"] = start_time.isoformat()
    return serialized


def _serialize_activity_id_enriched(activity_record: dict) -> dict:
    serialized = _serialize_activity_id(activity_record)
    client_id = activity_record.get("client_id")
    manual_client_id = activity_record.get("manual_client_id")
    manual_client_name = activity_record.get("manual_client_name")
    scheduled_name = activity_record.get("scheduled_name")
    if client_id is not None:
        serialized["client_id"] = client_id
    if manual_client_id is not None:
        serialized["manual_client_id"] = manual_client_id
    if manual_client_name is not None:
        serialized["manual_client_name"] = manual_client_name
    if scheduled_name is not None:
        serialized["scheduled_name"] = scheduled_name
    for flag in (
        "profile_name",
        "sent_clientbot",
        "sent_strava",
        "sent_intervals",
        "start_time",
    ):
        value = activity_record.get(flag)
        if hasattr(value, "isoformat"):
            serialized[flag] = value.isoformat()
        elif value is not None:
            serialized[flag] = value
    for metric in (
        "distance",
        "elapsed_time",
        "elevation_gain",
        "average_power",
        "average_cadence",
        "average_heartrate",
        "fit_path",
    ):
        value = activity_record.get(metric)
        if value is not None:
            serialized[metric] = value
    return serialized


@router.get("")
def api_get_activity_ids(
    account_id: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    sort: Optional[str] = None,
    dir: Optional[str] = None,
):
    """Get list of activity IDs."""
    try:
        schedule_repository.ensure_activity_ids_table()
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 100:
            page_size = 50

        offset = (page - 1) * page_size
        sort_key = (sort or "created_at").lower()
        dir_key = (dir or "desc").lower()
        sort_column = "created_at" if sort_key not in {"start_time", "created_at"} else sort_key
        order_dir = "ASC" if dir_key == "asc" else "DESC"
        order_clause = f"ORDER BY {sort_column} {order_dir} NULLS LAST"

        with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
            if account_id:
                cur.execute(
                    "SELECT COUNT(*) as count FROM seen_activity_ids WHERE account_id = %s",
                    (account_id,),
                )
            else:
                cur.execute("SELECT COUNT(*) as count FROM seen_activity_ids")
            total_count = cur.fetchone()["count"]

            cur.execute(
                f"""
                SELECT *
                FROM seen_activity_ids
                {"WHERE account_id = %s" if account_id else ""}
                {order_clause}
                LIMIT %s OFFSET %s
                """,
                ((account_id,) if account_id else tuple()) + (page_size, offset),
            )
            rows = cur.fetchall()
            items = [_serialize_activity_id_enriched(row) for row in rows]

        total_pages = (total_count + page_size - 1) // page_size

        return {
            "items": items,
            "pagination": {
                "page": page,
                "pageSize": page_size,
                "total": total_count,
                "totalPages": total_pages,
            },
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch activity IDs"
        ) from exc


@router.get("/{account_id}/{activity_id}")
def api_get_activity_id(
    account_id: str,
    activity_id: str,
):
    """Return a single activity record."""
    try:
        schedule_repository.ensure_activity_ids_table()
        with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
            cur.execute(
                """
                SELECT *
                FROM seen_activity_ids
                WHERE account_id = %s AND activity_id = %s
                """,
                (account_id, activity_id),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Activity not found")
        return {"item": _serialize_activity_id_enriched(row)}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch activity"
        ) from exc


def _load_activity_row(account_id: str, activity_id: str) -> dict:
    schedule_repository.ensure_activity_ids_table()
    with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM seen_activity_ids
            WHERE account_id = %s AND activity_id = %s
            """,
            (account_id, activity_id),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Activity not found")
    return row


def _build_activity_payload(row: dict) -> dict:
    start_dt = row.get("start_time") or row.get("created_at")
    start_iso = start_dt.isoformat() if hasattr(start_dt, "isoformat") else None
    name = (
        row.get("manual_client_name")
        or row.get("scheduled_name")
        or row.get("profile_name")
        or str(row.get("activity_id"))
    )
    return {
        "id": row.get("activity_id"),
        "mapNameRu": name,
        "name": name,
        "startTime": start_iso,
        "distance": row.get("distance"),
        "elapsedTime": row.get("elapsed_time"),
        "totalElevationGain": row.get("elevation_gain"),
        "averageWatts": row.get("average_power"),
        "averageCadence": row.get("average_cadence"),
        "averageHeartrate": row.get("average_heartrate"),
    }


@router.post("/{account_id}/{activity_id}/strava")
def api_upload_activity_to_strava(
    account_id: str,
    activity_id: str,
):
    """Upload a single activity to Strava for the linked user."""
    try:
        activity_row = _load_activity_row(account_id, activity_id)

        client_id = activity_row.get("manual_client_id") or activity_row.get("client_id")
        if not client_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Strava не привязана: клиент не найден")

        link = client_link_repository.get_link_by_client(int(client_id))
        tg_user_id = link.get("tg_user_id") if link else None
        if not tg_user_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Strava не привязана у клиента")

        straver = StraverClient()
        if not straver.is_configured():
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Straver сервис не настроен")

        statuses = straver.connection_status([int(tg_user_id)])
        status_row = statuses.get(int(tg_user_id))
        if not status_row or not status_row.get("connected"):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Strava не привязана у пользователя")

        file_path = _resolve_fit_file_path(activity_row)
        if not file_path:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "FIT-файл не найден, загрузка невозможна")

        upload_name, description = _build_strava_payload(activity_row)
        try:
            straver.upload_activity(
                tg_user_id=int(tg_user_id),
                file_path=file_path,
                name=upload_name,
                description=description,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed to upload activity %s/%s to Strava", account_id, activity_id)
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, f"Не удалось загрузить в Strava: {exc}") from exc

        schedule_repository.record_seen_activity_id(
            str(account_id),
            str(activity_id),
            sent_strava=True,
        )

        return {"status": "uploaded", "message": "Активность отправлена в Strava"}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("Unexpected error while uploading activity %s/%s to Strava", account_id, activity_id)
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось отправить активность в Strava"
        ) from exc


@router.post("/{account_id}/{activity_id}/clientbot")
def api_send_activity_to_clientbot(
    account_id: str,
    activity_id: str,
):
    """Send a single activity to the linked Telegram client bot."""
    try:
        activity_row = _load_activity_row(account_id, activity_id)
        client_id = activity_row.get("manual_client_id") or activity_row.get("client_id")
        if not client_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Бот не привязан: клиент не найден")

        link = client_link_repository.get_link_by_client(int(client_id))
        tg_user_id = link.get("tg_user_id") if link else None
        if not tg_user_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Бот не привязан у клиента")

        token = os.environ.get("KRUTILKAVN_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
        if not token:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "KRUTILKAVN_BOT_TOKEN не настроен")

        file_path = _resolve_fit_file_path(activity_row)
        if not file_path:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "FIT-файл не найден, отправка в бота невозможна")
        activity_payload = _build_activity_payload(activity_row)
        caption = format_activity_meta(
            activity_payload,
            account_id,
            profile=None,
            scheduled_name=activity_row.get("scheduled_name"),
        )
        telegram_send_document(
            token=token,
            chat_id=str(tg_user_id),
            file_path=file_path,
            filename=file_path.name,
            caption=caption,
            timeout=30,
        )

        schedule_repository.record_seen_activity_id(
            str(account_id),
            str(activity_id),
            sent_clientbot=True,
        )
        return {"status": "sent", "message": "Отправлено в бота"}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("Unexpected error while sending activity %s/%s to bot", account_id, activity_id)
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось отправить в бота"
        ) from exc


@router.post("/{account_id}/{activity_id}/intervals")
def api_send_activity_to_intervals(
    account_id: str,
    activity_id: str,
):
    """Upload a single activity to Intervals.icu for the linked user."""
    try:
        activity_row = _load_activity_row(account_id, activity_id)
        client_id = activity_row.get("manual_client_id") or activity_row.get("client_id")
        if not client_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Intervals не привязан: клиент не найден")

        link = client_link_repository.get_link_by_client(int(client_id))
        tg_user_id = link.get("tg_user_id") if link else None
        if not tg_user_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Intervals не привязан у клиента")

        intervals_link = intervals_link_repository.get_link(int(tg_user_id))
        if not intervals_link or not intervals_link.get("intervals_api_key"):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Intervals не привязан у пользователя")

        file_path = _resolve_fit_file_path(activity_row)
        if not file_path:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "FIT-файл не найден, загрузка невозможна")

        activity_payload = _build_activity_payload(activity_row)
        description = format_strava_activity_description(
            activity_payload,
            account_id,
            profile=None,
            scheduled_name=activity_row.get("scheduled_name"),
        )

        uploaded = intervals_sync.upload_activity(
            tg_user_id=int(tg_user_id),
            temp_file=file_path,
            description=description,
            activity_id=activity_id,
            timeout=30.0,
            activity_name=activity_payload.get("name") or "Крутилка",
        )
        if not uploaded:
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Не удалось загрузить в Intervals")

        schedule_repository.record_seen_activity_id(
            str(account_id),
            str(activity_id),
            sent_intervals=True,
        )
        return {"status": "sent", "message": "Отправлено в Intervals"}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception(
            "Unexpected error while sending activity %s/%s to Intervals", account_id, activity_id
        )
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось отправить в Intervals"
        ) from exc


@router.patch("/{account_id}/{activity_id}/client")
async def api_update_activity_client(account_id: str, activity_id: str, payload: dict):
    """Update linked client for an activity."""
    try:
        activity_row = _load_activity_row(account_id, activity_id)
        client_id_raw = payload.get("client_id") if isinstance(payload, dict) else None
        new_client_id: Optional[int] = None
        new_client_name: Optional[str] = None
        if client_id_raw not in (None, ""):
            try:
                new_client_id = int(client_id_raw)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Некорректный client_id") from exc
            if new_client_id <= 0:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "client_id должен быть > 0")
            client_row = client_repository.get_client(new_client_id)
            if not client_row:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Клиент не найден")
            new_client_name = client_row.get("full_name") or (
                f"{client_row.get('first_name') or ''} {client_row.get('last_name') or ''}".strip()
            )

        schedule_repository.ensure_activity_ids_table()
        with schedule_repository.db_connection() as conn, schedule_repository.dict_cursor(conn) as cur:
            cur.execute(
                """
                UPDATE seen_activity_ids
                SET client_id = %s,
                    manual_client_id = %s,
                    manual_client_name = %s
                WHERE account_id = %s AND activity_id = %s
                RETURNING *
                """,
                (new_client_id, new_client_id, new_client_name, account_id, activity_id),
            )
            updated = cur.fetchone()
            conn.commit()

        if not updated:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Activity not found")

        return {"item": _serialize_activity_id_enriched(updated)}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to update client for activity %s/%s", account_id, activity_id)
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось обновить клиента"
        ) from exc


@router.delete("/{account_id}/{activity_id}")
def api_delete_activity_id(
    account_id: str,
    activity_id: str,
):
    """Delete a specific activity ID for an account."""
    try:
        success = schedule_repository.delete_activity_id(account_id, activity_id)
        if success:
            return {"status": "ok", "message": "Activity ID deleted successfully"}
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Activity ID not found")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to delete activity ID"
        ) from exc


@router.get("/accounts")
def api_list_accounts():
    """Get list of all accounts that have activity IDs."""
    try:
        schedule_repository.ensure_activity_ids_table()
        accounts = schedule_repository.list_all_accounts()
        return {"accounts": accounts}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch accounts"
        ) from exc
