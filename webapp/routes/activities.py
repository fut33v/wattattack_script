from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status

from repositories import schedule_repository
from ..dependencies import require_admin

router = APIRouter(prefix="/activities", tags=["activities"], dependencies=[Depends(require_admin)])


def _serialize_activity_id(activity_record: dict) -> dict:
    serialized = dict(activity_record)
    created_at = serialized.get("created_at")
    if hasattr(created_at, "isoformat"):
        serialized["created_at"] = created_at.isoformat()
    return serialized


def _serialize_activity_id_enriched(activity_record: dict) -> dict:
    serialized = _serialize_activity_id(activity_record)
    client_id = activity_record.get("client_id")
    scheduled_name = activity_record.get("scheduled_name")
    if client_id is not None:
        serialized["client_id"] = client_id
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
        accounts = schedule_repository.list_all_accounts()
        return {"accounts": accounts}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to fetch accounts"
        ) from exc
