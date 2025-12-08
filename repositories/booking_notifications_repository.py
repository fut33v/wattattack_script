"""Store booking-related notifications for later viewing in the admin UI."""
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import date, time
import json

from .db_utils import db_connection, dict_cursor


def _ensure_table() -> None:
    """Create table if missing; safe to call often."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS booking_notifications (
                id SERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                client_id INTEGER,
                client_name TEXT,
                slot_date DATE,
                start_time TIME,
                slot_label TEXT,
                stand_label TEXT,
                bike_label TEXT,
                source TEXT,
                message_text TEXT,
                payload JSONB,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS booking_notifications_created_at_idx ON booking_notifications (created_at DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS booking_notifications_event_type_idx ON booking_notifications (event_type)"
        )
        conn.commit()


def insert_notification(
    *,
    event_type: str,
    client_id: Optional[int],
    client_name: Optional[str],
    slot_date: Optional[date],
    start_time: Optional[time],
    slot_label: Optional[str],
    stand_label: Optional[str],
    bike_label: Optional[str],
    source: Optional[str],
    message_text: Optional[str],
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Persist a notification row and return it."""
    _ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO booking_notifications (
                event_type,
                client_id,
                client_name,
                slot_date,
                start_time,
                slot_label,
                stand_label,
                bike_label,
                source,
                message_text,
                payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, event_type, client_id, client_name, slot_date, start_time, slot_label,
                      stand_label, bike_label, source, message_text, payload, created_at
            """,
            (
                event_type,
                client_id,
                client_name,
                slot_date,
                start_time,
                slot_label,
                stand_label,
                bike_label,
                source,
                message_text,
                json.dumps(payload) if payload is not None else None,
            ),
        )
        row = cur.fetchone()
        conn.commit()
    return row


def list_notifications(*, limit: int = 50, offset: int = 0) -> list[Dict[str, Any]]:
    """Return booking/client notifications ordered by newest first."""
    _ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, event_type, client_id, client_name, slot_date, start_time, slot_label,
                   stand_label, bike_label, source, message_text, payload, created_at
            FROM booking_notifications
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
    return rows


def count_notifications() -> int:
    _ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS count FROM booking_notifications")
        row = cur.fetchone()
    return int(row["count"]) if row and row.get("count") is not None else 0
