"""Helpers for managing client subscriptions and session balances."""
from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

from .db_utils import db_connection, dict_cursor
from . import schedule_repository


def ensure_subscription_tables() -> None:
    """Create subscription tables if they do not yet exist."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_subscriptions (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id),
                plan_code TEXT NOT NULL,
                plan_name TEXT NOT NULL,
                sessions_total INTEGER,
                sessions_remaining INTEGER,
                price_rub INTEGER,
                valid_from DATE,
                valid_until DATE,
                notes TEXT,
                created_by BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_subscription_adjustments (
                id SERIAL PRIMARY KEY,
                subscription_id INTEGER NOT NULL REFERENCES client_subscriptions(id) ON DELETE CASCADE,
                delta_sessions INTEGER NOT NULL,
                reason TEXT NOT NULL,
                reservation_id INTEGER REFERENCES schedule_reservations(id) ON DELETE SET NULL,
                reservation_label TEXT,
                created_by BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS client_subscriptions_client_id_idx ON client_subscriptions (client_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS client_subscription_adjustments_subscription_id_idx ON client_subscription_adjustments (subscription_id)"
        )
        cur.execute(
            """
            ALTER TABLE client_subscription_adjustments
            ADD COLUMN IF NOT EXISTS reservation_id INTEGER REFERENCES schedule_reservations(id) ON DELETE SET NULL
            """
        )
        cur.execute(
            """
            ALTER TABLE client_subscription_adjustments
            ADD COLUMN IF NOT EXISTS reservation_label TEXT
            """
        )
        conn.commit()


def _clean_int(value: object, *, allow_none: bool = True) -> Optional[int]:
    if value is None:
        return None if allow_none else 0
    if isinstance(value, bool):
        raise ValueError("Boolean is not a valid integer value")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None if allow_none else 0
        return int(stripped)
    raise ValueError("Invalid integer value")


def create_subscription(
    *,
    client_id: int,
    plan_code: str,
    plan_name: str,
    sessions_total: Optional[int],
    price_rub: Optional[int],
    valid_from: Optional[date],
    valid_until: Optional[date],
    notes: Optional[str],
    created_by: Optional[int],
    sessions_remaining: Optional[int] = None,
) -> Dict:
    """Create a subscription and seed the remaining sessions."""
    ensure_subscription_tables()

    total_sessions = _clean_int(sessions_total, allow_none=True)
    if total_sessions is not None and total_sessions < 0:
        raise ValueError("sessions_total must be non-negative")

    remaining = sessions_remaining
    if remaining is None:
        remaining = total_sessions
    remaining_clean = _clean_int(remaining, allow_none=True)
    if remaining_clean is not None and remaining_clean < 0:
        raise ValueError("sessions_remaining must be non-negative")

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO client_subscriptions (
                client_id,
                plan_code,
                plan_name,
                sessions_total,
                sessions_remaining,
                price_rub,
                valid_from,
                valid_until,
                notes,
                created_by
            )
            VALUES (%(client_id)s, %(plan_code)s, %(plan_name)s, %(sessions_total)s, %(sessions_remaining)s,
                    %(price_rub)s, %(valid_from)s, %(valid_until)s, %(notes)s, %(created_by)s)
            RETURNING *
            """,
            {
                "client_id": client_id,
                "plan_code": plan_code,
                "plan_name": plan_name,
                "sessions_total": total_sessions,
                "sessions_remaining": remaining_clean,
                "price_rub": _clean_int(price_rub, allow_none=True),
                "valid_from": valid_from,
                "valid_until": valid_until,
                "notes": (notes or "").strip() or None,
                "created_by": created_by,
            },
        )
        subscription = cur.fetchone()

        if remaining_clean is not None:
            cur.execute(
                """
                INSERT INTO client_subscription_adjustments (subscription_id, delta_sessions, reason, created_by)
                VALUES (%s, %s, %s, %s)
                """,
                (subscription["id"], remaining_clean, "purchase", created_by),
            )
        conn.commit()

    return subscription


def list_client_subscriptions(client_id: int) -> List[Dict]:
    """Return subscriptions for a client ordered by creation time."""
    ensure_subscription_tables()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM client_subscriptions
            WHERE client_id = %s
            ORDER BY created_at DESC, id DESC
            """,
            (client_id,),
        )
        subscriptions = cur.fetchall()

        ids = [row["id"] for row in subscriptions]
        adjustments: Dict[int, List[Dict]] = {sub_id: [] for sub_id in ids}
        if ids:
            cur.execute(
                """
                SELECT *
                FROM client_subscription_adjustments
                WHERE subscription_id = ANY(%s)
                ORDER BY created_at DESC, id DESC
                """,
                (ids,),
            )
            for row in cur.fetchall():
                adjustments.setdefault(row["subscription_id"], []).append(row)

    for item in subscriptions:
        item["adjustments"] = adjustments.get(item["id"], [])
    return subscriptions


def get_subscription(subscription_id: int) -> Optional[Dict]:
    ensure_subscription_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM client_subscriptions WHERE id = %s", (subscription_id,))
        return cur.fetchone()


def delete_subscription(subscription_id: int) -> bool:
    """Delete a subscription and its adjustments."""
    ensure_subscription_tables()
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM client_subscriptions WHERE id = %s", (subscription_id,))
            deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def add_adjustment(
    *,
    subscription_id: int,
    delta_sessions: int,
    reason: str,
    created_by: Optional[int],
    reservation_id: Optional[int] = None,
) -> Tuple[Dict, Dict]:
    """Adjust remaining sessions and return (subscription, adjustment)."""
    ensure_subscription_tables()

    if not isinstance(delta_sessions, (int, float)):
        raise ValueError("delta_sessions must be a number")

    delta = int(delta_sessions)
    if delta == 0:
        raise ValueError("delta_sessions must be non-zero")

    reason_clean = (reason or "").strip() or "adjustment"

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, sessions_remaining, client_id, valid_from, valid_until
            FROM client_subscriptions
            WHERE id = %s
            """,
            (subscription_id,),
        )
        current = cur.fetchone()
        if not current:
            raise ValueError("Subscription not found")
        remaining = current["sessions_remaining"]
        if remaining is None:
            raise ValueError("Невозможно изменить безлимитный абонемент")

        new_remaining = remaining + delta
        if new_remaining < 0:
            raise ValueError("Недостаточно занятий в абонементе")

        reservation_label: Optional[str] = None
        if reservation_id:
            reservation = schedule_repository.get_reservation(reservation_id)
            if not reservation:
                raise ValueError("Запись не найдена")
            if reservation.get("client_id") not in (None, current.get("client_id")):
                raise ValueError("Запись относится к другому клиенту")
            slot = schedule_repository.get_slot(reservation["slot_id"])
            if slot:
                slot_date = slot.get("slot_date")
                valid_from = current.get("valid_from")
                valid_until = current.get("valid_until")
                if valid_from and slot_date and slot_date < valid_from:
                    raise ValueError("Слот вне периода абонемента")
                if valid_until and slot_date and slot_date > valid_until:
                    raise ValueError("Слот вне периода абонемента")
                start_time = slot.get("start_time")
                date_str = slot_date.isoformat() if hasattr(slot_date, "isoformat") else str(slot_date)
                time_str = start_time.strftime("%H:%M") if hasattr(start_time, "strftime") else (start_time or "")
                title = slot.get("label") or slot.get("session_kind") or "Тренировка"
                stand = reservation.get("stand_code") or reservation.get("stand_id")
                parts = [date_str, time_str, title, stand]
                reservation_label = " · ".join([p for p in parts if p])

        cur.execute(
            """
            UPDATE client_subscriptions
            SET sessions_remaining = %s
            WHERE id = %s
            RETURNING *
            """,
            (new_remaining, subscription_id),
        )
        updated = cur.fetchone()
        cur.execute(
            """
            INSERT INTO client_subscription_adjustments (
                subscription_id,
                delta_sessions,
                reason,
                reservation_id,
                reservation_label,
                created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (subscription_id, delta, reason_clean, reservation_id, reservation_label, created_by),
        )
        adjustment = cur.fetchone()
        conn.commit()

    return updated, adjustment


def list_adjustments(subscription_ids: Sequence[int]) -> Dict[int, List[Dict]]:
    """Return adjustments grouped by subscription id."""
    ensure_subscription_tables()
    if not subscription_ids:
        return {}
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM client_subscription_adjustments
            WHERE subscription_id = ANY(%s)
            ORDER BY created_at DESC, id DESC
            """,
            (list(subscription_ids),),
        )
        adjustments: Dict[int, List[Dict]] = {}
        for row in cur.fetchall():
            adjustments.setdefault(row["subscription_id"], []).append(row)
    return adjustments
