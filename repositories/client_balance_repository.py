"""Helpers for managing client monetary balances."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from .db_utils import db_connection, dict_cursor


def ensure_balance_tables() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_balances (
                client_id INTEGER PRIMARY KEY REFERENCES clients(id) ON DELETE CASCADE,
                balance_rub INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_balance_adjustments (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                delta_rub INTEGER NOT NULL,
                reason TEXT,
                reservation_id INTEGER REFERENCES schedule_reservations(id) ON DELETE SET NULL,
                created_by BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS client_balance_adjustments_client_idx ON client_balance_adjustments(client_id)"
        )
        conn.commit()


def get_balance(client_id: int) -> Dict:
    ensure_balance_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT client_id, balance_rub, updated_at FROM client_balances WHERE client_id = %s", (client_id,))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "INSERT INTO client_balances (client_id, balance_rub) VALUES (%s, 0) RETURNING client_id, balance_rub, updated_at",
                (client_id,),
            )
            row = cur.fetchone()
            conn.commit()
    return row or {"client_id": client_id, "balance_rub": 0}


def list_adjustments(client_id: int) -> List[Dict]:
    ensure_balance_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, client_id, delta_rub, reason, reservation_id, created_by, created_at
            FROM client_balance_adjustments
            WHERE client_id = %s
            ORDER BY created_at DESC, id DESC
            LIMIT 200
            """,
            (client_id,),
        )
        return cur.fetchall()


def add_adjustment(
    *,
    client_id: int,
    delta_rub: int,
    reason: Optional[str],
    created_by: Optional[int],
    reservation_id: Optional[int] = None,
    created_at: Optional[str] = None,
) -> Tuple[Dict, Dict]:
    ensure_balance_tables()
    if delta_rub == 0:
        raise ValueError("delta_rub must be non-zero")
    reason_clean = (reason or "").strip() or ("spend" if delta_rub < 0 else "top-up")

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT balance_rub FROM client_balances WHERE client_id = %s", (client_id,))
        row = cur.fetchone()
        if row is None:
            current_balance = 0
            cur.execute(
                "INSERT INTO client_balances (client_id, balance_rub) VALUES (%s, %s)",
                (client_id, current_balance),
            )
        else:
            current_balance = int(row["balance_rub"] or 0)

        new_balance = current_balance + int(delta_rub)
        cur.execute(
            "UPDATE client_balances SET balance_rub = %s, updated_at = NOW() WHERE client_id = %s RETURNING client_id, balance_rub, updated_at",
            (new_balance, client_id),
        )
        balance = cur.fetchone()
        cur.execute(
            """
            INSERT INTO client_balance_adjustments (client_id, delta_rub, reason, reservation_id, created_by, created_at)
            VALUES (%s, %s, %s, %s, %s, COALESCE(%s, NOW()))
            RETURNING *
            """,
            (client_id, delta_rub, reason_clean, reservation_id, created_by, created_at),
        )
        adjustment = cur.fetchone()
        conn.commit()

    return balance, adjustment


def delete_adjustment(client_id: int, adjustment_id: int) -> Dict:
    """Delete an adjustment and roll back the balance change."""
    ensure_balance_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            DELETE FROM client_balance_adjustments
            WHERE id = %s AND client_id = %s
            RETURNING delta_rub
            """,
            (adjustment_id, client_id),
        )
        deleted = cur.fetchone()
        if not deleted:
            conn.rollback()
            raise ValueError("Операция не найдена")
        delta_rub = int(deleted["delta_rub"])
        cur.execute(
            """
            UPDATE client_balances
            SET balance_rub = balance_rub - %s, updated_at = NOW()
            WHERE client_id = %s
            RETURNING *
            """,
            (delta_rub, client_id),
        )
        balance = cur.fetchone()
        conn.commit()
    return balance or {"client_id": client_id, "balance_rub": 0}
