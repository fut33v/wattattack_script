"""Database utilities for managing pedal types."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db_utils import db_connection, dict_cursor

PEDAL_TYPES = ("platform", "road_look", "road_shimano", "mtb_shimano")


def ensure_pedals_table() -> None:
    """Create the pedals table if missing and ensure indexes exist."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pedals (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                pedal_type TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS pedals_name_idx ON pedals (LOWER(name))"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS pedals_type_idx ON pedals (pedal_type)"
        )
        conn.commit()


def list_pedals(limit: Optional[int] = None, offset: int = 0) -> List[Dict]:
    ensure_pedals_table()
    query = "SELECT id, name, pedal_type, created_at FROM pedals ORDER BY name"
    params: tuple = ()
    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params = (limit, offset)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        return cur.fetchall()


def create_pedal(*, name: str, pedal_type: str) -> Dict:
    ensure_pedals_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        try:
            cur.execute(
                """
                INSERT INTO pedals (name, pedal_type)
                VALUES (%s, %s)
                RETURNING id, name, pedal_type, created_at
                """,
                (name, pedal_type),
            )
            row = cur.fetchone()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return row


def delete_pedal(pedal_id: int) -> bool:
    ensure_pedals_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("DELETE FROM pedals WHERE id = %s", (pedal_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
