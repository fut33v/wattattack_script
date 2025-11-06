"""Database helpers for managing schedule instructors."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from .db_utils import db_connection, dict_cursor


DEFAULT_INSTRUCTORS = (
    "Евгений Балакин",
    "Илья Фатеев",
    "Кирилл Иванов",
)


def ensure_instructors_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_instructors (
                id SERIAL PRIMARY KEY,
                full_name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS schedule_instructors_name_idx
            ON schedule_instructors (lower(full_name))
            """
        )
        for name in DEFAULT_INSTRUCTORS:
            clean = name.strip()
            if not clean:
                continue
            cur.execute(
                """
                INSERT INTO schedule_instructors (full_name)
                VALUES (%s)
                ON CONFLICT DO NOTHING
                """,
                (clean,),
            )
        conn.commit()


def list_instructors() -> List[Dict]:
    ensure_instructors_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, full_name, created_at
            FROM schedule_instructors
            ORDER BY lower(full_name)
            """
        )
        rows = cur.fetchall()
    return rows


def get_instructor(instructor_id: int) -> Optional[Dict]:
    ensure_instructors_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT id, full_name FROM schedule_instructors WHERE id = %s",
            (instructor_id,),
        )
        row = cur.fetchone()
    return row


def create_instructor(full_name: str) -> Dict:
    ensure_instructors_table()
    full_name_clean = (full_name or "").strip()
    if not full_name_clean:
        raise ValueError("full_name is required")
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO schedule_instructors (full_name)
            VALUES (%s)
            ON CONFLICT DO NOTHING
            RETURNING id, full_name, created_at
            """,
            (full_name_clean,),
        )
        row = cur.fetchone()
        if row:
            conn.commit()
            return row
        cur.execute(
            """
            SELECT id, full_name, created_at
            FROM schedule_instructors
            WHERE lower(full_name) = lower(%s)
            """,
            (full_name_clean,),
        )
        row = cur.fetchone()
        conn.commit()
    if not row:
        raise RuntimeError("Failed to fetch instructor after insert")
    return row


def delete_instructor(instructor_id: int) -> bool:
    ensure_instructors_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "DELETE FROM schedule_instructors WHERE id = %s",
            (instructor_id,),
        )
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted
