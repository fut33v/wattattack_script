"""Manage bot administrators stored in the database."""
from __future__ import annotations

import os
from typing import Dict, List, Optional, Tuple

from .db_utils import db_connection, dict_cursor


def _sanitize_username(username: Optional[str]) -> Optional[str]:
    if not username:
        return None
    username = username.strip()
    if username.startswith("@"):
        username = username[1:]
    return username or None


def _normalize_username(username: Optional[str]) -> Optional[str]:
    username = _sanitize_username(username)
    return username.lower() if username else None


def ensure_admin_table() -> None:
    """Create the admins table if missing and ensure required columns/indexes."""

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT,
                username TEXT,
                username_lower TEXT,
                display_name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "ALTER TABLE admins ADD COLUMN IF NOT EXISTS username_lower TEXT"
        )
        cur.execute(
            "ALTER TABLE admins ADD COLUMN IF NOT EXISTS display_name TEXT"
        )
        cur.execute(
            "ALTER TABLE admins ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()"
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS admins_tg_id_idx ON admins (tg_id) WHERE tg_id IS NOT NULL"
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS admins_username_lower_idx ON admins (username_lower) WHERE username_lower IS NOT NULL"
        )
        # Backfill username_lower
        cur.execute(
            "UPDATE admins SET username_lower = LOWER(username) WHERE username IS NOT NULL AND (username_lower IS NULL OR username_lower <> LOWER(username))"
        )
        conn.commit()


def seed_admins_from_env(env_value: Optional[str] = None) -> None:
    """Seed admins from TELEGRAM_ADMIN_IDS if table is empty or missing entries."""

    ensure_admin_table()
    env_value = env_value if env_value is not None else os.environ.get("TELEGRAM_ADMIN_IDS", "")
    if not env_value:
        return
    tokens = [token.strip() for token in env_value.split(",") if token.strip()]
    if not tokens:
        return
    for token in tokens:
        tg_id: Optional[int] = None
        username: Optional[str] = None
        if token.isdigit():
            tg_id = int(token)
        else:
            username = token
        try:
            add_admin(tg_id=tg_id, username=username, display_name=token)
        except Exception:
            # ignore seeding errors
            pass


def list_admins() -> List[Dict]:
    ensure_admin_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT id, tg_id, username, display_name, created_at FROM admins ORDER BY COALESCE(username_lower, ''), COALESCE(display_name, ''), tg_id"
        )
        rows = cur.fetchall()
    return rows


def get_admin_ids() -> List[int]:
    ensure_admin_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT tg_id FROM admins WHERE tg_id IS NOT NULL")
        rows = cur.fetchall()
    return [int(row["tg_id"]) for row in rows if row.get("tg_id") is not None]


def is_admin(tg_id: Optional[int], username: Optional[str]) -> bool:
    ensure_admin_table()
    username_norm = _normalize_username(username)
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT 1 FROM admins WHERE (tg_id = %s AND %s IS NOT NULL) OR (username_lower = %s AND %s IS NOT NULL) LIMIT 1",
            (tg_id, tg_id, username_norm, username_norm),
        )
        row = cur.fetchone()
    return row is not None


def add_admin(
    *, tg_id: Optional[int], username: Optional[str], display_name: Optional[str] = None
) -> Tuple[bool, Dict]:
    """Insert or update an admin.

    Returns tuple (created, record).
    """

    ensure_admin_table()
    username_clean = _sanitize_username(username)
    username_lower = _normalize_username(username)
    if tg_id is None and username_clean is None:
        raise ValueError("tg_id or username must be provided")

    with db_connection() as conn, dict_cursor(conn) as cur:
        if tg_id is not None:
            cur.execute(
                "SELECT * FROM admins WHERE tg_id = %s OR (username_lower = %s AND %s IS NOT NULL) LIMIT 1",
                (tg_id, username_lower, username_lower),
            )
        else:
            cur.execute(
                "SELECT * FROM admins WHERE username_lower = %s LIMIT 1",
                (username_lower,),
            )
        existing = cur.fetchone()

        display_value = display_name or username_clean or (str(tg_id) if tg_id is not None else None)

        if existing:
            cur.execute(
                "UPDATE admins SET tg_id = COALESCE(%s, tg_id), username = COALESCE(%s, username), username_lower = COALESCE(%s, username_lower), display_name = COALESCE(%s, display_name) WHERE id = %s RETURNING id, tg_id, username, display_name, created_at",
                (
                    tg_id,
                    username_clean,
                    username_lower,
                    display_value,
                    existing["id"],
                ),
            )
            record = cur.fetchone()
            conn.commit()
            return False, record
        else:
            cur.execute(
                "INSERT INTO admins (tg_id, username, username_lower, display_name) VALUES (%s, %s, %s, %s) RETURNING id, tg_id, username, display_name, created_at",
                (
                    tg_id,
                    username_clean,
                    username_lower,
                    display_value,
                ),
            )
            record = cur.fetchone()
            conn.commit()
            return True, record


def remove_admin(*, tg_id: Optional[int], username: Optional[str]) -> bool:
    ensure_admin_table()
    username_lower = _normalize_username(username)
    with db_connection() as conn, dict_cursor(conn) as cur:
        if tg_id is not None:
            cur.execute("DELETE FROM admins WHERE tg_id = %s", (tg_id,))
        elif username_lower:
            cur.execute("DELETE FROM admins WHERE username_lower = %s", (username_lower,))
        else:
            return False
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted
