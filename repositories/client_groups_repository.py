"""Helpers for storing client group memberships."""
from __future__ import annotations

from typing import List, Dict, Optional

from .db_utils import db_connection, dict_cursor

DEFAULT_GROUPS = ("САМОКРУТЧИКИ",)

def _normalize_group_name(value: str) -> str:
    return value.strip()


def ensure_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_group_defs (
                group_name TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_groups (
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                group_name TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (client_id, group_name)
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS client_groups_lower_idx ON client_groups (client_id, LOWER(group_name))"
        )
        conn.commit()


def list_groups(client_id: int) -> List[str]:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT group_name
            FROM client_groups
            WHERE client_id = %s
            ORDER BY LOWER(group_name), group_name
            """,
            (client_id,),
        )
        rows = cur.fetchall()
    return [str(row["group_name"]) for row in rows if row.get("group_name") not in (None, "")]


def list_all_groups() -> List[Dict[str, str]]:
    ensure_table()
    for group in DEFAULT_GROUPS:
        upsert_group(group)
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT group_name, created_at
            FROM client_group_defs
            ORDER BY LOWER(group_name), group_name
            """
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def upsert_group(group_name: str) -> Optional[str]:
    normalized = _normalize_group_name(group_name)
    if not normalized:
        return None
    ensure_table()
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO client_group_defs (group_name)
            VALUES (%s)
            ON CONFLICT (group_name) DO NOTHING
            """,
            (normalized,),
        )
        conn.commit()
    return normalized


def delete_group(group_name: str) -> bool:
    normalized = _normalize_group_name(group_name)
    if not normalized:
        return False
    ensure_table()
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM client_group_defs WHERE group_name = %s", (normalized,))
        removed = cur.rowcount > 0
        conn.commit()
    return removed


def add_client_to_group(client_id: int, group_name: str) -> bool:
    normalized = _normalize_group_name(group_name)
    if not normalized:
        return False

    ensure_table()
    upsert_group(normalized)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO client_groups (client_id, group_name)
            VALUES (%s, %s)
            ON CONFLICT (client_id, group_name) DO NOTHING
            """,
            (client_id, normalized),
        )
        added = cur.rowcount > 0
        conn.commit()
    return added


def remove_client_from_group(client_id: int, group_name: str) -> bool:
    normalized = _normalize_group_name(group_name)
    if not normalized:
        return False

    ensure_table()
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM client_groups
            WHERE client_id = %s AND LOWER(group_name) = LOWER(%s)
            """,
            (client_id, normalized),
        )
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def is_client_in_group(client_id: int, group_name: str) -> bool:
    normalized = _normalize_group_name(group_name)
    if not normalized:
        return False

    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT 1
            FROM client_groups
            WHERE client_id = %s AND LOWER(group_name) = LOWER(%s)
            """,
            (client_id, normalized),
        )
        row = cur.fetchone()
    return bool(row)
