"""Store pending client link approvals for adminbot callbacks."""
from __future__ import annotations

from typing import Dict, Optional

from .db_utils import db_connection, dict_cursor


def ensure_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS link_requests (
                request_id TEXT PRIMARY KEY,
                client_id INTEGER NOT NULL,
                tg_user_id BIGINT NOT NULL,
                tg_username TEXT,
                tg_full_name TEXT,
                user_chat_id BIGINT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "ALTER TABLE link_requests ADD COLUMN IF NOT EXISTS user_chat_id BIGINT"
        )
        conn.commit()


def create_link_request(
    *,
    request_id: str,
    client_id: int,
    tg_user_id: int,
    tg_username: str | None,
    tg_full_name: str | None,
    user_chat_id: int | None,
) -> Dict:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO link_requests (
                request_id, client_id, tg_user_id, tg_username, tg_full_name, user_chat_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (request_id) DO UPDATE
            SET client_id = EXCLUDED.client_id,
                tg_user_id = EXCLUDED.tg_user_id,
                tg_username = EXCLUDED.tg_username,
                tg_full_name = EXCLUDED.tg_full_name,
                user_chat_id = EXCLUDED.user_chat_id,
                created_at = COALESCE(link_requests.created_at, NOW())
            RETURNING request_id, client_id, tg_user_id, tg_username, tg_full_name, user_chat_id, created_at
            """,
            (request_id, client_id, tg_user_id, tg_username, tg_full_name, user_chat_id),
        )
        row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def get_link_request(request_id: str) -> Optional[Dict]:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT request_id, client_id, tg_user_id, tg_username, tg_full_name, user_chat_id, created_at
            FROM link_requests
            WHERE request_id = %s
            """,
            (request_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def delete_link_request(request_id: str) -> bool:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("DELETE FROM link_requests WHERE request_id = %s", (request_id,))
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted
