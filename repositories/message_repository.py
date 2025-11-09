"""Manage user messages sent to the krutilkavnbot."""
from __future__ import annotations

from typing import Dict, List, Optional
from datetime import datetime

from .db_utils import db_connection, dict_cursor


def ensure_user_messages_table() -> None:
    """Create the user_messages table if it does not exist."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_messages (
                id SERIAL PRIMARY KEY,
                tg_user_id BIGINT NOT NULL,
                tg_username TEXT,
                tg_full_name TEXT,
                message_text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS user_messages_tg_user_idx ON user_messages (tg_user_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS user_messages_created_at_idx ON user_messages (created_at DESC)"
        )
        conn.commit()


def store_user_message(
    *,
    tg_user_id: int,
    message_text: str,
    tg_username: Optional[str] = None,
    tg_full_name: Optional[str] = None,
) -> Dict:
    """Store a user message in the database."""
    ensure_user_messages_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO user_messages (tg_user_id, tg_username, tg_full_name, message_text, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING id, tg_user_id, tg_username, tg_full_name, message_text, created_at
            """,
            (tg_user_id, tg_username, tg_full_name, message_text),
        )
        record = cur.fetchone()
        conn.commit()
    return record


def list_user_messages(limit: int = 100, offset: int = 0) -> List[Dict]:
    """Return user messages, optionally paginated."""
    ensure_user_messages_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, tg_user_id, tg_username, tg_full_name, message_text, created_at
            FROM user_messages
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
    return rows


def get_user_message_count() -> int:
    """Return the total count of user messages."""
    ensure_user_messages_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) as count FROM user_messages")
        result = cur.fetchone()
    return result["count"] if result else 0