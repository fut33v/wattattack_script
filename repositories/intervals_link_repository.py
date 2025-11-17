"""Store Intervals.icu API keys linked to Telegram users."""
from __future__ import annotations

from typing import Dict, Optional

from .db_utils import db_connection, dict_cursor


def ensure_intervals_links_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS intervals_links (
                tg_user_id BIGINT PRIMARY KEY,
                intervals_api_key TEXT NOT NULL,
                intervals_athlete_id TEXT DEFAULT '0',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "ALTER TABLE intervals_links ADD COLUMN IF NOT EXISTS intervals_athlete_id TEXT DEFAULT '0'"
        )
        cur.execute(
            "ALTER TABLE intervals_links ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()"
        )
        cur.execute(
            "ALTER TABLE intervals_links ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()"
        )
        cur.execute(
            "ALTER TABLE intervals_links ALTER COLUMN updated_at SET DEFAULT NOW()"
        )
        conn.commit()


def upsert_link(*, tg_user_id: int, api_key: str, athlete_id: str | int | None = None) -> Dict:
    ensure_intervals_links_table()
    athlete_id_str = str(athlete_id or "0")
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO intervals_links (tg_user_id, intervals_api_key, intervals_athlete_id, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (tg_user_id) DO UPDATE SET
                intervals_api_key = EXCLUDED.intervals_api_key,
                intervals_athlete_id = EXCLUDED.intervals_athlete_id,
                updated_at = NOW()
            RETURNING tg_user_id, intervals_api_key, intervals_athlete_id, created_at, updated_at
            """,
            (tg_user_id, api_key, athlete_id_str),
        )
        row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def get_link(tg_user_id: int) -> Optional[Dict]:
    ensure_intervals_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT tg_user_id, intervals_api_key, intervals_athlete_id, created_at, updated_at
            FROM intervals_links
            WHERE tg_user_id = %s
            """,
            (tg_user_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def remove_link(tg_user_id: int) -> bool:
    ensure_intervals_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("DELETE FROM intervals_links WHERE tg_user_id = %s", (tg_user_id,))
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def list_links() -> list[Dict]:
    """Return all Intervals.icu links."""
    ensure_intervals_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT tg_user_id, intervals_api_key, intervals_athlete_id, created_at, updated_at
            FROM intervals_links
            ORDER BY updated_at DESC
            """
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]
