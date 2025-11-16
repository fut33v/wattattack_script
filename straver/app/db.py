"""Database helpers for persisting Strava credentials."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

DATABASE_URL: Optional[str] = None


def configure(dsn: str) -> None:
    """Set the DSN used for new DB connections."""
    global DATABASE_URL
    DATABASE_URL = dsn


@contextmanager
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("Database URL is not configured")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def ensure_tables() -> None:
    """Create tables used by Straver."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS strava_tokens (
                tg_user_id BIGINT PRIMARY KEY,
                access_token TEXT NOT NULL,
                refresh_token TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                athlete_id BIGINT,
                athlete_firstname TEXT,
                athlete_lastname TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS strava_tokens_expires_at_idx ON strava_tokens (expires_at)"
        )
        conn.commit()


def serialize_row(row: psycopg2.extras.RealDictRow) -> Dict:
    """Convert DB row to a regular dict with basic types."""
    data = dict(row)
    expires_at = data.get("expires_at")
    if isinstance(expires_at, datetime):
        data["expires_at"] = expires_at.isoformat()
    return data


def save_tokens(
    *,
    tg_user_id: int,
    access_token: str,
    refresh_token: str,
    expires_at: datetime,
    athlete_id: Optional[int],
    athlete_firstname: Optional[str],
    athlete_lastname: Optional[str],
) -> Dict:
    """Insert or update a token row."""
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO strava_tokens (
                tg_user_id,
                access_token,
                refresh_token,
                expires_at,
                athlete_id,
                athlete_firstname,
                athlete_lastname,
                created_at,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (tg_user_id) DO UPDATE SET
                access_token = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                expires_at = EXCLUDED.expires_at,
                athlete_id = EXCLUDED.athlete_id,
                athlete_firstname = EXCLUDED.athlete_firstname,
                athlete_lastname = EXCLUDED.athlete_lastname,
                updated_at = NOW()
            RETURNING tg_user_id, access_token, refresh_token, expires_at, athlete_id, athlete_firstname, athlete_lastname, created_at, updated_at
            """,
            (
                tg_user_id,
                access_token,
                refresh_token,
                expires_at,
                athlete_id,
                athlete_firstname,
                athlete_lastname,
            ),
        )
        row = cur.fetchone()
        conn.commit()
    return serialize_row(row) if row else {}


def get_token(tg_user_id: int) -> Optional[Dict]:
    """Return a token record for the Telegram user."""
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT tg_user_id, access_token, refresh_token, expires_at, athlete_id, athlete_firstname, athlete_lastname
            FROM strava_tokens
            WHERE tg_user_id = %s
            """,
            (tg_user_id,),
        )
        row = cur.fetchone()
    return serialize_row(row) if row else None


def get_tokens_for_users(user_ids: Iterable[int]) -> Dict[int, Dict]:
    """Fetch token records for the given users."""
    ids: List[int] = list(user_ids)
    if not ids:
        return {}
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT tg_user_id, access_token, refresh_token, expires_at, athlete_id, athlete_firstname, athlete_lastname
            FROM strava_tokens
            WHERE tg_user_id = ANY(%s)
            """,
            (ids,),
        )
        rows = cur.fetchall()
    return {int(row["tg_user_id"]): serialize_row(row) for row in rows}


def delete_token(tg_user_id: int) -> bool:
    """Remove stored tokens for a user."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM strava_tokens WHERE tg_user_id = %s", (tg_user_id,))
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted
