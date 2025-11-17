"""Track Intervals.icu workouts uploaded to WattAttack per account."""
from __future__ import annotations

from typing import Optional

from .db_utils import db_connection, dict_cursor


def ensure_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS intervals_uploaded (
                tg_user_id BIGINT NOT NULL,
                intervals_event_id BIGINT NOT NULL,
                account_id TEXT NOT NULL,
                status TEXT NOT NULL,
                info TEXT,
                uploaded_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (tg_user_id, intervals_event_id, account_id)
            )
            """
        )
        cur.execute(
            "ALTER TABLE intervals_uploaded ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'success'"
        )
        cur.execute(
            "ALTER TABLE intervals_uploaded ADD COLUMN IF NOT EXISTS info TEXT"
        )
        cur.execute(
            "ALTER TABLE intervals_uploaded ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMP DEFAULT NOW()"
        )
        conn.commit()


def is_uploaded(tg_user_id: int, event_id: int, account_id: str) -> bool:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT 1 FROM intervals_uploaded
            WHERE tg_user_id = %s AND intervals_event_id = %s AND account_id = %s
            LIMIT 1
            """,
            (tg_user_id, event_id, account_id),
        )
        row = cur.fetchone()
    return bool(row)


def record_upload(
    tg_user_id: int,
    event_id: int,
    account_id: str,
    *,
    status: str = "success",
    info: Optional[str] = None,
) -> None:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO intervals_uploaded (
                tg_user_id, intervals_event_id, account_id, status, info, uploaded_at
            ) VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (tg_user_id, intervals_event_id, account_id) DO UPDATE SET
                status = EXCLUDED.status,
                info = EXCLUDED.info,
                uploaded_at = NOW()
            """,
            (tg_user_id, event_id, account_id, status, info),
        )
        conn.commit()
