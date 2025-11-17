"""Store last sent Intervals.icu plan hash per user."""
from __future__ import annotations

from typing import Optional

from .db_utils import db_connection, dict_cursor


def ensure_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS intervals_plan_cache (
                tg_user_id BIGINT PRIMARY KEY,
                plan_hash TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "ALTER TABLE intervals_plan_cache ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()"
        )
        cur.execute(
            "ALTER TABLE intervals_plan_cache ALTER COLUMN updated_at SET DEFAULT NOW()"
        )
        conn.commit()


def get_plan_hash(tg_user_id: int) -> Optional[str]:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT plan_hash FROM intervals_plan_cache WHERE tg_user_id = %s",
            (tg_user_id,),
        )
        row = cur.fetchone()
    return row["plan_hash"] if row else None


def upsert_plan_hash(tg_user_id: int, plan_hash: str) -> None:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO intervals_plan_cache (tg_user_id, plan_hash, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (tg_user_id) DO UPDATE SET
                plan_hash = EXCLUDED.plan_hash,
                updated_at = NOW()
            """,
            (tg_user_id, plan_hash),
        )
        conn.commit()
