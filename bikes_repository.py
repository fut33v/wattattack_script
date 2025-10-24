"""Database utilities for managing WattAttack bicycle inventory."""
from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

from db_utils import db_connection, dict_cursor


def ensure_bikes_table() -> None:
    """Create the bikes table if missing and ensure indexes exist."""

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bikes (
                id SERIAL PRIMARY KEY,
                position INTEGER,
                title TEXT NOT NULL,
                owner TEXT,
                size_label TEXT,
                frame_size_cm TEXT,
                height_min_cm NUMERIC,
                height_max_cm NUMERIC,
                gears TEXT,
                axle_type TEXT,
                cassette TEXT
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS bikes_title_idx ON bikes (title)"
        )
        conn.commit()


def truncate_bikes() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("TRUNCATE TABLE bikes")
        conn.commit()


def upsert_bikes(rows: Iterable[Dict]) -> Tuple[int, int]:
    """Insert or update bicycle rows.

    Returns a tuple (inserted, updated).
    """

    inserted = 0
    updated = 0

    with db_connection() as conn, dict_cursor(conn) as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO bikes (
                    position,
                    title,
                    owner,
                    size_label,
                    frame_size_cm,
                    height_min_cm,
                    height_max_cm,
                    gears,
                    axle_type,
                    cassette
                )
                VALUES (%(position)s, %(title)s, %(owner)s, %(size_label)s, %(frame_size_cm)s,
                        %(height_min_cm)s, %(height_max_cm)s, %(gears)s, %(axle_type)s, %(cassette)s)
                ON CONFLICT (title) DO UPDATE
                SET
                    position = EXCLUDED.position,
                    owner = EXCLUDED.owner,
                    size_label = EXCLUDED.size_label,
                    frame_size_cm = EXCLUDED.frame_size_cm,
                    height_min_cm = EXCLUDED.height_min_cm,
                    height_max_cm = EXCLUDED.height_max_cm,
                    gears = EXCLUDED.gears,
                    axle_type = EXCLUDED.axle_type,
                    cassette = EXCLUDED.cassette
                """,
                row,
            )
            if cur.statusmessage.startswith("INSERT"):
                inserted += 1
            else:
                updated += 1
        conn.commit()

    return inserted, updated


def list_bikes(limit: int | None = None, offset: int = 0) -> List[Dict]:
    """Return bicycles ordered by explicit position then title."""

    query = (
        "SELECT id, position, title, owner, size_label, frame_size_cm, height_min_cm, "
        "height_max_cm, gears, axle_type, cassette "
        "FROM bikes "
        "ORDER BY position NULLS LAST, title"
    )
    params: tuple = ()
    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params = (limit, offset)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return rows


def search_bikes(term: str, limit: int = 20) -> List[Dict]:
    """Search bicycles by title, owner or axle/cassette fields."""

    pattern = f"%{term}%"
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, position, title, owner, size_label, frame_size_cm, height_min_cm,
                   height_max_cm, gears, axle_type, cassette
            FROM bikes
            WHERE title ILIKE %s
               OR COALESCE(owner, '') ILIKE %s
               OR COALESCE(size_label, '') ILIKE %s
               OR COALESCE(axle_type, '') ILIKE %s
               OR COALESCE(cassette, '') ILIKE %s
            ORDER BY position NULLS LAST, title
            LIMIT %s
            """,
            (pattern, pattern, pattern, pattern, pattern, limit),
        )
        rows = cur.fetchall()
    return rows


def bikes_count() -> int:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM bikes")
        row = cur.fetchone()
    return int(row.get("cnt", 0)) if row else 0


def find_bikes_for_height(height_cm: float, limit: int = 10) -> List[Dict]:
    """Return bicycles that match the provided rider height."""

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, position, title, owner, size_label, frame_size_cm,
                   height_min_cm, height_max_cm, gears, axle_type, cassette
            FROM bikes
            WHERE (height_min_cm IS NULL OR height_min_cm <= %s)
              AND (height_max_cm IS NULL OR height_max_cm >= %s)
            ORDER BY
                position NULLS LAST,
                ABS(COALESCE((height_min_cm + height_max_cm) / 2.0, %s) - %s),
                title
            LIMIT %s
            """,
            (height_cm, height_cm, height_cm, height_cm, limit),
        )
        rows = cur.fetchall()
    return rows
