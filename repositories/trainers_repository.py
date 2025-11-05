"""Database utilities for managing WattAttack trainer inventory."""
from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

from .db_utils import db_connection, dict_cursor
from .layout_repository import ensure_layout_table


def ensure_trainers_table() -> None:
    """Create the trainers table if missing and ensure indexes exist."""

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trainers (
                id SERIAL PRIMARY KEY,
                position INTEGER,
                code TEXT UNIQUE,
                title TEXT,
                display_name TEXT,
                owner TEXT,
                axle_types TEXT,
                cassette TEXT,
                notes TEXT
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS trainers_code_idx ON trainers (code)"
        )
        conn.commit()


def get_trainer(trainer_id: int) -> Dict | None:
    ensure_layout_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                t.id,
                t.position,
                t.code,
                t.title,
                t.display_name,
                t.owner,
                t.axle_types,
                t.cassette,
                t.notes,
                bl.bike_id,
                b.title AS bike_title,
                b.owner AS bike_owner
            FROM trainers AS t
            LEFT JOIN bike_layout AS bl ON bl.stand_id = t.id
            LEFT JOIN bikes AS b ON b.id = bl.bike_id
            WHERE t.id = %s
            """,
            (trainer_id,),
        )
        row = cur.fetchone()
    return row


def truncate_trainers() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("TRUNCATE TABLE trainers")
        conn.commit()


def upsert_trainers(rows: Iterable[Dict]) -> Tuple[int, int]:
    """Insert or update trainer rows. Returns (inserted, updated)."""

    inserted = 0
    updated = 0

    with db_connection() as conn, dict_cursor(conn) as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO trainers (
                    position,
                    code,
                    title,
                    display_name,
                    owner,
                    axle_types,
                    cassette,
                    notes
                )
                VALUES (
                    %(position)s,
                    %(code)s,
                    %(title)s,
                    %(display_name)s,
                    %(owner)s,
                    %(axle_types)s,
                    %(cassette)s,
                    %(notes)s
                )
                ON CONFLICT (code) DO UPDATE
                SET
                    position = EXCLUDED.position,
                    title = EXCLUDED.title,
                    display_name = EXCLUDED.display_name,
                    owner = EXCLUDED.owner,
                    axle_types = EXCLUDED.axle_types,
                    cassette = EXCLUDED.cassette,
                    notes = EXCLUDED.notes
                """,
                row,
            )
            if cur.statusmessage.startswith("INSERT"):
                inserted += 1
            else:
                updated += 1
        conn.commit()

    return inserted, updated


def list_trainers(limit: int | None = None, offset: int = 0) -> List[Dict]:
    """Return trainer inventory ordered by position then code."""

    ensure_layout_table()

    query = (
        "SELECT "
        "t.id, "
        "t.position, "
        "t.code, "
        "t.title, "
        "t.display_name, "
        "t.owner, "
        "t.axle_types, "
        "t.cassette, "
        "t.notes, "
        "bl.bike_id, "
        "b.title AS bike_title, "
        "b.owner AS bike_owner "
        "FROM trainers AS t "
        "LEFT JOIN bike_layout AS bl ON bl.stand_id = t.id "
        "LEFT JOIN bikes AS b ON b.id = bl.bike_id "
        "ORDER BY t.position NULLS LAST, t.code"
    )
    params: tuple = ()
    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params = (limit, offset)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return rows


def search_trainers(term: str, limit: int = 20) -> List[Dict]:
    """Search trainers by code, title, display_name or owner."""

    pattern = f"%{term}%"
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, position, code, title, display_name, owner, axle_types, cassette, notes
            FROM trainers
            WHERE COALESCE(code, '') ILIKE %s
               OR COALESCE(title, '') ILIKE %s
               OR COALESCE(display_name, '') ILIKE %s
               OR COALESCE(owner, '') ILIKE %s
            ORDER BY position NULLS LAST, code
            LIMIT %s
            """,
            (pattern, pattern, pattern, pattern, limit),
        )
        rows = cur.fetchall()
    return rows


def trainers_count() -> int:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM trainers")
        row = cur.fetchone()
    return int(row.get("cnt", 0)) if row else 0


EDITABLE_TRAINER_FIELDS = {
    "title": "title",
    "display_name": "display_name",
    "owner": "owner",
    "axle_types": "axle_types",
    "cassette": "cassette",
}


def update_trainer_fields(trainer_id: int, **fields: object) -> bool:
    updates = {
        EDITABLE_TRAINER_FIELDS[key]: value
        for key, value in fields.items()
        if key in EDITABLE_TRAINER_FIELDS
    }
    if not updates:
        return False

    assignments = ", ".join(f"{column} = %s" for column in updates)
    values = list(updates.values())
    values.append(trainer_id)

    query = f"UPDATE trainers SET {assignments} WHERE id = %s"
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
        conn.commit()
    return True
