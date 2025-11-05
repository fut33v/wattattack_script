"""Database helpers for bike-to-stand layout assignments."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db_utils import db_connection, dict_cursor


def ensure_layout_table() -> None:
    """Create the bike layout table and indexes if absent."""

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bike_layout (
                stand_id INTEGER PRIMARY KEY,
                bike_id INTEGER NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                assigned_by INTEGER
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS bike_layout_bike_idx ON bike_layout (bike_id)"
        )
        conn.commit()


def list_layout_details() -> List[Dict]:
    """Return current layout with bike and stand metadata."""

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                bl.stand_id,
                bl.bike_id,
                bl.assigned_at,
                bl.assigned_by,
                t.code AS stand_code,
                t.title AS stand_title,
                t.display_name AS stand_display,
                t.position AS stand_position,
                b.title AS bike_title,
                b.owner AS bike_owner,
                b.size_label AS bike_size
            FROM bike_layout AS bl
            LEFT JOIN trainers AS t ON t.id = bl.stand_id
            LEFT JOIN bikes AS b ON b.id = bl.bike_id
            ORDER BY
                t.position NULLS LAST,
                COALESCE(t.code, t.display_name, t.title),
                b.title
            """
        )
        rows = cur.fetchall()
    return rows


def get_assignment_for_bike(bike_id: int) -> Optional[Dict]:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                bl.stand_id,
                bl.bike_id,
                bl.assigned_at,
                bl.assigned_by,
                t.code AS stand_code,
                t.title AS stand_title,
                t.display_name AS stand_display,
                t.position AS stand_position
            FROM bike_layout AS bl
            LEFT JOIN trainers AS t ON t.id = bl.stand_id
            WHERE bl.bike_id = %s
            """,
            (bike_id,),
        )
        row = cur.fetchone()
    return row


def get_assignment_for_stand(stand_id: int) -> Optional[Dict]:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                bl.stand_id,
                bl.bike_id,
                bl.assigned_at,
                bl.assigned_by
            FROM bike_layout AS bl
            WHERE bl.stand_id = %s
            """,
            (stand_id,),
        )
        row = cur.fetchone()
    return row


def set_bike_assignment(stand_id: int, bike_id: int, assigned_by: Optional[int] = None) -> None:
    """Assign the bike to the stand, replacing existing links."""

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM bike_layout WHERE stand_id = %s OR bike_id = %s",
            (stand_id, bike_id),
        )
        cur.execute(
            """
            INSERT INTO bike_layout (stand_id, bike_id, assigned_at, assigned_by)
            VALUES (%s, %s, NOW(), %s)
            """,
            (stand_id, bike_id, assigned_by),
        )
        conn.commit()


def clear_bike_assignment_for_stand(stand_id: int) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM bike_layout WHERE stand_id = %s", (stand_id,))
        conn.commit()


def clear_bike_assignment_for_bike(bike_id: int) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM bike_layout WHERE bike_id = %s", (bike_id,))
        conn.commit()
