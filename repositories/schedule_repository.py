"""Helpers for managing training schedule weeks, slots, and reservations."""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from pathlib import Path
import os

from .db_utils import db_connection, dict_cursor
from . import trainers_repository, instructors_repository

LOGGER = logging.getLogger(__name__)
FIT_FILES_DIR = Path(os.environ.get("FIT_FILES_DIR", "data/fit_files")).resolve()


DEFAULT_TEMPLATE_SLOTS: Tuple[Tuple[str, str, str, Optional[str]], ...] = (
    ("06:00", "08:00", "self_service", None),
    ("08:30", "10:30", "self_service", None),
    ("10:30", "12:30", "self_service", None),
    ("12:30", "14:30", "self_service", None),
    ("14:30", "16:30", "self_service", None),
    ("16:30", "18:30", "self_service", None),
    ("18:30", "20:30", "self_service", None),
    ("20:30", "22:30", "self_service", None),
)


def ensure_schedule_tables() -> None:
    """Create schedule-related tables and indexes when missing."""

    instructors_repository.ensure_instructors_table()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_weeks (
                id SERIAL PRIMARY KEY,
                week_start_date DATE NOT NULL UNIQUE,
                title TEXT,
                notes TEXT,
                copied_from_week_id INTEGER REFERENCES schedule_weeks (id) ON DELETE SET NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_slots (
                id SERIAL PRIMARY KEY,
                week_id INTEGER NOT NULL REFERENCES schedule_weeks (id) ON DELETE CASCADE,
                slot_date DATE NOT NULL,
                start_time TIME NOT NULL,
                end_time TIME NOT NULL,
                label TEXT,
                session_kind TEXT NOT NULL DEFAULT 'self_service',
                is_cancelled BOOLEAN NOT NULL DEFAULT FALSE,
                sort_index INTEGER,
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT schedule_slots_time_check CHECK (end_time > start_time)
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE schedule_slots
            ADD COLUMN IF NOT EXISTS instructor_id INTEGER REFERENCES schedule_instructors (id) ON DELETE SET NULL
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS schedule_slots_instructor_idx
            ON schedule_slots (instructor_id)
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS schedule_slots_unique_time_idx
                ON schedule_slots (week_id, slot_date, start_time, end_time, COALESCE(label, ''))
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS schedule_slots_week_idx
                ON schedule_slots (week_id, slot_date, start_time)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_reservations (
                id SERIAL PRIMARY KEY,
                slot_id INTEGER NOT NULL REFERENCES schedule_slots (id) ON DELETE CASCADE,
                stand_id INTEGER REFERENCES trainers (id) ON DELETE SET NULL,
                stand_code TEXT,
                client_id INTEGER REFERENCES clients (id) ON DELETE SET NULL,
                client_name TEXT,
                status TEXT NOT NULL DEFAULT 'booked',
                source TEXT,
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS schedule_reservations_stand_unique_idx
                ON schedule_reservations (slot_id, stand_id)
                WHERE stand_id IS NOT NULL
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS schedule_reservations_code_unique_idx
                ON schedule_reservations (slot_id, stand_code)
                WHERE stand_id IS NULL AND stand_code IS NOT NULL
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS schedule_reservations_slot_idx
                ON schedule_reservations (slot_id)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_account_assignments (
                id SERIAL PRIMARY KEY,
                reservation_id INTEGER NOT NULL REFERENCES schedule_reservations (id) ON DELETE CASCADE,
                account_id TEXT NOT NULL,
                client_id INTEGER REFERENCES clients (id) ON DELETE SET NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS schedule_account_assignments_reservation_idx
                ON schedule_account_assignments (reservation_id, account_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS schedule_account_assignments_account_idx
                ON schedule_account_assignments (account_id)
            """
        )
        conn.commit()


def _normalize_week_start(value: date) -> date:
    """Return Monday of the week for given date."""

    if not isinstance(value, date):
        raise TypeError("week_start_date must be a date instance")
    return value - timedelta(days=value.weekday())


def create_week(
    *,
    week_start_date: date,
    title: Optional[str] = None,
    notes: Optional[str] = None,
    copied_from_week_id: Optional[int] = None,
) -> Dict:
    """Insert a new week. Raises ValueError if already exists."""

    ensure_schedule_tables()
    normalized = _normalize_week_start(week_start_date)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO schedule_weeks (week_start_date, title, notes, copied_from_week_id)
            VALUES (%(week_start_date)s, %(title)s, %(notes)s, %(copied_from_week_id)s)
            ON CONFLICT (week_start_date) DO NOTHING
            RETURNING *
            """,
            {
                "week_start_date": normalized,
                "title": title,
                "notes": notes,
                "copied_from_week_id": copied_from_week_id,
            },
        )
        row = cur.fetchone()
        conn.commit()

    if not row:
        raise ValueError("Week already exists")
    return row


def update_week(week_id: int, **fields: object) -> Optional[Dict]:
    """Patch existing week metadata."""

    ensure_schedule_tables()
    if not fields:
        return get_week(week_id)

    assignments = []
    params: List[object] = []
    for column, value in fields.items():
        if column == "week_start_date" and isinstance(value, (date, str)):
            if isinstance(value, str):
                # Try to parse string as date
                try:
                    parsed_date = datetime.strptime(value, "%Y-%m-%d").date()
                    value = _normalize_week_start(parsed_date)
                except ValueError:
                    # If parsing fails, leave value as is
                    pass
            else:
                value = _normalize_week_start(value)
        assignments.append(f"{column} = %s")
        params.append(value)
    params.append(week_id)

    query = f"""
        UPDATE schedule_weeks
        SET {', '.join(assignments)}, updated_at = NOW()
        WHERE id = %s
        RETURNING *
    """

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        conn.commit()
    return row


def list_weeks(*, limit: int = 20, offset: int = 0) -> List[Dict]:
    """List schedule weeks ordered by start date descending."""

    ensure_schedule_tables()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                w.*,
                (
                    SELECT COUNT(*) FROM schedule_slots s WHERE s.week_id = w.id
                ) AS slots_count
            FROM schedule_weeks AS w
            ORDER BY w.week_start_date DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
    return rows


def count_weeks() -> int:
    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM schedule_weeks")
        row = cur.fetchone() or {}
    return int(row.get("cnt", 0))


def get_week(week_id: int) -> Optional[Dict]:
    ensure_schedule_tables()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM schedule_weeks WHERE id = %s", (week_id,))
        row = cur.fetchone()
    return row


def get_week_by_start(week_start_date: date) -> Optional[Dict]:
    ensure_schedule_tables()
    normalized = _normalize_week_start(week_start_date)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM schedule_weeks WHERE week_start_date = %s", (normalized,))
        row = cur.fetchone()
    return row


def get_adjacent_week_starts(week_start_date: date) -> Dict[str, Optional[date]]:
    """Return previous/next existing weeks relative to the given start date."""

    ensure_schedule_tables()
    normalized = _normalize_week_start(week_start_date)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                (
                    SELECT week_start_date
                    FROM schedule_weeks
                    WHERE week_start_date < %s
                    ORDER BY week_start_date DESC
                    LIMIT 1
                ) AS prev_week_start,
                (
                    SELECT week_start_date
                    FROM schedule_weeks
                    WHERE week_start_date > %s
                    ORDER BY week_start_date ASC
                    LIMIT 1
                ) AS next_week_start
            """,
            (normalized, normalized),
        )
        row = cur.fetchone() or {}

    return {
        "previous": row.get("prev_week_start"),
        "next": row.get("next_week_start"),
    }


def get_or_create_week(
    *,
    week_start_date: date,
    title: Optional[str] = None,
    notes: Optional[str] = None,
    copied_from_week_id: Optional[int] = None,
) -> Dict:
    """
    Return existing week or create a new one.

    When created, optional title/notes/copied_from_week_id values are applied.
    """

    existing = get_week_by_start(week_start_date)
    if existing:
        return existing
    try:
        return create_week(
            week_start_date=week_start_date,
            title=title,
            notes=notes,
            copied_from_week_id=copied_from_week_id,
        )
    except ValueError:
        # Week was created concurrently
        existing = get_week_by_start(week_start_date)
        if existing:
            return existing
        raise


def delete_week(week_id: int) -> None:
    ensure_schedule_tables()

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule_weeks WHERE id = %s", (week_id,))
        conn.commit()


def create_slot(
    *,
    week_id: int,
    slot_date: date,
    start_time: time,
    end_time: time,
    label: Optional[str] = None,
    session_kind: str = "self_service",
    sort_index: Optional[int] = None,
    notes: Optional[str] = None,
    instructor_id: Optional[int] = None,
) -> Dict:
    ensure_schedule_tables()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO schedule_slots (
                week_id, slot_date, start_time, end_time,
                label, session_kind, sort_index, notes, instructor_id
            )
            VALUES (%(week_id)s, %(slot_date)s, %(start_time)s, %(end_time)s,
                    %(label)s, %(session_kind)s, %(sort_index)s, %(notes)s, %(instructor_id)s)
            RETURNING *
            """,
            {
                "week_id": week_id,
                "slot_date": slot_date,
                "start_time": start_time,
                "end_time": end_time,
                "label": label,
                "session_kind": session_kind,
                "sort_index": sort_index,
                "notes": notes,
                "instructor_id": instructor_id,
            },
        )
        row = cur.fetchone()
        if row:
            _ensure_slot_capacity(conn, row["id"])
        conn.commit()
    return row or {}


def update_slot(slot_id: int, **fields: object) -> Optional[Dict]:
    """Update slot fields and return updated row."""

    ensure_schedule_tables()
    if not fields:
        return get_slot(slot_id)

    assignments = []
    params: List[object] = []
    for column, value in fields.items():
        if column == "instructor_id" and value == "":
            value = None
        assignments.append(f"{column} = %s")
        params.append(value)
    params.append(slot_id)

    query = f"""
        UPDATE schedule_slots
        SET {', '.join(assignments)}, updated_at = NOW()
        WHERE id = %s
        RETURNING *
    """

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        conn.commit()
    return row


def get_slot(slot_id: int) -> Optional[Dict]:
    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM schedule_slots WHERE id = %s", (slot_id,))
        row = cur.fetchone()
    return row


def delete_slot(slot_id: int) -> None:
    ensure_schedule_tables()
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule_slots WHERE id = %s", (slot_id,))
        conn.commit()


def clear_week_slots(week_id: int) -> int:
    """Delete all slots (and reservations) for the week. Returns number of deleted slots."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("DELETE FROM schedule_slots WHERE week_id = %s RETURNING id", (week_id,))
        deleted = cur.rowcount
        conn.commit()
    return int(deleted or 0)


def sync_week_capacity(week_id: int) -> int:
    """Ensure every slot in the week has placeholders for all stands. Returns total inserted placeholders."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT id FROM schedule_slots WHERE week_id = %s", (week_id,))
        rows = cur.fetchall()
        inserted = 0
        for row in rows:
            inserted += _ensure_slot_capacity(conn, row["id"])
        conn.commit()
    return inserted


def create_default_slots_for_week(week_id: int, *, force: bool = False) -> int:
    """Populate week with the default template slots. Returns number of new slots."""

    ensure_schedule_tables()
    week = get_week(week_id)
    if not week:
        raise ValueError("Week not found")

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT slot_date, start_time, end_time FROM schedule_slots WHERE week_id = %s",
            (week_id,),
        )
        existing = {
            (row["slot_date"], row["start_time"], row["end_time"])
            for row in cur.fetchall()
        }

    if existing and not force:
        return 0

    if existing and force:
        clear_week_slots(week_id)
        existing.clear()

    start_date: date = week["week_start_date"]
    created = 0

    with db_connection() as conn, dict_cursor(conn) as cur:
        for day_offset in range(7):
            current_date = start_date + timedelta(days=day_offset)
            for start_str, end_str, session_kind, label in DEFAULT_TEMPLATE_SLOTS:
                start_time = datetime.strptime(start_str, "%H:%M").time()
                end_time = datetime.strptime(end_str, "%H:%M").time()
                key = (current_date, start_time, end_time)
                if key in existing:
                    continue
                cur.execute(
                    """
                    INSERT INTO schedule_slots (
                        week_id, slot_date, start_time, end_time,
                        label, session_kind
                    )
                    VALUES (%(week_id)s, %(slot_date)s, %(start_time)s, %(end_time)s,
                            %(label)s, %(session_kind)s)
                    RETURNING id
                    """,
                    {
                        "week_id": week_id,
                        "slot_date": current_date,
                        "start_time": start_time,
                        "end_time": end_time,
                        "label": label,
                        "session_kind": session_kind,
                    },
                )
                row = cur.fetchone()
                if not row:
                    continue
                slot_id = row["id"]
                created += 1
                existing.add(key)
                _ensure_slot_capacity(conn, slot_id)
        conn.commit()

    return created


def list_slots_with_reservations(week_id: int) -> List[Dict]:
    """Return slots for the week including reservation lists."""

    ensure_schedule_tables()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                s.*,
                i.full_name AS instructor_name
            FROM schedule_slots AS s
            LEFT JOIN schedule_instructors AS i ON i.id = s.instructor_id
            WHERE s.week_id = %s
            ORDER BY s.slot_date, s.start_time, COALESCE(s.sort_index, 0), s.id
            """,
            (week_id,),
        )
        slots = cur.fetchall()

        if not slots:
            return []

        slot_ids = [slot["id"] for slot in slots]
        cur.execute(
            """
            SELECT r.*
            FROM schedule_reservations AS r
            WHERE r.slot_id = ANY(%s)
            ORDER BY r.id
            """,
            (slot_ids,),
        )
        reservations = cur.fetchall()

    by_slot: Dict[int, List[Dict]] = {slot["id"]: [] for slot in slots}
    for reservation in reservations:
        by_slot.setdefault(reservation["slot_id"], []).append(reservation)

    for slot in slots:
        slot["reservations"] = by_slot.get(slot["id"], [])
    return slots


def list_reservations_by_date(target_date: date) -> List[Dict]:
    """Return all reservations for a specific date with client_id filled."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT r.*, s.slot_date, s.start_time, s.end_time
            FROM schedule_reservations AS r
            JOIN schedule_slots AS s ON s.id = r.slot_id
            WHERE s.slot_date = %s
            """,
            (target_date,),
        )
        rows = cur.fetchall()
    return rows


def clear_reservations_for_slot(slot_id: int) -> None:
    ensure_schedule_tables()
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule_reservations WHERE slot_id = %s", (slot_id,))
            _ensure_slot_capacity(conn, slot_id)
        conn.commit()


def create_reservation(
    *,
    slot_id: int,
    stand_id: Optional[int],
    stand_code: Optional[str],
    client_id: Optional[int],
    client_name: Optional[str],
    status: str = "booked",
    source: Optional[str] = None,
    notes: Optional[str] = None,
) -> Dict:
    ensure_schedule_tables()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO schedule_reservations (
                slot_id, stand_id, stand_code,
                client_id, client_name, status, source, notes
            )
            VALUES (%(slot_id)s, %(stand_id)s, %(stand_code)s,
                    %(client_id)s, %(client_name)s, %(status)s, %(source)s, %(notes)s)
            RETURNING *
            """,
            {
                "slot_id": slot_id,
                "stand_id": stand_id,
                "stand_code": stand_code,
                "client_id": client_id,
                "client_name": client_name,
                "status": status,
                "source": source,
                "notes": notes,
            },
        )
        row = cur.fetchone()
        conn.commit()
    return row or {}


def update_reservation(reservation_id: int, **fields: object) -> Optional[Dict]:
    ensure_schedule_tables()
    if not fields:
        return get_reservation(reservation_id)

    assignments = []
    params: List[object] = []
    for column, value in fields.items():
        assignments.append(f"{column} = %s")
        params.append(value)
    params.append(reservation_id)

    query = f"""
        UPDATE schedule_reservations
        SET {', '.join(assignments)}, updated_at = NOW()
        WHERE id = %s
        RETURNING *
    """

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        conn.commit()
    return row


def get_reservation(reservation_id: int) -> Optional[Dict]:
    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM schedule_reservations WHERE id = %s", (reservation_id,))
        row = cur.fetchone()
    return row


def delete_reservation(reservation_id: int) -> None:
    ensure_schedule_tables()
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedule_reservations WHERE id = %s", (reservation_id,))
        conn.commit()


def get_reservation_for_stand(slot_id: int, stand_id: int) -> Optional[Dict]:
    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM schedule_reservations
            WHERE slot_id = %s AND stand_id = %s
            """,
            (slot_id, stand_id),
        )
        row = cur.fetchone()
    return row


def copy_slots_from_week(source_week_id: int, target_week_id: int) -> Tuple[int, int]:
    """Duplicate slots from source week into target week. Returns (created_slots, copied_placeholders)."""

    ensure_schedule_tables()

    source_week = get_week(source_week_id)
    target_week = get_week(target_week_id)
    if not source_week or not target_week:
        raise ValueError("Source or target week not found")

    delta_days = target_week["week_start_date"] - source_week["week_start_date"]
    if delta_days is None:
        delta_days = timedelta(days=0)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT * FROM schedule_slots WHERE week_id = %s ORDER BY slot_date, start_time, id",
            (source_week_id,),
        )
        source_slots = cur.fetchall()

        created_slots = 0
        copied_placeholders = 0

        for slot in source_slots:
            cur.execute(
                """
                INSERT INTO schedule_slots (
                    week_id, slot_date, start_time, end_time,
                    label, session_kind, is_cancelled, sort_index, notes, instructor_id
                )
                VALUES (%(week_id)s, %(slot_date)s, %(start_time)s, %(end_time)s,
                        %(label)s, %(session_kind)s, %(is_cancelled)s, %(sort_index)s, %(notes)s, %(instructor_id)s)
                ON CONFLICT (week_id, slot_date, start_time, end_time, COALESCE(label, ''))
                DO NOTHING
                RETURNING id
                """,
                {
                    "week_id": target_week_id,
                    "slot_date": slot["slot_date"] + delta_days,
                    "start_time": slot["start_time"],
                    "end_time": slot["end_time"],
                    "label": slot["label"],
                    "session_kind": slot["session_kind"],
                    "is_cancelled": slot["is_cancelled"],
                    "sort_index": slot["sort_index"],
                    "notes": slot["notes"],
                    "instructor_id": slot.get("instructor_id"),
                },
            )
            new_slot_row = cur.fetchone()
            if not new_slot_row:
                continue
            created_slots += 1
            new_slot_id = new_slot_row["id"]
            placeholders = _ensure_slot_capacity(conn, new_slot_id)
            copied_placeholders += placeholders

        conn.commit()

    return created_slots, copied_placeholders


def list_future_slots_for_copy(
    slot_date: date,
    start_time: time,
    exclude_slot_id: Optional[int] = None,
) -> List[Dict]:
    """Return slots on or after the given datetime (excluding the current slot)."""

    ensure_schedule_tables()
    params: List[object] = [slot_date, slot_date, start_time]
    exclude_clause = ""
    if exclude_slot_id is not None:
        exclude_clause = " AND s.id <> %s"
        params.append(exclude_slot_id)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT s.*, w.week_start_date, instr.full_name AS instructor_name
            FROM schedule_slots AS s
            JOIN schedule_weeks AS w ON w.id = s.week_id
            LEFT JOIN schedule_instructors AS instr ON instr.id = s.instructor_id
            WHERE (s.slot_date > %s OR (s.slot_date = %s AND s.start_time >= %s))
            {exclude_clause}
            ORDER BY s.slot_date, s.start_time, s.id
            """,
            params,
        )
        rows = cur.fetchall()
    return rows


def copy_slot_seating(source_slot_id: int, target_slot_id: int) -> Dict[str, Any]:
    """Copy seating (stand assignments) from one slot to another."""

    ensure_schedule_tables()
    if source_slot_id == target_slot_id:
        raise ValueError("Source and target slots must differ")

    source_slot = get_slot(source_slot_id)
    target_slot = get_slot(target_slot_id)
    if not source_slot or not target_slot:
        raise ValueError("Source or target slot not found")

    with db_connection() as conn, dict_cursor(conn) as cur:
        _ensure_slot_capacity(conn, target_slot_id)

        cur.execute("SELECT * FROM schedule_reservations WHERE slot_id = %s", (source_slot_id,))
        source_reservations = cur.fetchall()
        cur.execute("SELECT * FROM schedule_reservations WHERE slot_id = %s", (target_slot_id,))
        target_reservations = cur.fetchall()

        source_map = {
            res.get("stand_id"): res for res in source_reservations if res.get("stand_id") is not None
        }
        target_map = {
            res.get("stand_id"): res for res in target_reservations if res.get("stand_id") is not None
        }

        updated = 0
        cleared = 0
        missing_stands: List[int] = []

        for stand_id, target_reservation in target_map.items():
            source_reservation = source_map.get(stand_id)
            if source_reservation:
                cur.execute(
                    """
                    UPDATE schedule_reservations
                    SET client_id = %s,
                        client_name = %s,
                        status = %s,
                        notes = %s,
                        source = COALESCE(%s, 'copy'),
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (
                        source_reservation.get("client_id"),
                        source_reservation.get("client_name"),
                        source_reservation.get("status"),
                        source_reservation.get("notes"),
                        source_reservation.get("source"),
                        target_reservation["id"],
                    ),
                )
                cur.fetchone()
                updated += 1
            else:
                cur.execute(
                    """
                    UPDATE schedule_reservations
                    SET client_id = NULL,
                        client_name = NULL,
                        status = 'available',
                        notes = NULL,
                        source = 'copy',
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (target_reservation["id"],),
                )
                cur.fetchone()
                cleared += 1

        for stand_id in source_map.keys():
            if stand_id not in target_map and stand_id is not None:
                missing_stands.append(int(stand_id))

        conn.commit()

    return {
        "updated": updated,
        "cleared": cleared,
        "missing_stands": missing_stands,
    }


def reservations_for_slots(slot_ids: Iterable[int]) -> Dict[int, List[Dict]]:
    """Return reservations grouped by slot id."""

    ensure_schedule_tables()
    ids = list(slot_ids)
    if not ids:
        return {}

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM schedule_reservations
            WHERE slot_id = ANY(%s)
            ORDER BY slot_id, id
            """,
            (ids,),
        )
        rows = cur.fetchall()

    grouped: Dict[int, List[Dict]] = {}
    for row in rows:
        grouped.setdefault(row["slot_id"], []).append(row)
    return grouped


def list_available_slots(
    start_datetime: datetime,
    end_datetime: Optional[datetime] = None,
    limit: int = 120,
) -> List[Dict]:
    """Return upcoming slots with at least one available reservation."""

    ensure_schedule_tables()
    if end_datetime is not None and end_datetime < start_datetime:
        raise ValueError("end_datetime must not precede start_datetime")

    params: Dict[str, object] = {
        "start_date": start_datetime.date(),
        "start_time": start_datetime.time(),
        "limit": limit,
    }

    conditions = [
        "(s.slot_date > %(start_date)s OR (s.slot_date = %(start_date)s AND s.start_time > %(start_time)s))"
    ]
    if end_datetime is not None:
        params["end_date"] = end_datetime.date()
        params["end_time"] = end_datetime.time()
        conditions.append(
            "(s.slot_date < %(end_date)s OR (s.slot_date = %(end_date)s AND s.start_time <= %(end_time)s))"
        )

    where_clause = " AND ".join(["s.is_cancelled = FALSE"] + conditions)

    query = f"""
        SELECT
            s.id,
            s.week_id,
            s.slot_date,
            s.start_time,
            s.end_time,
            s.label,
            s.session_kind,
            s.instructor_id,
            i.full_name AS instructor_name,
            COUNT(*) FILTER (WHERE r.status = 'available') AS free_count,
            COUNT(*) AS total_count
        FROM schedule_slots AS s
        JOIN schedule_reservations AS r ON r.slot_id = s.id
        LEFT JOIN schedule_instructors AS i ON i.id = s.instructor_id
        WHERE {where_clause}
        GROUP BY
            s.id,
            s.week_id,
            s.slot_date,
            s.start_time,
            s.end_time,
            s.label,
            s.session_kind,
            s.instructor_id,
            i.full_name
        HAVING COUNT(*) FILTER (WHERE r.status = 'available') > 0
        ORDER BY s.slot_date, s.start_time, s.id
        LIMIT %(limit)s
    """

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return rows


def get_slot_with_reservations(slot_id: int) -> Optional[Dict]:
    """Return slot row together with its reservations."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                s.*,
                i.full_name AS instructor_name
            FROM schedule_slots AS s
            LEFT JOIN schedule_instructors AS i ON i.id = s.instructor_id
            WHERE s.id = %s
            """,
            (slot_id,),
        )
        slot = cur.fetchone()
        if not slot:
            return None

        cur.execute(
            """
            SELECT *
            FROM schedule_reservations
            WHERE slot_id = %s
            ORDER BY id
            """,
            (slot_id,),
        )
        reservations = cur.fetchall()

    slot["reservations"] = reservations
    return slot


def book_available_reservation(
    reservation_id: int,
    *,
    client_id: int,
    client_name: str,
    status: str = "booked",
    source: str = "clientbot",
    notes: Optional[str] = None,
) -> Optional[Dict]:
    """Mark placeholder reservation as occupied if it is still available."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            UPDATE schedule_reservations
            SET client_id = %s,
                client_name = %s,
                status = %s,
                source = %s,
                notes = %s,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'available'
            RETURNING *
            """,
            (client_id, client_name, status, source, notes, reservation_id),
        )
        row = cur.fetchone()
        conn.commit()
    return row


def list_future_reservations_for_client(client_id: int, since: datetime) -> List[Dict]:
    """Return upcoming reservations for a client (slots with start_time >= since)."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                r.*,
                s.slot_date,
                s.start_time,
                s.end_time,
                s.label,
                s.session_kind,
                s.instructor_id,
                i.full_name AS instructor_name,
                t.code AS stand_code,
                t.display_name AS stand_display_name,
                t.title AS stand_title,
                b.title AS bike_title,
                b.owner AS bike_owner
            FROM schedule_reservations AS r
            JOIN schedule_slots AS s ON s.id = r.slot_id
            LEFT JOIN schedule_instructors AS i ON i.id = s.instructor_id
            LEFT JOIN trainers AS t ON t.id = r.stand_id
            LEFT JOIN bike_layout AS bl ON bl.stand_id = t.id
            LEFT JOIN bikes AS b ON b.id = bl.bike_id
            WHERE r.client_id = %(client_id)s
              AND r.status <> 'cancelled'
              AND r.status <> 'legacy'
              AND r.status <> 'blocked'
              AND (
                    s.slot_date > %(since_date)s
                    OR (s.slot_date = %(since_date)s AND s.start_time >= %(since_time)s)
                  )
            ORDER BY s.slot_date, s.start_time, r.id
            """,
            {
                "client_id": client_id,
                "since_date": since.date(),
                "since_time": since.time(),
            },
        )
        rows = cur.fetchall()
    return rows


def list_past_reservations_for_client(client_id: int, before: datetime, limit: int = 10) -> List[Dict]:
    """Return past reservations for a client ordered from newest to oldest."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                r.*,
                s.slot_date,
                s.start_time,
                s.end_time,
                s.label,
                s.session_kind,
                s.instructor_id,
                i.full_name AS instructor_name,
                t.code AS stand_code,
                t.display_name AS stand_display_name,
                t.title AS stand_title,
                b.title AS bike_title,
                b.owner AS bike_owner
            FROM schedule_reservations AS r
            JOIN schedule_slots AS s ON s.id = r.slot_id
            LEFT JOIN schedule_instructors AS i ON i.id = s.instructor_id
            LEFT JOIN trainers AS t ON t.id = r.stand_id
            LEFT JOIN bike_layout AS bl ON bl.stand_id = t.id
            LEFT JOIN bikes AS b ON b.id = bl.bike_id
            WHERE r.client_id = %(client_id)s
              AND (
                    s.slot_date < %(before_date)s
                    OR (s.slot_date = %(before_date)s AND s.start_time < %(before_time)s)
                  )
            ORDER BY s.slot_date DESC, s.start_time DESC, r.id DESC
            LIMIT %(limit)s
            """,
            {
                "client_id": client_id,
                "before_date": before.date(),
                "before_time": before.time(),
                "limit": limit,
            },
        )
        rows = cur.fetchall()
    return rows


def _ensure_slot_capacity(conn, slot_id: int) -> int:
    """Ensure placeholder rows exist for all stands. Returns number of inserted placeholders."""

    trainers_repository.ensure_trainers_table()
    trainers = trainers_repository.list_trainers()
    if not trainers:
        return 0

    trainer_map = []
    for trainer in trainers:
        code = trainer.get("code") or trainer.get("display_name") or f"stand-{trainer['id']}"
        trainer_map.append((trainer["id"], code))
    placeholders = 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT stand_id FROM schedule_reservations WHERE slot_id = %s",
            (slot_id,),
        )
        existing_ids = {row[0] for row in cur.fetchall() if row[0] is not None}

        for trainer_id, trainer_code in trainer_map:
            if trainer_id in existing_ids:
                continue
            cur.execute(
                """
                INSERT INTO schedule_reservations (
                    slot_id, stand_id, stand_code, status, source
                )
                VALUES (%s, %s, %s, 'available', 'auto')
                """,
                (slot_id, trainer_id, trainer_code),
            )
            placeholders += 1
    return placeholders


def ensure_slot_capacity(slot_id: int) -> int:
    """Public helper to backfill placeholder reservations for a slot."""

    ensure_schedule_tables()
    with db_connection() as conn:
        placeholders = _ensure_slot_capacity(conn, slot_id)
        conn.commit()
    return placeholders


def ensure_workout_notifications_table() -> None:
    """Create table to track sent workout notifications."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS workout_notifications (
                id SERIAL PRIMARY KEY,
                reservation_id INTEGER NOT NULL REFERENCES schedule_reservations (id) ON DELETE CASCADE,
                notification_type TEXT NOT NULL,
                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(reservation_id, notification_type)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS workout_notifications_reservation_idx ON workout_notifications (reservation_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS workout_notifications_type_sent_idx ON workout_notifications (notification_type, sent_at)"
        )
        conn.commit()


def record_notification_sent(reservation_id: int, notification_type: str) -> bool:
    """Record that a notification was sent for a reservation."""
    ensure_workout_notifications_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        try:
            cur.execute(
                """
                INSERT INTO workout_notifications (reservation_id, notification_type)
                VALUES (%s, %s)
                ON CONFLICT (reservation_id, notification_type) DO NOTHING
                RETURNING id
                """,
                (reservation_id, notification_type)
            )
            row = cur.fetchone()
            conn.commit()
            return row is not None  # True if inserted, False if conflict
        except Exception:
            LOGGER.exception("Failed to record notification for reservation %s", reservation_id)
            return False


def was_notification_sent(reservation_id: int, notification_type: str) -> bool:
    """Check if a notification was already sent for a reservation."""
    ensure_workout_notifications_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT 1 FROM workout_notifications WHERE reservation_id = %s AND notification_type = %s",
            (reservation_id, notification_type)
        )
        return cur.fetchone() is not None


def list_workout_notifications(limit: int = 100, offset: int = 0) -> List[Dict]:
    """List all workout notifications with reservation and client details."""
    ensure_workout_notifications_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                wn.*,
                r.client_id,
                r.slot_id,
                r.stand_id,
                r.stand_code,
                r.client_name,
                r.status,
                s.slot_date,
                s.start_time,
                s.end_time,
                s.label,
                s.session_kind,
                c.first_name AS client_first_name,
                c.last_name AS client_last_name,
                c.full_name AS client_full_name,
                t.code AS stand_code,
                t.title AS stand_title
            FROM workout_notifications wn
            JOIN schedule_reservations r ON r.id = wn.reservation_id
            JOIN schedule_slots s ON s.id = r.slot_id
            LEFT JOIN clients c ON c.id = r.client_id
            LEFT JOIN trainers t ON t.id = r.stand_id
            ORDER BY wn.sent_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset)
        )
        rows = cur.fetchall()
    return rows


def get_workout_notification_settings() -> Dict[str, Any]:
    """Get workout notification settings from environment or database."""
    # For now, we'll just return the environment variable
    # In the future, we could store this in a settings table
    import os
    reminder_hours = int(os.environ.get("WORKOUT_REMINDER_HOURS", "4"))
    return {"reminder_hours": reminder_hours}


def update_workout_notification_settings(reminder_hours: int) -> bool:
    """Update workout notification settings.
    
    Note: This is a placeholder. In a real implementation, we would store
    this in a database settings table. For now, we just validate the input.
    """
    if reminder_hours < 1 or reminder_hours > 168:  # 1 hour to 1 week
        return False
    # In a real implementation, we would store this in the database
    return True


def ensure_fit_files_dir() -> Path:
    """Ensure directory for archived FIT files exists."""
    FIT_FILES_DIR.mkdir(parents=True, exist_ok=True)
    return FIT_FILES_DIR


def ensure_activity_ids_table() -> None:
    """Create table to track seen activity IDs."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_activity_ids (
                id SERIAL PRIMARY KEY,
                account_id TEXT NOT NULL,
                activity_id TEXT NOT NULL,
                client_id INTEGER,
                scheduled_name TEXT,
                start_time TIMESTAMPTZ,
                profile_name TEXT,
                sent_clientbot BOOLEAN DEFAULT FALSE,
                sent_strava BOOLEAN DEFAULT FALSE,
                sent_intervals BOOLEAN DEFAULT FALSE,
                distance DOUBLE PRECISION,
                elapsed_time INTEGER,
                elevation_gain DOUBLE PRECISION,
                average_power DOUBLE PRECISION,
                average_cadence DOUBLE PRECISION,
                average_heartrate DOUBLE PRECISION,
                fit_path TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(account_id, activity_id)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS seen_activity_ids_account_idx ON seen_activity_ids (account_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS seen_activity_ids_activity_idx ON seen_activity_ids (activity_id)"
        )
        cur.execute(
            """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'seen_activity_ids'
            """
        )
        existing = {row["column_name"] for row in cur.fetchall()}
        if "client_id" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS client_id INTEGER")
        if "manual_client_id" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS manual_client_id INTEGER")
        if "manual_client_name" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS manual_client_name TEXT")
        if "scheduled_name" not in existing:
            cur.execute(
                "ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS scheduled_name TEXT"
            )
        if "start_time" not in existing:
            cur.execute(
                "ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS start_time TIMESTAMPTZ"
            )
        if "profile_name" not in existing:
            cur.execute(
                "ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS profile_name TEXT"
            )
        if "sent_clientbot" not in existing:
            cur.execute(
                "ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS sent_clientbot BOOLEAN DEFAULT FALSE"
            )
        if "sent_strava" not in existing:
            cur.execute(
                "ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS sent_strava BOOLEAN DEFAULT FALSE"
            )
        if "sent_intervals" not in existing:
            cur.execute(
                "ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS sent_intervals BOOLEAN DEFAULT FALSE"
            )
        if "distance" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS distance DOUBLE PRECISION")
        if "elapsed_time" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS elapsed_time INTEGER")
        if "elevation_gain" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS elevation_gain DOUBLE PRECISION")
        if "average_power" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS average_power DOUBLE PRECISION")
        if "average_cadence" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS average_cadence DOUBLE PRECISION")
        if "average_heartrate" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS average_heartrate DOUBLE PRECISION")
        if "fit_path" not in existing:
            cur.execute("ALTER TABLE seen_activity_ids ADD COLUMN IF NOT EXISTS fit_path TEXT")
        conn.commit()


def record_seen_activity_id(
    account_id: str,
    activity_id: str,
    *,
    client_id: Optional[int] = None,
    scheduled_name: Optional[str] = None,
    start_time: Optional[datetime] = None,
    profile_name: Optional[str] = None,
    sent_clientbot: bool = False,
    sent_strava: bool = False,
    sent_intervals: bool = False,
    distance: Optional[float] = None,
    elapsed_time: Optional[int] = None,
    elevation_gain: Optional[float] = None,
    average_power: Optional[float] = None,
    average_cadence: Optional[float] = None,
    average_heartrate: Optional[float] = None,
    fit_path: Optional[str] = None,
) -> bool:
    """Record that an activity ID has been seen for an account, with optional ownership metadata."""
    ensure_activity_ids_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        try:
            cur.execute(
                """
                INSERT INTO seen_activity_ids (
                    account_id,
                    activity_id,
                    client_id,
                    scheduled_name,
                    start_time,
                    profile_name,
                    sent_clientbot,
                    sent_strava,
                    sent_intervals,
                    distance,
                    elapsed_time,
                    elevation_gain,
                    average_power,
                    average_cadence,
                    average_heartrate,
                    fit_path
                )
                VALUES (
                    %(account_id)s,
                    %(activity_id)s,
                    %(client_id)s,
                    %(scheduled_name)s,
                    %(start_time)s,
                    %(profile_name)s,
                    %(sent_clientbot)s,
                    %(sent_strava)s,
                    %(sent_intervals)s,
                    %(distance)s,
                    %(elapsed_time)s,
                    %(elevation_gain)s,
                    %(average_power)s,
                    %(average_cadence)s,
                    %(average_heartrate)s,
                    %(fit_path)s
                )
                ON CONFLICT (account_id, activity_id) DO UPDATE
                SET client_id = COALESCE(EXCLUDED.client_id, seen_activity_ids.client_id),
                    scheduled_name = COALESCE(EXCLUDED.scheduled_name, seen_activity_ids.scheduled_name),
                    start_time = COALESCE(EXCLUDED.start_time, seen_activity_ids.start_time),
                    profile_name = COALESCE(EXCLUDED.profile_name, seen_activity_ids.profile_name),
                    sent_clientbot = seen_activity_ids.sent_clientbot OR COALESCE(EXCLUDED.sent_clientbot, FALSE),
                    sent_strava = seen_activity_ids.sent_strava OR COALESCE(EXCLUDED.sent_strava, FALSE),
                    sent_intervals = seen_activity_ids.sent_intervals OR COALESCE(EXCLUDED.sent_intervals, FALSE),
                    distance = COALESCE(EXCLUDED.distance, seen_activity_ids.distance),
                    elapsed_time = COALESCE(EXCLUDED.elapsed_time, seen_activity_ids.elapsed_time),
                    elevation_gain = COALESCE(EXCLUDED.elevation_gain, seen_activity_ids.elevation_gain),
                    average_power = COALESCE(EXCLUDED.average_power, seen_activity_ids.average_power),
                    average_cadence = COALESCE(EXCLUDED.average_cadence, seen_activity_ids.average_cadence),
                    average_heartrate = COALESCE(EXCLUDED.average_heartrate, seen_activity_ids.average_heartrate),
                    fit_path = COALESCE(EXCLUDED.fit_path, seen_activity_ids.fit_path)
                RETURNING id
                """,
                {
                    "account_id": account_id,
                    "activity_id": activity_id,
                    "client_id": client_id,
                    "scheduled_name": scheduled_name,
                    "start_time": start_time,
                    "profile_name": profile_name,
                    "sent_clientbot": sent_clientbot,
                    "sent_strava": sent_strava,
                    "sent_intervals": sent_intervals,
                    "distance": distance,
                    "elapsed_time": elapsed_time,
                    "elevation_gain": elevation_gain,
                    "average_power": average_power,
                    "average_cadence": average_cadence,
                    "average_heartrate": average_heartrate,
                    "fit_path": fit_path,
                },
            )
            row = cur.fetchone()
            conn.commit()
            return row is not None  # True if inserted, False if conflict
        except Exception:
            LOGGER.exception("Failed to record seen activity ID for account %s, activity %s", account_id, activity_id)
            return False


def was_activity_id_seen(account_id: str, activity_id: str) -> bool:
    """Check if an activity ID has been seen for an account."""
    ensure_activity_ids_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT 1 FROM seen_activity_ids WHERE account_id = %s AND activity_id = %s",
            (account_id, activity_id)
        )
        return cur.fetchone() is not None


def get_seen_activity_ids_for_account(account_id: str, limit: int = 200) -> List[str]:
    """Get the most recent activity IDs seen for an account."""
    ensure_activity_ids_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT activity_id
            FROM seen_activity_ids
            WHERE account_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (account_id, limit)
        )
        rows = cur.fetchall()
    return [row["activity_id"] for row in rows]


def list_strava_backfill_stats(client_ids: Sequence[int] | None = None) -> List[Dict]:
    """Return aggregate stats for Strava uploads per client."""
    ensure_activity_ids_table()
    base_query = """
        SELECT
            client_id,
            COUNT(*) FILTER (WHERE fit_path IS NOT NULL) AS with_fit,
            COUNT(*) FILTER (WHERE fit_path IS NOT NULL AND sent_strava IS FALSE) AS pending,
            MAX(COALESCE(start_time, created_at)) AS last_activity_at
        FROM seen_activity_ids
        WHERE client_id IS NOT NULL
    """
    params: Dict[str, object] = {}
    if client_ids:
        base_query += " AND client_id = ANY(%(client_ids)s)"
        params["client_ids"] = list(client_ids)
    base_query += " GROUP BY client_id"

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(base_query, params)
        rows = cur.fetchall()
    results: List[Dict] = []
    for row in rows:
        last_activity_at = row.get("last_activity_at")
        results.append(
            {
                "client_id": row.get("client_id"),
                "with_fit": int(row.get("with_fit") or 0),
                "pending": int(row.get("pending") or 0),
                "last_activity_at": last_activity_at.isoformat() if hasattr(last_activity_at, "isoformat") else None,
            }
        )
    return results


def list_strava_backfill_activities(client_id: int, limit: int = 200) -> List[Dict]:
    """Return activities for a client that haven't been uploaded to Strava yet."""
    ensure_activity_ids_table()
    safe_limit = max(1, min(limit, 500))
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM seen_activity_ids
            WHERE client_id = %s
              AND sent_strava IS FALSE
              AND fit_path IS NOT NULL
            ORDER BY COALESCE(start_time, created_at) ASC
            LIMIT %s
            """,
            (client_id, safe_limit),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def list_intervals_backfill_stats(client_ids: Sequence[int] | None = None) -> List[Dict]:
    """Return aggregate stats for Intervals uploads per client."""
    ensure_activity_ids_table()
    base_query = """
        SELECT
            client_id,
            COUNT(*) FILTER (WHERE fit_path IS NOT NULL) AS with_fit,
            COUNT(*) FILTER (WHERE fit_path IS NOT NULL AND sent_intervals IS FALSE) AS pending,
            MAX(COALESCE(start_time, created_at)) AS last_activity_at
        FROM seen_activity_ids
        WHERE client_id IS NOT NULL
    """
    params: Dict[str, object] = {}
    if client_ids:
        base_query += " AND client_id = ANY(%(client_ids)s)"
        params["client_ids"] = list(client_ids)
    base_query += " GROUP BY client_id"

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(base_query, params)
        rows = cur.fetchall()
    results: List[Dict] = []
    for row in rows:
        last_activity_at = row.get("last_activity_at")
        results.append(
            {
                "client_id": row.get("client_id"),
                "with_fit": int(row.get("with_fit") or 0),
                "pending": int(row.get("pending") or 0),
                "last_activity_at": last_activity_at.isoformat() if hasattr(last_activity_at, "isoformat") else None,
            }
        )
    return results


def list_intervals_backfill_activities(client_id: int, limit: int = 200) -> List[Dict]:
    """Return activities for a client that haven't been uploaded to Intervals yet."""
    ensure_activity_ids_table()
    safe_limit = max(1, min(limit, 500))
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT *
            FROM seen_activity_ids
            WHERE client_id = %s
              AND sent_intervals IS FALSE
              AND fit_path IS NOT NULL
            ORDER BY COALESCE(start_time, created_at) ASC
            LIMIT %s
            """,
            (client_id, safe_limit),
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def find_reservation_for_activity(
    stand_ids: Sequence[int],
    target_dt: datetime,
    *,
    grace_minutes: int = 30,
) -> Optional[Dict]:
    """
    Return the reservation that matches the activity time and stand assignment.

    A small grace window before/after slot times is used to catch activities that
    start slightly earlier or end slightly later than the booked window.
    """

    if not stand_ids:
        return None

    ensure_schedule_tables()
    target_date = target_dt.date()
    grace_delta = timedelta(minutes=max(0, grace_minutes))

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                r.*,
                s.slot_date,
                s.start_time,
                s.end_time,
                s.label,
                s.session_kind,
                s.instructor_id,
                i.full_name AS instructor_name,
                t.code AS stand_code,
                t.display_name AS stand_display_name,
                t.title AS stand_title,
                c.first_name AS client_first_name,
                c.last_name AS client_last_name,
                c.full_name AS client_full_name
            FROM schedule_reservations AS r
            JOIN schedule_slots AS s ON s.id = r.slot_id
            LEFT JOIN schedule_instructors AS i ON i.id = s.instructor_id
            LEFT JOIN trainers AS t ON t.id = r.stand_id
            LEFT JOIN clients AS c ON c.id = r.client_id
            WHERE r.client_id IS NOT NULL
              AND r.status = 'booked'
              AND r.stand_id = ANY(%(stand_ids)s)
              AND s.slot_date = %(target_date)s
            ORDER BY s.start_time DESC NULLS LAST, r.id DESC
            """,
            {
                "stand_ids": list(stand_ids),
                "target_date": target_date,
            },
        )
        rows = cur.fetchall()

    if not rows:
        return None

    matched_row: Optional[Dict] = None
    best_distance = timedelta.max

    for row in rows:
        start_time: Optional[time] = row.get("start_time")
        end_time: Optional[time] = row.get("end_time")

        slot_start = datetime.combine(
            row.get("slot_date") or target_date,
            start_time or time.min,
        )
        slot_end = datetime.combine(
            row.get("slot_date") or target_date,
            end_time or time.max,
        )
        if target_dt.tzinfo:
            slot_start = slot_start.replace(tzinfo=target_dt.tzinfo)
            slot_end = slot_end.replace(tzinfo=target_dt.tzinfo)

        if slot_start - grace_delta <= target_dt <= slot_end + grace_delta:
            distance = abs(target_dt - slot_start)
            if distance < best_distance:
                best_distance = distance
                matched_row = row

    return matched_row


def find_reservation_by_client_name(
    target_dt: datetime,
    athlete_name: str,
    *,
    grace_minutes: int = 30,
    statuses: Sequence[str] | None = None,
) -> Optional[Dict]:
    """
    Find a reservation near target_dt where client name matches athlete_name.

    Used when the athlete on WattAttack differs from the scheduled stand to
    catch swaps inside the same time window.
    """

    if not athlete_name:
        return None

    def _normalize(name: Optional[str]) -> str:
        if not name:
            return ""
        return " ".join(name.lower().replace("ё", "е").split())

    def _tokens(name: Optional[str]) -> set[str]:
        if not name:
            return set()
        normalized = _normalize(name)
        return {part for part in normalized.split(" ") if part}

    ensure_schedule_tables()
    target_date = target_dt.date()
    grace_delta = timedelta(minutes=max(0, grace_minutes))
    athlete_lower = _normalize(athlete_name)
    athlete_tokens = _tokens(athlete_name)
    status_filter = list(statuses) if statuses else ["booked"]

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                r.*,
                s.slot_date,
                s.start_time,
                s.end_time,
                s.label,
                s.session_kind,
                t.code AS stand_code,
                t.display_name AS stand_display_name,
                t.title AS stand_title,
                c.first_name AS client_first_name,
                c.last_name AS client_last_name,
                c.full_name AS client_full_name
            FROM schedule_reservations AS r
            JOIN schedule_slots AS s ON s.id = r.slot_id
            LEFT JOIN trainers AS t ON t.id = r.stand_id
            LEFT JOIN clients AS c ON c.id = r.client_id
            WHERE r.client_id IS NOT NULL
              AND (%(statuses)s IS NULL OR r.status = ANY(%(statuses)s))
              AND s.slot_date = %(target_date)s
            """,
            {"target_date": target_date, "statuses": status_filter},
        )
        rows = cur.fetchall()

    if not rows:
        return None

    matched_row: Optional[Dict] = None
    best_distance = timedelta.max

    for row in rows:
        start_time: Optional[time] = row.get("start_time")
        end_time: Optional[time] = row.get("end_time")

        slot_start = datetime.combine(
            row.get("slot_date") or target_date,
            start_time or time.min,
        )
        slot_end = datetime.combine(
            row.get("slot_date") or target_date,
            end_time or time.max,
        )
        if target_dt.tzinfo:
            slot_start = slot_start.replace(tzinfo=target_dt.tzinfo)
            slot_end = slot_end.replace(tzinfo=target_dt.tzinfo)

        if not (slot_start - grace_delta <= target_dt <= slot_end + grace_delta):
            continue

        candidate_names = [
            row.get("client_full_name"),
            " ".join(
                part
                for part in [row.get("client_first_name"), row.get("client_last_name")]
                if part
            ).strip(),
            (row.get("client_first_name") or "").strip(),
            (row.get("client_last_name") or "").strip(),
        ]
        match_found = False
        for name in candidate_names:
            if not name:
                continue
            normalized = _normalize(name)
            if normalized == athlete_lower:
                match_found = True
                break
            candidate_tokens = _tokens(name)
            if candidate_tokens and athlete_tokens and candidate_tokens == athlete_tokens:
                match_found = True
                break
            if athlete_tokens and candidate_tokens and athlete_tokens.issubset(candidate_tokens):
                match_found = True
                break

        if match_found:
            distance = abs(target_dt - slot_start)
            if distance < best_distance:
                best_distance = distance
                matched_row = row

    return matched_row


def delete_activity_id(account_id: str, activity_id: str) -> bool:
    """Delete a specific activity ID for an account."""
    ensure_activity_ids_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        try:
            cur.execute(
                """
                DELETE FROM seen_activity_ids
                WHERE account_id = %s AND activity_id = %s
                """,
                (account_id, activity_id)
            )
            conn.commit()
            return cur.rowcount > 0  # True if at least one row was deleted
        except Exception:
            LOGGER.exception("Failed to delete activity ID %s for account %s", activity_id, account_id)
            return False


def list_all_accounts() -> List[str]:
    """Get a list of all accounts that have activity IDs."""
    ensure_activity_ids_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT account_id
            FROM seen_activity_ids
            ORDER BY account_id
            """
        )
        rows = cur.fetchall()
    return [row["account_id"] for row in rows]


def get_distance_leaderboard(
    limit: int = 100,
    *,
    sort_by: str = "distance",
    direction: str = "desc",
) -> Dict[str, object]:
    """Return clients ordered by aggregated ride stats for public leaderboard."""

    ensure_activity_ids_table()
    safe_limit = max(1, min(limit, 500))

    order_field_map = {
        "distance": "total_distance",
        "elevation": "total_elevation",
    }
    order_column = order_field_map.get(sort_by.lower(), "total_distance")
    order_dir_display = "ASC" if str(direction).lower() == "asc" else "DESC"

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT
                sai.client_id,
                COALESCE(
                    NULLIF(TRIM(c.full_name), ''),
                    CONCAT_WS(' ', NULLIF(TRIM(c.first_name), ''), NULLIF(TRIM(c.last_name), '')),
                    'Без имени'
                ) AS client_name,
                COUNT(*) AS rides_total,
                COUNT(*) FILTER (WHERE sai.distance IS NOT NULL) AS rides_with_distance,
                COALESCE(SUM(sai.distance), 0) AS total_distance,
                COALESCE(SUM(sai.elevation_gain), 0) AS total_elevation,
                MAX(COALESCE(sai.start_time, sai.created_at)) AS last_activity_at
            FROM seen_activity_ids AS sai
            JOIN clients AS c ON c.id = sai.client_id
            GROUP BY sai.client_id, c.full_name, c.first_name, c.last_name
            HAVING COALESCE(SUM(sai.distance), 0) > 0
            ORDER BY {order_column} DESC, rides_with_distance DESC, client_name ASC
            LIMIT %s
            """,
            (safe_limit,),
        )
        rows = cur.fetchall()

        cur.execute(
            """
            SELECT
                COUNT(DISTINCT client_id) FILTER (WHERE client_id IS NOT NULL) AS athletes,
                COALESCE(SUM(distance), 0) AS total_distance,
                COALESCE(SUM(elevation_gain), 0) AS total_elevation,
                COUNT(*) FILTER (WHERE distance IS NOT NULL) AS rides_with_distance
            FROM seen_activity_ids
            WHERE client_id IS NOT NULL
            """
        )
        totals_row = cur.fetchone() or {}

    leaderboard: List[Dict[str, object]] = []
    for row in rows:
        last_activity = row.get("last_activity_at")
        distance_meters = float(row.get("total_distance") or 0)
        distance_km = distance_meters / 1000.0
        elevation_m = float(row.get("total_elevation") or 0)

        leaderboard.append(
            {
                "client_id": row.get("client_id"),
                "name": (row.get("client_name") or "").strip() or "Без имени",
                "rides": int(row.get("rides_total") or 0),
                "rides_with_distance": int(row.get("rides_with_distance") or 0),
                "distance_meters": distance_meters,
                "distance_km": distance_km,
                "elevation_m": elevation_m,
                "last_activity_at": last_activity.isoformat() if hasattr(last_activity, "isoformat") else None,
            }
        )

    # Rank always computed in DESC order for consistency
    base_sorted = sorted(
        leaderboard,
        key=lambda item: (
            item.get("distance_meters", 0.0) if order_column == "total_distance" else item.get("elevation_m", 0.0),
            item.get("elevation_m", 0.0),
            (item.get("name") or "").lower(),
        ),
        reverse=True,
    )
    for idx, item in enumerate(base_sorted, start=1):
        item["rank"] = idx
    leaderboard = base_sorted if order_dir_display == "DESC" else list(reversed(base_sorted))

    total_distance_meters = float(totals_row.get("total_distance") or 0)
    total_elevation_m = float(totals_row.get("total_elevation") or 0)
    summary = {
        "athletes": int(totals_row.get("athletes") or 0),
        "total_distance_meters": total_distance_meters,
        "total_distance_km": total_distance_meters / 1000.0,
        "total_elevation_m": total_elevation_m,
        "rides_with_distance": int(totals_row.get("rides_with_distance") or 0),
    }

    return {"items": leaderboard, "summary": summary}


def list_upcoming_reservations(since: datetime, until: datetime) -> List[Dict]:
    """Return all upcoming reservations within a time window."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT
                r.*,
                s.slot_date,
                s.start_time,
                s.end_time,
                s.label,
                s.session_kind,
                s.instructor_id,
                i.full_name AS instructor_name,
                t.code AS stand_code,
                t.display_name AS stand_display_name,
                t.title AS stand_title,
                b.title AS bike_title,
                b.owner AS bike_owner,
                c.first_name AS client_first_name,
                c.last_name AS client_last_name,
                c.full_name AS client_full_name
            FROM schedule_reservations AS r
            JOIN schedule_slots AS s ON s.id = r.slot_id
            LEFT JOIN schedule_instructors AS i ON i.id = s.instructor_id
            LEFT JOIN trainers AS t ON t.id = r.stand_id
            LEFT JOIN bike_layout AS bl ON bl.stand_id = t.id
            LEFT JOIN bikes AS b ON b.id = bl.bike_id
            LEFT JOIN clients AS c ON c.id = r.client_id
            WHERE r.client_id IS NOT NULL
              AND r.status = 'booked'
              AND (
                    (s.slot_date > %(since_date)s)
                    OR (s.slot_date = %(since_date)s AND s.start_time >= %(since_time)s)
                  )
              AND (
                    (s.slot_date < %(until_date)s)
                    OR (s.slot_date = %(until_date)s AND s.start_time <= %(until_time)s)
                  )
            ORDER BY s.slot_date, s.start_time, r.id
            """,
            {
                "since_date": since.date(),
                "since_time": since.time(),
                "until_date": until.date(),
                "until_time": until.time(),
            },
        )
        rows = cur.fetchall()
    return rows


def record_account_assignment(reservation_id: int, account_id: str, client_id: Optional[int]) -> None:
    """Store that a WattAttack account was updated for the reservation."""

    ensure_schedule_tables()
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO schedule_account_assignments (reservation_id, account_id, client_id, applied_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (reservation_id, account_id)
            DO UPDATE SET client_id = EXCLUDED.client_id, applied_at = NOW()
            """,
            (reservation_id, account_id, client_id),
        )
        conn.commit()


def was_account_assignment_done(reservation_id: int, account_id: str) -> bool:
    """Return True if we already applied a client profile for this reservation/account."""

    ensure_schedule_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT 1
            FROM schedule_account_assignments
            WHERE reservation_id = %s AND account_id = %s
            """,
            (reservation_id, account_id),
        )
        row = cur.fetchone()
    return bool(row)
