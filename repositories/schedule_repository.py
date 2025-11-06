"""Helpers for managing training schedule weeks, slots, and reservations."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from .db_utils import db_connection, dict_cursor
from . import trainers_repository, instructors_repository


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
        if column == "week_start_date":
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
    source: str = "krutilkavnbot",
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
