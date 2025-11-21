"""Helpers for managing race registrations."""
from __future__ import annotations

import json
import re
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

from psycopg2.extras import Json

from .db_utils import db_connection, dict_cursor

RACE_STATUS_PENDING = "pending"
RACE_STATUS_APPROVED = "approved"
RACE_STATUS_REJECTED = "rejected"


def ensure_tables() -> None:
    """Create race tables if they are missing."""

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS races (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                race_date DATE NOT NULL,
                price_rub INTEGER NOT NULL,
                sbp_phone TEXT NOT NULL,
                payment_instructions TEXT,
                clusters JSONB NOT NULL DEFAULT '[]'::jsonb,
                notes TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute("ALTER TABLE races ADD COLUMN IF NOT EXISTS notes TEXT")
        cur.execute("ALTER TABLE races ADD COLUMN IF NOT EXISTS payment_instructions TEXT")
        cur.execute("ALTER TABLE races ADD COLUMN IF NOT EXISTS clusters JSONB NOT NULL DEFAULT '[]'::jsonb")
        cur.execute("ALTER TABLE races ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE")
        cur.execute("ALTER TABLE races ADD COLUMN IF NOT EXISTS slug TEXT")
        cur.execute("ALTER TABLE races ADD COLUMN IF NOT EXISTS description TEXT")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS races_slug_idx ON races (slug) WHERE slug IS NOT NULL"
        )
        cur.execute("SELECT id, title, slug FROM races WHERE slug IS NULL OR slug = ''")
        missing = cur.fetchall() or []
        for row in missing:
            base_slug = _normalize_race_slug(row.get("slug"), title=row.get("title"))
            allocated = _allocate_unique_slug(cur, base_slug, exclude_id=row["id"])
            cur.execute("UPDATE races SET slug = %s WHERE id = %s", (allocated, row["id"]))

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS race_registrations (
                id SERIAL PRIMARY KEY,
                race_id INTEGER NOT NULL REFERENCES races(id) ON DELETE CASCADE,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                tg_user_id BIGINT,
                tg_username TEXT,
                tg_full_name TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                cluster_code TEXT,
                cluster_label TEXT,
                payment_proof_file_id TEXT,
                payment_proof_unique_id TEXT,
                payment_proof_message_id BIGINT,
                payment_submitted_at TIMESTAMPTZ,
                notes TEXT,
                bring_own_bike BOOLEAN,
                axle_type TEXT,
                gears_label TEXT,
                race_mode TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS race_registrations_unique_client
            ON race_registrations (race_id, client_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS race_registrations_status_idx
            ON race_registrations (race_id, status)
            """
        )
        cur.execute("ALTER TABLE race_registrations ADD COLUMN IF NOT EXISTS bring_own_bike BOOLEAN")
        cur.execute("ALTER TABLE race_registrations ADD COLUMN IF NOT EXISTS axle_type TEXT")
        cur.execute("ALTER TABLE race_registrations ADD COLUMN IF NOT EXISTS gears_label TEXT")
        cur.execute("ALTER TABLE race_registrations ADD COLUMN IF NOT EXISTS race_mode TEXT")
        cur.execute("ALTER TABLE race_registrations ALTER COLUMN tg_user_id DROP NOT NULL")
        conn.commit()


def _normalize_clusters(raw_clusters: Optional[Sequence[Any]]) -> List[Dict[str, str]]:
    result: List[Dict[str, str]] = []
    if not raw_clusters:
        return result
    seen_codes: set[str] = set()

    def _normalize_cluster_time(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, time):
            return value.strftime("%H:%M")
        if isinstance(value, str):
            token = value.strip()
        else:
            token = str(value).strip()
        if not token:
            return None
        for fmt in ("%H:%M", "%H:%M:%S"):
            try:
                parsed = datetime.strptime(token, fmt)
                return parsed.strftime("%H:%M")
            except ValueError:
                continue
        return None

    for idx, entry in enumerate(raw_clusters):
        if isinstance(entry, str):
            label = entry.strip()
            extra = {}
        elif isinstance(entry, dict):
            label = str(entry.get("label") or entry.get("title") or "").strip()
            extra = dict(entry)
        else:
            continue
        if not label:
            continue
        code = str(extra.get("code") or extra.get("key") or _slugify(label))
        if not code:
            code = f"cluster-{idx+1}"
        base_code = code
        suffix = 1
        while code in seen_codes:
            suffix += 1
            code = f"{base_code}-{suffix}"
        seen_codes.add(code)
        normalized: Dict[str, str] = {"code": code, "label": label}
        start_time = _normalize_cluster_time(extra.get("start_time")) or _normalize_cluster_time(extra.get("start"))
        end_time = _normalize_cluster_time(extra.get("end_time")) or _normalize_cluster_time(extra.get("end"))
        if start_time:
            normalized["start_time"] = start_time
        if end_time:
            normalized["end_time"] = end_time
        result.append(normalized)
    return result


def _slugify(value: str, *, default: str = "cluster") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or default


def _allocate_unique_slug(cur, base_slug: str, *, exclude_id: Optional[int] = None) -> str:
    candidate = base_slug
    suffix = 1
    while True:
        if exclude_id is None:
            cur.execute("SELECT 1 FROM races WHERE slug = %s LIMIT 1", (candidate,))
        else:
            cur.execute("SELECT 1 FROM races WHERE slug = %s AND id <> %s LIMIT 1", (candidate, exclude_id))
        if not cur.fetchone():
            return candidate
        suffix += 1
        candidate = f"{base_slug}-{suffix}"


def _normalize_race_slug(slug_value: Optional[str], *, title: Optional[str] = None) -> str:
    candidate = (slug_value or "").strip()
    if not candidate and title:
        candidate = title
    normalized = _slugify(candidate, default="")
    if not normalized:
        normalized = f"race-{uuid4().hex[:8]}"
    return normalized


def _canonical_slug(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = _slugify(str(value), default="")
    return normalized or None


def _decode_clusters(value: Optional[Any]) -> List[Dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, list):
        decoded: List[Dict[str, str]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            entry: Dict[str, str] = {
                "code": str(item.get("code") or item.get("key") or ""),
                "label": str(item.get("label") or ""),
            }
            start_time = item.get("start_time") or item.get("start")
            end_time = item.get("end_time") or item.get("end")
            if start_time:
                entry["start_time"] = str(start_time).strip()
            if end_time:
                entry["end_time"] = str(end_time).strip()
            decoded.append(entry)
        return decoded
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return _decode_clusters(parsed)
    return []


def list_races() -> List[Dict]:
    ensure_tables()
    query = """
        SELECT
            r.*,
            COALESCE(SUM(CASE WHEN reg.status = %s THEN 1 ELSE 0 END), 0) AS pending_count,
            COALESCE(SUM(CASE WHEN reg.status = %s THEN 1 ELSE 0 END), 0) AS approved_count
        FROM races r
        LEFT JOIN race_registrations reg ON reg.race_id = r.id
        GROUP BY r.id
        ORDER BY r.race_date DESC, r.id DESC
    """
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, (RACE_STATUS_PENDING, RACE_STATUS_APPROVED))
        rows = cur.fetchall()
    for row in rows:
        row["clusters"] = _decode_clusters(row.get("clusters"))
    return rows


def create_race(
    *,
    title: str,
    race_date: date,
    price_rub: int,
    sbp_phone: str,
    payment_instructions: Optional[str],
    clusters: Optional[Sequence[Any]] = None,
    notes: Optional[str] = None,
    description: Optional[str] = None,
    is_active: bool = True,
    slug: Optional[str] = None,
) -> Dict:
    ensure_tables()
    clusters_payload = _normalize_clusters(clusters)
    base_slug = _normalize_race_slug(slug, title=title)
    with db_connection() as conn, dict_cursor(conn) as cur:
        final_slug = _allocate_unique_slug(cur, base_slug)
        cur.execute(
            """
            INSERT INTO races (title, race_date, price_rub, sbp_phone, payment_instructions, clusters, notes, description, is_active, slug)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                title,
                race_date,
                price_rub,
                sbp_phone,
                payment_instructions,
                Json(clusters_payload),
                notes,
                description,
                is_active,
                final_slug,
            ),
        )
        record = cur.fetchone()
        conn.commit()
    if record:
        record["clusters"] = clusters_payload
    return record


def get_race(race_id: int) -> Optional[Dict]:
    ensure_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM races WHERE id = %s", (race_id,))
        record = cur.fetchone()
        if record:
            record["clusters"] = _decode_clusters(record.get("clusters"))
    return record


def get_race_by_slug(slug: str) -> Optional[Dict]:
    ensure_tables()
    normalized = _canonical_slug(slug)
    if not normalized:
        return None
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT * FROM races WHERE slug = %s", (normalized,))
        record = cur.fetchone()
        if record:
            record["clusters"] = _decode_clusters(record.get("clusters"))
    return record


def get_active_race(only_future: bool = True) -> Optional[Dict]:
    ensure_tables()
    query = "SELECT * FROM races WHERE is_active = TRUE"
    params: List[Any] = []
    if only_future:
        query += " AND race_date >= CURRENT_DATE"
    query += " ORDER BY race_date ASC, id ASC LIMIT 1"
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        record = cur.fetchone()
        if record:
            record["clusters"] = _decode_clusters(record.get("clusters"))
    return record


def update_race(race_id: int, **fields: Any) -> Optional[Dict]:
    ensure_tables()
    if not fields:
        return get_race(race_id)

    assignments: List[str] = []
    values: List[Any] = []
    slug_override: Optional[str] = None

    for key, value in fields.items():
        if key == "clusters":
            normalized = _normalize_clusters(value if isinstance(value, Sequence) else [])
            assignments.append("clusters = %s")
            values.append(Json(normalized))
        elif key == "slug":
            slug_override = _normalize_race_slug(value)
        elif key == "description":
            assignments.append("description = %s")
            values.append(value)
        else:
            assignments.append(f"{key} = %s")
            values.append(value)

    with db_connection() as conn, dict_cursor(conn) as cur:
        if slug_override is not None:
            unique_slug = _allocate_unique_slug(cur, slug_override, exclude_id=race_id)
            assignments.append("slug = %s")
            values.append(unique_slug)

        if not assignments:
            return get_race(race_id)

        assignments.append("updated_at = NOW()")
        query = f"UPDATE races SET {', '.join(assignments)} WHERE id = %s RETURNING *"
        values.append(race_id)
        cur.execute(query, values)
        record = cur.fetchone()
        conn.commit()

    if record:
        record["clusters"] = _decode_clusters(record.get("clusters"))
    return record


def list_registrations(race_id: int) -> List[Dict]:
    ensure_tables()
    query = """
        SELECT
            reg.*,
            c.first_name,
            c.last_name,
            c.full_name
        FROM race_registrations reg
        LEFT JOIN clients c ON c.id = reg.client_id
        WHERE reg.race_id = %s
        ORDER BY reg.created_at DESC
    """
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, (race_id,))
        rows = cur.fetchall()
    for row in rows:
        row["client_name"] = _format_client_name(row)
    return rows


def get_registration_by_id(registration_id: int) -> Optional[Dict]:
    ensure_tables()
    query = """
        SELECT
            reg.*,
            c.first_name,
            c.last_name,
            c.full_name
        FROM race_registrations reg
        LEFT JOIN clients c ON c.id = reg.client_id
        WHERE reg.id = %s
    """
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, (registration_id,))
        record = cur.fetchone()
    if record:
        record["client_name"] = _format_client_name(record)
    return record


def get_registration(race_id: int, client_id: int) -> Optional[Dict]:
    ensure_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT * FROM race_registrations WHERE race_id = %s AND client_id = %s",
            (race_id, client_id),
        )
        record = cur.fetchone()
    return record


def upsert_registration(
    *,
    race_id: int,
    client_id: int,
    tg_user_id: Optional[int],
    tg_username: Optional[str],
    tg_full_name: Optional[str],
) -> Dict:
    ensure_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO race_registrations (race_id, client_id, tg_user_id, tg_username, tg_full_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (race_id, client_id) DO UPDATE SET
                tg_user_id = EXCLUDED.tg_user_id,
                tg_username = EXCLUDED.tg_username,
                tg_full_name = EXCLUDED.tg_full_name,
                updated_at = NOW()
            RETURNING *
            """,
            (race_id, client_id, tg_user_id, tg_username, tg_full_name),
        )
        record = cur.fetchone()
        conn.commit()
    return record


def save_payment_proof(
    *,
    registration_id: int,
    file_id: str,
    file_unique_id: Optional[str],
    message_id: Optional[int],
) -> Optional[Dict]:
    ensure_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            UPDATE race_registrations
            SET
                payment_proof_file_id = %s,
                payment_proof_unique_id = %s,
                payment_proof_message_id = %s,
                payment_submitted_at = NOW(),
                status = %s,
                updated_at = NOW()
            WHERE id = %s
            RETURNING *
            """,
            (
                file_id,
                file_unique_id,
                message_id,
                RACE_STATUS_PENDING,
                registration_id,
            ),
        )
        record = cur.fetchone()
        conn.commit()
    return record


def update_registration(
    registration_id: int,
    *,
    status: Optional[str] = None,
    cluster_code: Optional[str] = None,
    cluster_label: Optional[str] = None,
    notes: Optional[str] = None,
    bring_own_bike: Optional[bool] = None,
    axle_type: Optional[str] = None,
    gears_label: Optional[str] = None,
    race_mode: Optional[str] = None,
) -> Optional[Dict]:
    ensure_tables()
    updates: List[str] = []
    values: List[Any] = []
    if status is not None:
        updates.append("status = %s")
        values.append(status)
    if cluster_code is not None or cluster_label is not None:
        updates.append("cluster_code = %s")
        updates.append("cluster_label = %s")
        values.append(cluster_code)
        values.append(cluster_label)
    if notes is not None:
        updates.append("notes = %s")
        values.append(notes)
    if bring_own_bike is not None:
        updates.append("bring_own_bike = %s")
        values.append(bring_own_bike)
    if axle_type is not None:
        updates.append("axle_type = %s")
        values.append(axle_type)
    if gears_label is not None:
        updates.append("gears_label = %s")
        values.append(gears_label)
    if race_mode is not None:
        updates.append("race_mode = %s")
        values.append(race_mode)
    if not updates:
        return None
    updates.append("updated_at = NOW()")
    query = f"UPDATE race_registrations SET {', '.join(updates)} WHERE id = %s RETURNING *"
    values.append(registration_id)
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, values)
        record = cur.fetchone()
        conn.commit()
    return record


def delete_registration(race_id: int, registration_id: int) -> bool:
    ensure_tables()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "DELETE FROM race_registrations WHERE id = %s AND race_id = %s",
            (registration_id, race_id),
        )
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def _format_client_name(row: Dict[str, Any]) -> str:
    first = (row.get("first_name") or "").strip()
    last = (row.get("last_name") or "").strip()
    full = (row.get("full_name") or "").strip()
    if first and last:
        return f"{first} {last}".strip()
    if full:
        return full
    return last or first or "Клиент"
