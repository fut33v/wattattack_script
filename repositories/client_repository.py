"""Access helper for the clients database table."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from .db_utils import db_connection, dict_cursor

EDITABLE_FIELDS = {
    "weight": "weight",
    "ftp": "ftp",
    "favorite_bike": "favorite_bike",
    "pedals": "pedals",
}

def list_clients(limit: int, offset: int = 0) -> List[Dict]:
    query = (
        "SELECT id, first_name, last_name, full_name, gender, weight, height, ftp, pedals, goal, saddle_height, favorite_bike, submitted_at "
        "FROM clients ORDER BY COALESCE(last_name, full_name), COALESCE(first_name, ''), COALESCE(full_name, '') "
        "LIMIT %s OFFSET %s"
    )
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, (limit, offset))
        rows = cur.fetchall()
    return rows


def get_client(client_id: int) -> Optional[Dict]:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT id, first_name, last_name, full_name, gender, weight, height, ftp, pedals, goal, saddle_height, favorite_bike, submitted_at FROM clients WHERE id = %s",
            (client_id,),
        )
        row = cur.fetchone()
    return row


def count_clients() -> int:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM clients")
        row = cur.fetchone()
    return int(row.get("cnt", 0)) if row else 0


def get_clients_stats() -> Dict[str, Optional[float]]:
    query = """
        SELECT
            COUNT(*) AS total,
            MIN(height) AS min_height,
            MAX(height) AS max_height,
            MIN(ftp) AS min_ftp,
            MAX(ftp) AS max_ftp
        FROM clients
    """
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query)
        row = cur.fetchone() or {}

    def _to_float(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    total_value = row.get("total")
    total = int(total_value) if total_value is not None else 0

    return {
        "total": total,
        "min_height": _to_float(row.get("min_height")),
        "max_height": _to_float(row.get("max_height")),
        "min_ftp": _to_float(row.get("min_ftp")),
        "max_ftp": _to_float(row.get("max_ftp")),
    }


def search_clients(term: str, limit: int = 20) -> List[Dict]:
    query = (
        "SELECT id, first_name, last_name, full_name, gender, weight, height, ftp, pedals, goal, saddle_height, favorite_bike, submitted_at "
        "FROM clients WHERE COALESCE(first_name, '') ILIKE %s OR COALESCE(last_name, '') ILIKE %s OR COALESCE(full_name, '') ILIKE %s "
        "ORDER BY COALESCE(last_name, full_name), COALESCE(first_name, ''), COALESCE(full_name, '') LIMIT %s"
    )
    pattern = f"%{term}%"
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, (pattern, pattern, pattern, limit))
        rows = cur.fetchall()
    return rows


def update_client_fields(client_id: int, **fields: object) -> bool:
    updates = {EDITABLE_FIELDS[key]: value for key, value in fields.items() if key in EDITABLE_FIELDS}
    if not updates:
        return False

    assignments = ", ".join(f"{column} = %s" for column in updates)
    values = list(updates.values())
    values.append(client_id)

    query = f"UPDATE clients SET {assignments} WHERE id = %s"
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
        conn.commit()
    return True


def create_client(
    *,
    first_name: Optional[str],
    last_name: Optional[str],
    weight: Optional[float] = None,
    height: Optional[float] = None,
    gender: Optional[str] = None,
    ftp: Optional[float] = None,
    pedals: Optional[str] = None,
    goal: Optional[str] = None,
) -> Dict:
    first_name_clean = (first_name or "").strip() or None
    last_name_clean = (last_name or "").strip() or None

    full_name_parts = [part for part in [first_name_clean, last_name_clean] if part]
    full_name = " ".join(full_name_parts).strip()
    if not full_name:
        raise ValueError("first_name or last_name must be provided")

    pedals_clean = (pedals or "").strip() or None
    goal_clean = (goal or "").strip() or None
    gender_clean = (gender or "").strip() or None

    submitted_at = datetime.utcnow()

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO clients (
                submitted_at,
                first_name,
                last_name,
                full_name,
                gender,
                weight,
                height,
                ftp,
                pedals,
                goal
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, first_name, last_name, full_name, gender, weight, height, ftp, pedals, goal, submitted_at
            """,
            (
                submitted_at,
                first_name_clean,
                last_name_clean,
                full_name,
                gender_clean,
                weight,
                height,
                ftp,
                pedals_clean,
                goal_clean,
            ),
        )
        record = cur.fetchone()
        conn.commit()
    return record
