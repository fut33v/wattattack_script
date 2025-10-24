"""Access helper for the clients database table."""
from __future__ import annotations

from typing import Dict, List, Optional

from db_utils import db_connection, dict_cursor

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
