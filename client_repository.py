"""Access helper for the clients database table."""
from __future__ import annotations

from typing import Dict, List, Optional

from db_utils import db_connection, dict_cursor


def list_clients(limit: int, offset: int = 0) -> List[Dict]:
    query = (
        "SELECT id, first_name, last_name, full_name, gender, weight, height, ftp, pedals, goal, saddle_height, favorite_bike "
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
            "SELECT id, first_name, last_name, full_name, gender, weight, height, ftp, pedals, goal, saddle_height, favorite_bike FROM clients WHERE id = %s",
            (client_id,),
        )
        row = cur.fetchone()
    return row


def count_clients() -> int:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS cnt FROM clients")
        row = cur.fetchone()
    return int(row.get("cnt", 0)) if row else 0
