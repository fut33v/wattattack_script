"""Manage associations between VK users and client records."""
from __future__ import annotations

from typing import Dict, Optional, List

from .db_utils import db_connection, dict_cursor


def ensure_vk_client_links_table() -> None:
    """Create the vk_client_links table if it does not exist."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vk_client_links (
                client_id INTEGER PRIMARY KEY REFERENCES clients(id) ON DELETE CASCADE,
                vk_user_id BIGINT NOT NULL UNIQUE,
                vk_username TEXT,
                vk_full_name TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS vk_client_links_vk_user_idx ON vk_client_links (vk_user_id)")
        conn.commit()


def get_link_by_vk_user(vk_user_id: int) -> Optional[Dict]:
    """Return the linked client record for a VK user, if any."""
    ensure_vk_client_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT client_id, vk_user_id, vk_username, vk_full_name, created_at, updated_at
            FROM vk_client_links
            WHERE vk_user_id = %s
            """,
            (vk_user_id,),
        )
        record = cur.fetchone()
    return dict(record) if record else None


def get_link_by_client(client_id: int) -> Optional[Dict]:
    """Return VK link for a given client, if exists."""
    ensure_vk_client_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT client_id, vk_user_id, vk_username, vk_full_name, created_at, updated_at
            FROM vk_client_links
            WHERE client_id = %s
            """,
            (client_id,),
        )
        record = cur.fetchone()
    return dict(record) if record else None


def link_vk_user_to_client(
    *,
    vk_user_id: int,
    client_id: int,
    vk_username: Optional[str],
    vk_full_name: Optional[str],
) -> Dict:
    """Associate a VK user with a client record."""
    ensure_vk_client_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("DELETE FROM vk_client_links WHERE vk_user_id = %s", (vk_user_id,))
        cur.execute(
            """
            INSERT INTO vk_client_links (
                client_id, vk_user_id, vk_username, vk_full_name, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (client_id) DO UPDATE
            SET vk_user_id = EXCLUDED.vk_user_id,
                vk_username = EXCLUDED.vk_username,
                vk_full_name = EXCLUDED.vk_full_name,
                updated_at = NOW()
            RETURNING client_id, vk_user_id, vk_username, vk_full_name, created_at, updated_at
            """,
            (client_id, vk_user_id, vk_username, vk_full_name),
        )
        record = cur.fetchone()
        conn.commit()
    return dict(record) if record else {}


def remove_link(*, client_id: Optional[int] = None, vk_user_id: Optional[int] = None) -> bool:
    """Delete a link either by client_id or vk_user_id."""
    ensure_vk_client_links_table()
    if client_id is None and vk_user_id is None:
        return False
    conditions = []
    params = []
    if client_id is not None:
        conditions.append("client_id = %s")
        params.append(client_id)
    if vk_user_id is not None:
        conditions.append("vk_user_id = %s")
        params.append(vk_user_id)
    where_clause = " OR ".join(conditions)
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM vk_client_links WHERE {where_clause}", tuple(params))
        conn.commit()
    return True


def list_links(limit: int | None = None, offset: int = 0) -> List[Dict]:
    """Return all VK client links, optionally paginated."""

    ensure_vk_client_links_table()
    query = (
        "SELECT v.client_id, v.vk_user_id, v.vk_username, v.vk_full_name, "
        "v.created_at, v.updated_at, "
        "COALESCE(c.full_name, CONCAT_WS(' ', c.first_name, c.last_name)) AS client_name, "
        "c.gender AS gender "
        "FROM vk_client_links v "
        "LEFT JOIN clients c ON c.id = v.client_id "
        "ORDER BY v.created_at DESC"
    )
    params: tuple = ()
    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params = (limit, offset)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return [dict(row) for row in rows]
