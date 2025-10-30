"""Manage associations between Telegram users and client records."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db_utils import db_connection, dict_cursor


def ensure_client_links_table() -> None:
    """Create the client_links table if it does not exist."""
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS client_links (
                client_id INTEGER PRIMARY KEY REFERENCES clients(id) ON DELETE CASCADE,
                tg_user_id BIGINT NOT NULL,
                tg_username TEXT,
                tg_full_name TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "ALTER TABLE client_links ADD COLUMN IF NOT EXISTS tg_username TEXT"
        )
        cur.execute(
            "ALTER TABLE client_links ADD COLUMN IF NOT EXISTS tg_full_name TEXT"
        )
        cur.execute(
            "ALTER TABLE client_links ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()"
        )
        cur.execute(
            "ALTER TABLE client_links ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()"
        )
        cur.execute(
            "ALTER TABLE client_links ALTER COLUMN updated_at SET DEFAULT NOW()"
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS client_links_tg_user_idx ON client_links (tg_user_id)"
        )
        conn.commit()


def get_link_by_user(tg_user_id: int) -> Optional[Dict]:
    """Return the linked client record for a Telegram user, if any."""
    ensure_client_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT client_id, tg_user_id, tg_username, tg_full_name, created_at, updated_at "
            "FROM client_links WHERE tg_user_id = %s",
            (tg_user_id,),
        )
        record = cur.fetchone()
    return record


def get_link_by_client(client_id: int) -> Optional[Dict]:
    """Return the Telegram user linked to a client, if any."""
    ensure_client_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            "SELECT client_id, tg_user_id, tg_username, tg_full_name, created_at, updated_at "
            "FROM client_links WHERE client_id = %s",
            (client_id,),
        )
        record = cur.fetchone()
    return record


def link_user_to_client(
    *,
    tg_user_id: int,
    client_id: int,
    tg_username: Optional[str],
    tg_full_name: Optional[str],
) -> Dict:
    """Associate a Telegram user with a client record."""
    ensure_client_links_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("DELETE FROM client_links WHERE tg_user_id = %s", (tg_user_id,))
        cur.execute(
            """
            INSERT INTO client_links (client_id, tg_user_id, tg_username, tg_full_name, created_at, updated_at)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (client_id) DO UPDATE
            SET tg_user_id = EXCLUDED.tg_user_id,
                tg_username = EXCLUDED.tg_username,
                tg_full_name = EXCLUDED.tg_full_name,
                updated_at = NOW()
            RETURNING client_id, tg_user_id, tg_username, tg_full_name, created_at, updated_at
            """,
            (client_id, tg_user_id, tg_username, tg_full_name),
        )
        record = cur.fetchone()
        conn.commit()
    return record


def list_links(limit: int | None = None, offset: int = 0) -> List[Dict]:
    """Return all client links, optionally paginated."""

    ensure_client_links_table()
    query = (
        "SELECT client_id, tg_user_id, tg_username, tg_full_name, created_at, updated_at "
        "FROM client_links ORDER BY created_at DESC"
    )
    params: tuple = ()
    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params = (limit, offset)

    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return rows


def remove_link(*, client_id: Optional[int] = None, tg_user_id: Optional[int] = None) -> bool:
    """Delete a link either by client_id or tg_user_id."""

    ensure_client_links_table()
    if client_id is None and tg_user_id is None:
        return False

    with db_connection() as conn, dict_cursor(conn) as cur:
        if client_id is not None:
            cur.execute("DELETE FROM client_links WHERE client_id = %s", (client_id,))
        else:
            cur.execute("DELETE FROM client_links WHERE tg_user_id = %s", (tg_user_id,))
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted
