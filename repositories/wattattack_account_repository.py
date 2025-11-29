from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from .db_utils import db_connection, dict_cursor

LOGGER = logging.getLogger(__name__)


def ensure_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wattattack_accounts (
                id TEXT PRIMARY KEY,
                name TEXT,
                email TEXT NOT NULL,
                password TEXT NOT NULL,
                base_url TEXT,
                stand_ids INTEGER[],
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        conn.commit()


def list_accounts() -> List[Dict]:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, name, email, password, base_url, stand_ids, created_at, updated_at
            FROM wattattack_accounts
            ORDER BY id
            """
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_account(account_id: str) -> Optional[Dict]:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT id, name, email, password, base_url, stand_ids, created_at, updated_at
            FROM wattattack_accounts
            WHERE id = %s
            """,
            (account_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def upsert_account(
    *,
    account_id: str,
    name: Optional[str],
    email: str,
    password: str,
    base_url: Optional[str],
    stand_ids: Optional[List[int]],
) -> Dict:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO wattattack_accounts (id, name, email, password, base_url, stand_ids)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET name = EXCLUDED.name,
                email = EXCLUDED.email,
                password = EXCLUDED.password,
                base_url = EXCLUDED.base_url,
                stand_ids = EXCLUDED.stand_ids,
                updated_at = NOW()
            RETURNING id, name, email, password, base_url, stand_ids, created_at, updated_at
            """,
            (account_id, name, email, password, base_url, stand_ids),
        )
        row = cur.fetchone()
        conn.commit()
    return dict(row)


def delete_account(account_id: str) -> bool:
    ensure_table()
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("DELETE FROM wattattack_accounts WHERE id = %s", (account_id,))
        deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def import_from_json(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("Failed to parse accounts JSON from %s", path)
        return 0

    imported = 0
    if not isinstance(data, list):
        return 0
    for entry in data:
        if not isinstance(entry, dict) or "id" not in entry:
            continue
        account_id = str(entry["id"])
        name = entry.get("name")
        email = entry.get("email")
        password = entry.get("password")
        base_url = entry.get("base_url")
        stand_raw = entry.get("stand_ids") or entry.get("stand_id")
        stand_ids: List[int] = []
        if stand_raw is not None:
            iterable = stand_raw if isinstance(stand_raw, (list, tuple)) else [stand_raw]
            for value in iterable:
                try:
                    stand_ids.append(int(value))
                except (TypeError, ValueError):
                    LOGGER.warning("Invalid stand id %r for account %s", value, account_id)
        if not email or not password:
            LOGGER.warning("Skipping account %s: missing email/password", account_id)
            continue
        upsert_account(
            account_id=account_id,
            name=name or account_id,
            email=email,
            password=password,
            base_url=base_url,
            stand_ids=stand_ids or None,
        )
        imported += 1
    return imported
