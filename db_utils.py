"""Database helper utilities for WattAttack scripts."""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extras import RealDictCursor


def _db_params() -> dict[str, str | int]:
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "wattattack"),
        "user": os.environ.get("DB_USER", "wattattack"),
        "password": os.environ.get("DB_PASSWORD", "wattattack"),
    }


@contextmanager
def db_connection() -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(**_db_params())
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def dict_cursor(conn: psycopg2.extensions.connection) -> Iterator[RealDictCursor]:
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cursor
    finally:
        cursor.close()
