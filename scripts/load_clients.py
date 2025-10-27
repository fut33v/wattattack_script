#!/usr/bin/env python3
"""Import client records from CSV into PostgreSQL."""
from __future__ import annotations

import argparse
import csv
from io import StringIO
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from repositories.db_utils import db_connection, dict_cursor

DEFAULT_CSV = Path("data")

CSV_HEADERS = {
    "Отметка времени": "submitted_at",
    "Имя": "first_name",
    "Фамилия": "last_name",
    "Ваш вес": "weight",
    "Ваш рост": "height",
    "Ваш FTP, если знаете": "ftp",
    "Педали": "pedals",
    "Ваша цель": "goal",
    "ПОЛ": "gender",
    "высота седла": "saddle_height",
    "любимый велосипед": "favorite_bike",
}


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load client data into PostgreSQL")
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=None,
        help="Path to CSV file (defaults to first CSV in ./data)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate table before loading",
    )
    return parser.parse_args(argv)


def resolve_csv(path_arg: str | None) -> Path:
    if path_arg:
        path = Path(path_arg)
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    data_dir = DEFAULT_CSV
    if not data_dir.exists():
        raise FileNotFoundError("data directory not found")
    candidates = sorted(data_dir.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError("no CSV files found in data directory")
    return candidates[0]


def create_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                submitted_at TIMESTAMP,
                first_name TEXT,
                last_name TEXT,
                full_name TEXT UNIQUE,
                gender TEXT,
                weight NUMERIC,
                height NUMERIC,
                ftp NUMERIC,
                pedals TEXT,
                goal TEXT,
                saddle_height TEXT,
                favorite_bike TEXT
            )
            """
        )
        cur.execute("ALTER TABLE clients ADD COLUMN IF NOT EXISTS gender TEXT")
        conn.commit()


def truncate_table() -> None:
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute("TRUNCATE TABLE clients")
        conn.commit()


def normalise_float(value: str) -> float | None:
    value = (value or "").strip().replace(",", ".")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def normalise_gender(value: str) -> str | None:
    value = (value or "").strip().lower()
    if not value:
        return None
    if value in {"м", "муж", "мужской", "male"}:
        return "male"
    if value in {"ж", "жен", "женский", "female"}:
        return "female"
    return value


def _map_rows(reader: csv.DictReader) -> List[dict]:
    rows: List[dict] = []
    for raw in reader:
        mapped: dict = {}
        for src, target in CSV_HEADERS.items():
            mapped[target] = raw.get(src, "")
        rows.append(mapped)
    return rows


def load_rows(rows: List[dict]) -> tuple[int, int]:
    inserted = 0
    updated = 0

    with db_connection() as conn, dict_cursor(conn) as cur:
        for row in rows:
            submitted_at = row.get("submitted_at") or None
            if submitted_at:
                try:
                    submitted_dt = datetime.strptime(submitted_at, "%d.%m.%Y %H:%M:%S")
                except ValueError:
                    submitted_dt = None
            else:
                submitted_dt = None

            first_name = (row.get("first_name") or "").strip()
            last_name = (row.get("last_name") or "").strip()
            if first_name or last_name:
                full_name = (first_name + " " + last_name).strip()
            else:
                full_name = None
            weight = normalise_float(row.get("weight"))
            height = normalise_float(row.get("height"))
            ftp = normalise_float(row.get("ftp"))
            gender = normalise_gender(row.get("gender"))
            pedals = (row.get("pedals") or "").strip() or None
            goal = (row.get("goal") or "").strip() or None
            saddle_height = (row.get("saddle_height") or "").strip() or None
            favorite_bike = (row.get("favorite_bike") or "").strip() or None

            if not full_name:
                # Skip entries without at least a combined name to keep uniqueness meaningful
                continue

            cur.execute(
                """
                INSERT INTO clients (submitted_at, first_name, last_name, full_name, gender, weight, height, ftp, pedals, goal, saddle_height, favorite_bike)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (full_name) DO UPDATE
                SET submitted_at = EXCLUDED.submitted_at,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    gender = EXCLUDED.gender,
                    weight = EXCLUDED.weight,
                    height = EXCLUDED.height,
                    ftp = EXCLUDED.ftp,
                    pedals = EXCLUDED.pedals,
                    goal = EXCLUDED.goal,
                    saddle_height = EXCLUDED.saddle_height,
                    favorite_bike = EXCLUDED.favorite_bike
                """,
                (
                    submitted_dt,
                    first_name or None,
                    last_name or None,
                    full_name,
                    gender,
                    weight,
                    height,
                    ftp,
                    pedals,
                    goal,
                    saddle_height,
                    favorite_bike,
                ),
            )
            if cur.statusmessage.startswith("INSERT"):
                inserted += 1
            else:
                updated += 1
        conn.commit()

    return inserted, updated


def load_data(csv_path: Path) -> tuple[int, int]:
    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        rows = _map_rows(reader)
    return load_rows(rows)


def load_clients_from_csv_bytes(data: bytes, truncate: bool = False) -> tuple[int, int]:
    create_table()
    if truncate:
        truncate_table()

    text = data.decode("utf-8-sig")
    reader = csv.DictReader(StringIO(text))
    rows = _map_rows(reader)
    return load_rows(rows)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    csv_path = resolve_csv(args.csv_path)
    create_table()
    if args.truncate:
        truncate_table()
    inserted, updated = load_data(csv_path)
    print(f"Imported clients from {csv_path}: inserted={inserted}, updated={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
