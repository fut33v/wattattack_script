#!/usr/bin/env python3
"""Import trainer (станки) inventory from CSV into PostgreSQL."""
from __future__ import annotations

import argparse
import csv
from io import StringIO
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, TypedDict

from trainers_repository import ensure_trainers_table, truncate_trainers, upsert_trainers

DEFAULT_DATA_DIR = Path("data")


class TrainerRow(TypedDict, total=False):
    position: Optional[int]
    code: str
    title: Optional[str]
    display_name: Optional[str]
    owner: Optional[str]
    axle_types: Optional[str]
    cassette: Optional[str]
    notes: Optional[str]


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load trainer data into PostgreSQL")
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=None,
        help="Path to CSV file (defaults to first *станк*.csv in ./data)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate trainers table before loading",
    )
    return parser.parse_args(argv)


def resolve_csv(path_arg: str | None) -> Path:
    if path_arg:
        path = Path(path_arg)
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    if not DEFAULT_DATA_DIR.exists():
        raise FileNotFoundError("data directory not found")

    candidates = sorted(
        file for file in DEFAULT_DATA_DIR.glob("*.csv") if "станк" in file.name.lower()
    )
    if not candidates:
        candidates = sorted(DEFAULT_DATA_DIR.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError("no CSV files found in data directory")
    return candidates[0]


def _extract_position(code: str) -> Optional[int]:
    digits = "".join(ch for ch in code if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _map_row(row: List[str]) -> Optional[TrainerRow]:
    if not row:
        return None

    while row and not row[-1]:
        row.pop()

    if len(row) < 2:
        return None

    code = (row[0] or "").strip()
    if not code:
        return None

    title = (row[1] or "").strip() if len(row) > 1 else ""
    display_name_raw = (row[2] or "").strip() if len(row) > 2 else ""
    display_name = display_name_raw or code
    owner = (row[3] or "").strip() if len(row) > 3 else ""
    axle_types = (row[4] or "").strip() if len(row) > 4 else ""
    cassette = (row[5] or "").strip() if len(row) > 5 else ""
    notes = (row[6] or "").strip() if len(row) > 6 else ""

    data: TrainerRow = {
        "code": code,
        "display_name": display_name or None,
        "title": title or None,
        "display_name": display_name or None,
        "owner": owner or None,
        "axle_types": axle_types or None,
        "cassette": cassette or None,
        "notes": notes or None,
        "position": _extract_position(code),
    }
    return data


def load_rows(csv_path: Path) -> List[TrainerRow]:
    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        rows: List[TrainerRow] = []
        for raw in reader:
            mapped = _map_row(raw)
            if mapped:
                rows.append(mapped)
    return rows


def load_trainers(csv_path: Path, truncate: bool = False) -> Tuple[int, int]:
    ensure_trainers_table()
    if truncate:
        truncate_trainers()
    rows = load_rows(csv_path)
    return upsert_trainers(rows)


def load_trainers_from_csv_bytes(data: bytes, truncate: bool = False) -> Tuple[int, int]:
    ensure_trainers_table()
    if truncate:
        truncate_trainers()

    reader = csv.reader(StringIO(data.decode("utf-8-sig")))
    next(reader, None)
    rows: List[TrainerRow] = []
    for raw in reader:
        mapped = _map_row(raw)
        if mapped:
            rows.append(mapped)
    return upsert_trainers(rows)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    csv_path = resolve_csv(args.csv_path)
    inserted, updated = load_trainers(csv_path, truncate=args.truncate)
    print(f"Imported trainers from {csv_path}: inserted={inserted}, updated={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
