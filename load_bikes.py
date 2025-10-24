#!/usr/bin/env python3
"""Import bicycle inventory from CSV into PostgreSQL."""
from __future__ import annotations

import argparse
import csv
from io import StringIO
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, TypedDict

from bikes_repository import ensure_bikes_table, truncate_bikes, upsert_bikes

DEFAULT_DATA_DIR = Path("data")


class BikeRow(TypedDict, total=False):
    position: Optional[int]
    title: str
    owner: Optional[str]
    size_label: Optional[str]
    frame_size_cm: Optional[str]
    height_min_cm: Optional[float]
    height_max_cm: Optional[float]
    gears: Optional[str]
    axle_type: Optional[str]
    cassette: Optional[str]


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load bicycle data into PostgreSQL")
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=None,
        help="Path to CSV file (defaults to first *велос*.csv in ./data)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate bikes table before loading",
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
        file for file in DEFAULT_DATA_DIR.glob("*.csv") if "вело" in file.name.lower()
    )
    if not candidates:
        candidates = sorted(DEFAULT_DATA_DIR.glob("*.csv"))

    if not candidates:
        raise FileNotFoundError("no CSV files found in data directory")
    return candidates[0]


def _normalise_numeric(value: str) -> Optional[float]:
    value = (value or "").strip().replace(",", ".")
    if not value:
        return None
    filtered = "".join(ch for ch in value if ch.isdigit() or ch in ".-")
    if not filtered:
        return None
    try:
        return float(filtered)
    except ValueError:
        return None


def _normalise_int(value: str) -> Optional[int]:
    value = (value or "").strip()
    if not value:
        return None
    filtered = "".join(ch for ch in value if ch.isdigit())
    if not filtered:
        return None
    try:
        return int(filtered)
    except ValueError:
        return None


def _map_row(row: List[str]) -> Optional[BikeRow]:
    if not row:
        return None

    while row and not row[-1]:
        # strip trailing empty cells
        row.pop()

    if len(row) < 2:
        return None

    title = (row[1] or "").strip()
    if not title:
        return None

    owner = (row[2] or "").strip() if len(row) > 2 else ""
    size_label = (row[3] or "").strip() if len(row) > 3 else ""
    frame_size_cm = (row[4] or "").strip() if len(row) > 4 else ""
    height_min = _normalise_numeric(row[5] if len(row) > 5 else "")
    height_max = _normalise_numeric(row[6] if len(row) > 6 else "")
    gears = (row[7] or "").strip() if len(row) > 7 else ""
    axle_type = (row[8] or "").strip() if len(row) > 8 else ""
    cassette = (row[9] or "").strip() if len(row) > 9 else ""

    data: BikeRow = {
        "position": _normalise_int(row[0]) if len(row) > 0 else None,
        "title": title,
        "owner": owner or None,
        "size_label": size_label or None,
        "frame_size_cm": frame_size_cm or None,
        "height_min_cm": height_min,
        "height_max_cm": height_max,
        "gears": gears or None,
        "axle_type": axle_type or None,
        "cassette": cassette or None,
    }

    return data


def load_rows(csv_path: Path) -> List[BikeRow]:
    with csv_path.open(encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        # Skip header
        next(reader, None)
        rows: List[BikeRow] = []
        for raw in reader:
            mapped = _map_row(raw)
            if mapped:
                rows.append(mapped)
    return rows


def load_bikes(csv_path: Path, truncate: bool = False) -> Tuple[int, int]:
    ensure_bikes_table()
    if truncate:
        truncate_bikes()
    rows = load_rows(csv_path)
    return upsert_bikes(rows)


def load_bikes_from_csv_bytes(data: bytes, truncate: bool = False) -> Tuple[int, int]:
    ensure_bikes_table()
    if truncate:
        truncate_bikes()

    reader = csv.reader(StringIO(data.decode("utf-8-sig")))
    next(reader, None)
    rows: List[BikeRow] = []
    for raw in reader:
        mapped = _map_row(raw)
        if mapped:
            rows.append(mapped)
    return upsert_bikes(rows)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    csv_path = resolve_csv(args.csv_path)
    inserted, updated = load_bikes(csv_path, truncate=args.truncate)
    print(
        f"Imported bikes from {csv_path}: inserted={inserted}, updated={updated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
