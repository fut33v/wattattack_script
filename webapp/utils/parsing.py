"""Parsing helpers shared across routes."""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Optional

from fastapi import HTTPException, status


def parse_iso_date(field: str, value: object) -> date:
    if not value or not isinstance(value, str):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}") from exc


def parse_iso_time(field: str, value: object) -> time:
    if not value or not isinstance(value, str):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue
    raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")


def parse_cluster_time(field: str, value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, time):
        return value.strftime("%H:%M")
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
    else:
        token = str(value).strip()
        if not token:
            return None
    parsed = parse_iso_time(field, token)
    return parsed.strftime("%H:%M")


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def parse_positive_int(field: str, value: object) -> int:
    if isinstance(value, int):
        if value <= 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"{field} must be positive")
        return value
    if isinstance(value, float):
        if value <= 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"{field} must be positive")
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"{field} is required")
        try:
            parsed = int(stripped)
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}") from exc
        if parsed <= 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"{field} must be positive")
        return parsed
    raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid {field}")
