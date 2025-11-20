#!/usr/bin/env python3
"""CLI helper to create WattAttack cabinet events."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient

EVENT_TYPES = ["group_ride", "group_race", "ttt_race", "itt_race"]
MEASURE_TYPES = ["distance", "duration", "laps"]


@dataclass(frozen=True)
class Account:
    identifier: str
    email: str
    password: str
    base_url: str


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create WattAttack race/event via the hidden cabinet API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--accounts-file",
        type=Path,
        default=Path("accounts.json"),
        help="Path to WattAttack accounts JSON",
    )
    parser.add_argument(
        "--account-id",
        required=True,
        help="Account identifier from accounts.json",
    )
    parser.add_argument(
        "--list-routes",
        action="store_true",
        help="List available routes and exit",
    )
    parser.add_argument("--name", help="Event title (ru)")
    parser.add_argument("--description", help="Event description (ru)")
    parser.add_argument(
        "--route-id",
        type=int,
        help="Route ID from /api/v1/map/get-with-routes",
    )
    parser.add_argument(
        "--event-type",
        choices=EVENT_TYPES,
        default="group_ride",
        help="Event type",
    )
    parser.add_argument(
        "--measure-type",
        choices=MEASURE_TYPES,
        default="distance",
        help="Event measure type",
    )
    parser.add_argument(
        "--distance-km",
        type=float,
        help="Distance in km when measure type is distance",
    )
    parser.add_argument(
        "--duration-minutes",
        type=int,
        help="Duration in minutes when measure type is duration",
    )
    parser.add_argument(
        "--laps-count",
        type=int,
        help="Lap count when measure type is laps",
    )
    parser.add_argument(
        "--start",
        help="Start date/time (e.g. 2025-02-01T19:00 or 2025-02-01T19:00+03:00)",
    )
    parser.add_argument(
        "--late-join-at",
        help="Optional late join cutoff (ISO timestamp)",
    )
    parser.add_argument(
        "--late-join-minutes",
        type=int,
        help="Allow late join N minutes after start",
    )
    parser.add_argument(
        "--leader-magnet",
        action="store_true",
        help="Enable leader magnet",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="Run event in isolation mode",
    )
    parser.add_argument(
        "--show-results",
        action="store_true",
        help="Publish results",
    )
    parser.add_argument(
        "--disable-drafting",
        action="store_true",
        help="Disable drafting (drafting is enabled by default)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds",
    )
    return parser.parse_args(argv)


def load_accounts(path: Path) -> Dict[str, Account]:
    if not path.exists():
        raise FileNotFoundError(f"Accounts file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    accounts: Dict[str, Account] = {}
    for entry in raw:
        identifier = entry["id"]
        accounts[identifier] = Account(
            identifier=identifier,
            email=entry["email"],
            password=entry["password"],
            base_url=entry.get("base_url", DEFAULT_BASE_URL),
        )
    return accounts


def parse_iso_datetime(value: str, tz_fallback) -> datetime:
    candidate = value
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    dt = datetime.fromisoformat(candidate)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz_fallback)
    return dt.astimezone(timezone.utc)


def to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def print_routes(routes: List[Dict[str, object]]) -> None:
    for item in routes:
        name = item.get("nameRu") or item.get("nameEn") or item.get("publicId")
        print(f"Map {item.get('id')} – {name}")
        for route in item.get("routes") or []:
            distance_km = (route.get("distance") or 0) / 1000
            route_name = route.get("nameRu") or route.get("nameEn")
            route_id = route.get("id")
            if isinstance(route_id, int):
                route_id_text = f"{route_id:>4}"
            else:
                route_id_text = str(route_id)
            print(
                f"  route_id={route_id_text}  "
                f"{route_name}  "
                f"{distance_km:.1f} km"
            )
        print()


def find_route(routes: List[Dict[str, object]], route_id: int) -> Optional[Tuple[Dict[str, object], Dict[str, object]]]:
    for item in routes:
        for route in item.get("routes") or []:
            if route.get("id") == route_id:
                return route, item
    return None


def build_payload(args: argparse.Namespace, routes: List[Dict[str, object]]) -> Dict[str, object]:
    missing: List[str] = []
    if not args.name:
        missing.append("--name")
    if not args.description:
        missing.append("--description")
    if args.route_id is None:
        missing.append("--route-id")
    if not args.start:
        missing.append("--start")
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing required arguments: {joined}")

    tz_fallback = datetime.now().astimezone().tzinfo or timezone.utc
    start_dt = parse_iso_datetime(args.start, tz_fallback)
    late_join_deadline: Optional[datetime] = None
    if args.late_join_at:
        late_join_deadline = parse_iso_datetime(args.late_join_at, tz_fallback)
    elif args.late_join_minutes is not None:
        if args.late_join_minutes <= 0:
            raise ValueError("--late-join-minutes must be positive")
        late_join_deadline = start_dt + timedelta(minutes=args.late_join_minutes)

    if late_join_deadline and late_join_deadline <= start_dt:
        raise ValueError("late join cutoff must be after the start time")

    measure = args.measure_type
    payload: Dict[str, object] = {
        "nameRu": args.name,
        "descriptionRu": args.description,
        "routeId": args.route_id,
        "eventType": args.event_type,
        "startDate": to_iso_z(start_dt),
        "isLateJoin": bool(late_join_deadline),
        "isLeaderMagnet": args.leader_magnet,
        "isIsolated": args.isolated,
        "isShowResults": args.show_results,
        "isDraftingEnabled": not args.disable_drafting,
        "eventMeasureType": measure,
    }
    if late_join_deadline:
        payload["lateJoinDeadline"] = to_iso_z(late_join_deadline)

    if measure == "distance":
        distance = args.distance_km
        if distance is None:
            distance = 10.0
        payload["distanceKm"] = distance
    elif measure == "duration":
        duration = args.duration_minutes
        if duration is None:
            raise ValueError("--duration-minutes is required for duration events")
        payload["durationMinutes"] = duration
    elif measure == "laps":
        laps = args.laps_count
        if laps is None:
            raise ValueError("--laps-count is required for laps events")
        payload["lapsCount"] = laps

    route_info = find_route(routes, args.route_id)
    if not route_info:
        raise ValueError(f"Route id {args.route_id} was not found in /map/get-with-routes response")

    return payload


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    accounts = load_accounts(args.accounts_file)
    if args.account_id not in accounts:
        known = ", ".join(sorted(accounts))
        raise SystemExit(f"Unknown account id {args.account_id}. Known ids: {known}")
    account = accounts[args.account_id]
    client = WattAttackClient(account.base_url)
    client.login(account.email, account.password, timeout=args.timeout)

    routes = client.fetch_maps_with_routes(timeout=args.timeout)
    if args.list_routes:
        print_routes(routes)
        return 0

    try:
        payload = build_payload(args, routes)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        result = client.create_event(payload, timeout=args.timeout)
    except RuntimeError as exc:
        raise SystemExit(f"Event creation failed: {exc}") from exc

    event = result.get("event") if isinstance(result, dict) else None
    if isinstance(event, dict):
        print("Event created:")
        print(f"  id={event.get('id')} publicId={event.get('publicId')}")
        print(f"  startDate={event.get('startDate')} routeId={event.get('routeId')}")
    else:
        print("Event creation response:")
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
