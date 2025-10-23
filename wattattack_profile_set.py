#!/usr/bin/env python3
"""Update WattAttack profile fields for accounts defined in accounts.json."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update WattAttack profile fields for one or more accounts",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "account_ids",
        nargs="+",
        help="Account identifiers as defined in accounts.json",
    )
    parser.add_argument(
        "--accounts-file",
        type=Path,
        default=Path("accounts.json"),
        help="Path to accounts JSON file",
    )
    parser.add_argument("--first-name")
    parser.add_argument("--last-name")
    parser.add_argument("--gender", help="M or F")
    parser.add_argument("--weight", type=float, help="Weight in kg")
    parser.add_argument("--ftp", type=int, help="FTP in Watts")
    parser.add_argument(
        "--birth-date",
        dest="birth_date",
        help="Birth date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--height",
        type=float,
        help="Height in centimeters",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Fetch and display profile info before and after the update",
    )
    return parser.parse_args(argv)


def load_accounts(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Accounts file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    accounts: Dict[str, Dict[str, Any]] = {}
    for entry in data:
        identifier = entry["id"]
        accounts[identifier] = {
            "email": entry["email"],
            "password": entry["password"],
            "base_url": entry.get("base_url", DEFAULT_BASE_URL),
            "name": entry.get("name", identifier),
        }
    return accounts


def show_profile(label: str, profile: Dict[str, Any]) -> None:
    user = profile.get("athlete") or profile.get("user") or profile
    first = user.get("firstName") if isinstance(user, dict) else None
    last = user.get("lastName") if isinstance(user, dict) else None
    gender = user.get("gender") if isinstance(user, dict) else None
    weight = user.get("weight") if isinstance(user, dict) else None
    ftp = user.get("ftp") if isinstance(user, dict) else None
    birth_date = user.get("birthDate") if isinstance(user, dict) else None
    height = user.get("height") if isinstance(user, dict) else None
    print(
        f"{label}: firstName={first!r} lastName={last!r} gender={gender!r} "
        f"weight={weight!r} ftp={ftp!r} birthDate={birth_date!r} height={height!r}"
    )


def build_payload(args: argparse.Namespace) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if args.first_name is not None:
        payload["firstName"] = args.first_name
    if args.last_name is not None:
        payload["lastName"] = args.last_name
    if args.gender is not None:
        gender = args.gender.strip().upper()
        if gender and gender[0] in {"M", "F"}:
            payload["gender"] = "male" if gender[0] == "M" else "female"
        else:
            raise ValueError("Gender must start with 'M' or 'F'")
    if args.weight is not None:
        payload["weight"] = args.weight
    if args.ftp is not None:
        payload["ftp"] = args.ftp
    if args.birth_date is not None:
        payload["birthDate"] = args.birth_date
    if args.height is not None:
        payload["height"] = args.height
    return payload


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_payload(args)
    if not payload:
        print("Nothing to update: specify at least one field (use --help for options).")
        return 1

    accounts = load_accounts(args.accounts_file)
    missing: List[str] = [acc for acc in args.account_ids if acc not in accounts]
    if missing:
        print(f"Unknown account id(s): {', '.join(missing)}")
        return 1

    for account_id in args.account_ids:
        account = accounts[account_id]
        print(f"Updating {account_id} ({account.get('name', account_id)})")
        client = WattAttackClient(account.get("base_url", DEFAULT_BASE_URL))
        client.login(account["email"], account["password"], timeout=args.timeout)

        if args.show:
            before = client.fetch_profile(timeout=args.timeout)
            show_profile("Before", before)

        athletic_payload: Dict[str, Any] = {}
        user_payload: Dict[str, Any] = {}

        for key in list(payload.keys()):
            if key in {"firstName", "lastName"}:
                user_payload[key] = payload.pop(key)

        athletic_payload = payload

        if user_payload:
            user_response = client.update_user(user_payload, timeout=args.timeout)
            print("User update response:", user_response)

        if athletic_payload:
            profile_response = client.update_profile(athletic_payload, timeout=args.timeout)
            print("Athlete update response:", profile_response)

        if args.show:
            after = client.fetch_profile(timeout=args.timeout)
            show_profile("After", after)

        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
