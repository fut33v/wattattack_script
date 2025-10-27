#!/usr/bin/env python3
"""Debug script: login to WattAttack and print profile names via API and HTML."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable

from wattattack_activities import DEFAULT_BASE_URL, WattAttackClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect WattAttack profiles for all accounts")
    parser.add_argument(
        "--accounts",
        type=Path,
        default=Path("accounts.json"),
        help="Path to accounts JSON (same format as accounts.sample.json)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--dump-html-dir",
        type=Path,
        help="Optional directory to store raw /cabinet HTML per account",
    )
    parser.add_argument(
        "--filter",
        nargs="*",
        help="Process only these account IDs",
    )
    return parser.parse_args()


def pretty(value):
    return json.dumps(value, ensure_ascii=False, indent=2)


def load_accounts(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Accounts file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    accounts: Dict[str, Dict[str, str]] = {}
    for entry in data:
        identifier = entry["id"]
        accounts[identifier] = {
            "email": entry["email"],
            "password": entry["password"],
            "base_url": entry.get("base_url", DEFAULT_BASE_URL),
            "name": entry.get("name", identifier),
        }
    return accounts


def maybe_dump_html(client: WattAttackClient, base_url: str, destination: Path, account_id: str) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    html = client.session.get(f"{base_url}/cabinet").text
    (destination / f"{account_id}.html").write_text(html, encoding="utf-8")


def process_account(
    account_id: str,
    account: Dict[str, str],
    *,
    timeout: float,
    dump_dir: Path | None,
) -> None:
    base_url = account.get("base_url", DEFAULT_BASE_URL)
    client = WattAttackClient(base_url)
    client.login(account["email"], account["password"], timeout=timeout)

    try:
        profile_api = client.fetch_profile(timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        profile_api = {"error": str(exc)}

    profile_html = {}
    if dump_dir:
        maybe_dump_html(client, base_url, dump_dir, account_id)
        print(f"\n/cabinet HTML saved to {dump_dir / (account_id + '.html')}")

    print(f"===== {account_id} ({account.get('name', account_id)}) =====")
    print("/api/v1/athlete:")
    print(pretty(profile_api))
    if profile_html:
        print("\n/cabinet HTML saved; inspect manually")
    print()


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    accounts = load_accounts(args.accounts)

    selected = set(args.filter) if args.filter else set(accounts)

    dump_dir = args.dump_html_dir
    if dump_dir:
        dump_dir.mkdir(parents=True, exist_ok=True)

    for account_id, account in accounts.items():
        if selected and account_id not in selected:
            continue
        process_account(account_id, account, timeout=args.timeout, dump_dir=dump_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
