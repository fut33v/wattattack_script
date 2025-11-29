from __future__ import annotations

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from wattattack_activities import DEFAULT_BASE_URL
from repositories import wattattack_account_repository

LOGGER = logging.getLogger(__name__)

ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")


def _parse_stand_ids(entry: Dict[str, Any]) -> List[int]:
    raw_value = entry.get("stand_ids")
    if raw_value is None and entry.get("stand_id") is not None:
        raw_value = [entry.get("stand_id")]
    if raw_value is None:
        return []
    iterable = raw_value if isinstance(raw_value, (list, tuple)) else [raw_value]
    values: List[int] = []
    for value in iterable:
        try:
            values.append(int(value))
        except (TypeError, ValueError):
            LOGGER.warning("Account %s has invalid stand id %r", entry.get("id"), value)
    return values


def _load_from_json(config_path: Path) -> Dict[str, Dict[str, Any]]:
    data = json.loads(config_path.read_text(encoding="utf-8"))
    accounts: Dict[str, Dict[str, Any]] = {}
    for entry in data:
        identifier = entry["id"]
        accounts[identifier] = {
            "id": identifier,
            "name": entry.get("name", identifier),
            "email": entry["email"],
            "password": entry["password"],
            "base_url": entry.get("base_url", DEFAULT_BASE_URL),
            "stand_ids": _parse_stand_ids(entry),
        }
    return accounts


def _db_accounts_dict() -> Dict[str, Dict[str, Any]]:
    records = wattattack_account_repository.list_accounts()
    result: Dict[str, Dict[str, Any]] = {}
    for row in records:
        identifier = row.get("id")
        if not identifier:
            continue
        result[str(identifier)] = {
            "id": identifier,
            "name": row.get("name") or str(identifier),
            "email": row.get("email"),
            "password": row.get("password"),
            "base_url": row.get("base_url") or DEFAULT_BASE_URL,
            "stand_ids": row.get("stand_ids") or [],
        }
    return result


def load_accounts(config_path: Path | None = None) -> Dict[str, Dict[str, Any]]:
    if config_path is None:
        config_path = Path(os.environ.get(ACCOUNTS_ENV, DEFAULT_ACCOUNTS_PATH))

    db_accounts = _db_accounts_dict()
    if db_accounts:
        return db_accounts

    if not config_path.exists():
        raise FileNotFoundError(
            f"Accounts config file not found: {config_path}. "
            "Supply --accounts or set WATTATTACK_ACCOUNTS_FILE."
        )

    json_accounts = _load_from_json(config_path)
    if not json_accounts:
        raise ValueError("Accounts list is empty")

    # Persist JSON accounts into DB for future runs
    for acc in json_accounts.values():
        try:
            wattattack_account_repository.upsert_account(
                account_id=acc["id"],
                name=acc.get("name"),
                email=acc["email"],
                password=acc["password"],
                base_url=acc.get("base_url"),
                stand_ids=acc.get("stand_ids") or None,
            )
        except Exception:  # noqa: BLE001
            LOGGER.exception("Failed to import account %s into database", acc["id"])

    return json_accounts
