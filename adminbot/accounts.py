"""Shared helpers for WattAttack account configuration."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from wattattack_activities import DEFAULT_BASE_URL


@dataclass(frozen=True)
class AccountConfig:
    identifier: str
    name: str
    email: str
    password: str
    base_url: str = DEFAULT_BASE_URL


def load_accounts(config_path: Path) -> Dict[str, AccountConfig]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Accounts config file not found: {config_path}. "
            "Create it from the sample template."
        )

    raw_data = json.loads(config_path.read_text(encoding="utf-8"))
    accounts: Dict[str, AccountConfig] = {}

    for entry in raw_data:
        identifier = entry["id"]
        accounts[identifier] = AccountConfig(
            identifier=identifier,
            name=entry.get("name", identifier),
            email=entry["email"],
            password=entry["password"],
            base_url=entry.get("base_url", DEFAULT_BASE_URL),
        )

    if not accounts:
        raise ValueError("Accounts list is empty")

    return accounts


def normalize_account_id(value: str) -> str:
    return value.replace("_", "").lower()


def resolve_account_identifier(
    registry: Mapping[str, AccountConfig], raw_id: str
) -> Optional[str]:
    if raw_id in registry:
        return raw_id
    target = normalize_account_id(raw_id)
    for account_id in registry:
        if normalize_account_id(account_id) == target:
            return account_id
    return None


def resolve_account_tokens(
    registry: Mapping[str, AccountConfig], tokens: Iterable[str]
) -> Tuple[List[str], List[str]]:
    tokens = list(tokens)
    if not tokens:
        return [], []
    lowered = [token.lower() for token in tokens]
    if len(tokens) == 1 and lowered[0] in {"all", "*", "any"}:
        return list(registry.keys()), []

    resolved: List[str] = []
    missing: List[str] = []
    for token in tokens:
        account_id = resolve_account_identifier(registry, token)
        if account_id is None:
            missing.append(token)
        elif account_id not in resolved:
            resolved.append(account_id)
    return resolved, missing


def format_account_list(registry: Mapping[str, AccountConfig]) -> str:
    lines: List[str] = []
    for key in sorted(registry):
        alias = normalize_account_id(key)
        account_name = registry[key].name
        lines.append(f"{alias} ({key}) — {account_name}")
    return "\n".join(lines)

