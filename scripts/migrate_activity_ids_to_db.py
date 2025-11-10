#!/usr/bin/env python3
"""Migrate activity IDs from JSON state file to database."""

import argparse
import json
import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any

# Add the parent directory to the path so we can import repositories
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from repositories.schedule_repository import (
    ensure_activity_ids_table,
    record_seen_activity_id,
)

LOGGER = logging.getLogger(__name__)


def load_state(path: Path) -> Dict[str, Any]:
    """Load state from JSON file."""
    if path.is_dir():
        raise IsADirectoryError(f"State path points to a directory: {path}")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        LOGGER.warning("State file is corrupted, starting fresh: %s", path)
        return {}


def migrate_activity_ids(state_file: Path) -> int:
    """Migrate activity IDs from JSON state file to database."""
    # Ensure the database table exists
    ensure_activity_ids_table()
    
    # Load state from JSON file
    state = load_state(state_file)
    if not state or "accounts" not in state:
        LOGGER.info("No activity IDs found in state file")
        return 0
    
    # Count migrated IDs
    migrated_count = 0
    
    # Migrate activity IDs for each account
    for account_id, account_data in state["accounts"].items():
        known_ids = account_data.get("known_ids", [])
        LOGGER.info("Migrating %d activity IDs for account %s", len(known_ids), account_id)
        
        for activity_id in known_ids:
            if record_seen_activity_id(account_id, str(activity_id)):
                migrated_count += 1
    
    LOGGER.info("Migrated %d activity IDs to database", migrated_count)
    return migrated_count


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    
    parser = argparse.ArgumentParser(description="Migrate activity IDs from JSON state file to database")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("notifier_state.json"),
        help="Path to JSON state file (default: notifier_state.json)"
    )
    
    args = parser.parse_args()
    
    try:
        count = migrate_activity_ids(args.state_file)
        LOGGER.info("Successfully migrated %d activity IDs to database", count)
    except Exception as exc:
        LOGGER.error("Failed to migrate activity IDs: %s", exc)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())