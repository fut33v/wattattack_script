#!/usr/bin/env python3
"""Test script to demonstrate activity tracking functionality."""

import logging
from repositories.schedule_repository import (
    ensure_activity_ids_table,
    record_seen_activity_id,
    was_activity_id_seen,
    get_seen_activity_ids_for_account,
)

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def test_activity_tracking():
    """Test the activity tracking functionality."""
    LOGGER.info("Testing activity tracking functionality")
    
    # Ensure the database table exists
    ensure_activity_ids_table()
    LOGGER.info("Database table ensured")
    
    # Test account and activities
    account_id = "test_account_1"
    activity_ids = ["activity_1", "activity_2", "activity_3"]
    
    # Record some activities
    LOGGER.info("Recording activities...")
    for activity_id in activity_ids:
        result = record_seen_activity_id(account_id, activity_id)
        LOGGER.info(f"Recorded activity {activity_id}: {result}")
    
    # Try to record a duplicate
    result = record_seen_activity_id(account_id, "activity_1")
    LOGGER.info(f"Recording duplicate activity activity_1: {result}")
    
    # Check if activities were seen
    LOGGER.info("Checking if activities were seen...")
    for activity_id in activity_ids:
        result = was_activity_id_seen(account_id, activity_id)
        LOGGER.info(f"Activity {activity_id} seen: {result}")
    
    # Get all seen activities for the account
    seen_activities = get_seen_activity_ids_for_account(account_id)
    LOGGER.info(f"All seen activities for {account_id}: {seen_activities}")
    
    LOGGER.info("Test completed successfully!")


if __name__ == "__main__":
    test_activity_tracking()