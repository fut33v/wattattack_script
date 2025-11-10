"""Strava API client for uploading FIT files."""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

LOGGER = logging.getLogger(__name__)

# Strava API configuration
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
STRAVA_REDIRECT_URI = os.environ.get("STRAVA_REDIRECT_URI", "https://krutilkaweb.ru.tuna.am/strava/callback")
STRAVA_BASE_URL = "https://www.strava.com/api/v3"


class StravaClient:
    """Client for interacting with the Strava API."""

    def __init__(self, access_token: Optional[str] = None):
        self.access_token = access_token
        self.session = requests.Session()
        if access_token:
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    def get_authorization_url(self, state: str = "") -> str:
        """Generate the Strava authorization URL."""
        params = {
            "client_id": STRAVA_CLIENT_ID,
            "redirect_uri": STRAVA_REDIRECT_URI,
            "response_type": "code",
            "scope": "activity:write",
            "state": state,
        }
        return f"https://www.strava.com/oauth/authorize?{urlencode(params)}"

    def exchange_code_for_token(self, code: str) -> Dict[str, Any]:
        """Exchange authorization code for access and refresh tokens."""
        if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
            raise ValueError("STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET must be set")

        data = {
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
        }

        response = requests.post("https://www.strava.com/oauth/token", data=data)
        response.raise_for_status()
        return response.json()

    def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh an expired access token."""
        if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
            raise ValueError("STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET must be set")

        data = {
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }

        response = requests.post("https://www.strava.com/oauth/token", data=data)
        response.raise_for_status()
        return response.json()

    def upload_activity(
        self, file_path: str, name: str = "WattAttack Activity", description: str = ""
    ) -> Dict[str, Any]:
        """Upload a FIT file to Strava."""
        if not self.access_token:
            raise ValueError("Access token is required for uploading activities")

        with open(file_path, "rb") as f:
            files = {"file": f}
            data = {
                "name": name,
                "description": description,
                "data_type": "fit",
                "private": 0,  # Public activity
            }
            response = self.session.post(
                f"{STRAVA_BASE_URL}/uploads", files=files, data=data
            )
            response.raise_for_status()
            return response.json()

    def get_athlete(self) -> Dict[str, Any]:
        """Get the authenticated athlete's information."""
        if not self.access_token:
            raise ValueError("Access token is required for getting athlete info")

        response = self.session.get(f"{STRAVA_BASE_URL}/athlete")
        response.raise_for_status()
        return response.json()

    def is_token_expired(self, expires_at: int) -> bool:
        """Check if the access token is expired."""
        return time.time() > expires_at


def refresh_strava_token_if_needed(link_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Refresh Strava token if it's expired and update the database record."""
    from repositories.client_link_repository import update_strava_tokens
    
    strava_refresh_token = link_record.get("strava_refresh_token")
    strava_token_expires_at = link_record.get("strava_token_expires_at")
    
    if not strava_refresh_token or not strava_token_expires_at:
        return None
    
    # Convert timestamp to int for comparison
    try:
        # Handle different timestamp formats
        if isinstance(strava_token_expires_at, (int, float)):
            expires_at = int(strava_token_expires_at)
        elif isinstance(strava_token_expires_at, str):
            # Try to parse as integer timestamp first
            try:
                expires_at = int(strava_token_expires_at)
            except ValueError:
                # If that fails, try to parse as datetime string
                from datetime import datetime
                dt = datetime.fromisoformat(strava_token_expires_at.replace(' ', 'T'))
                expires_at = int(dt.timestamp())
        elif hasattr(strava_token_expires_at, 'timestamp'):
            # Handle datetime objects
            expires_at = int(strava_token_expires_at.timestamp())
        else:
            LOGGER.warning("Invalid expires_at value: %s", strava_token_expires_at)
            return None
    except (ValueError, TypeError):
        LOGGER.warning("Invalid expires_at value: %s", strava_token_expires_at)
        return None
    
    # Check if token is expired (with 5 minute buffer)
    if time.time() > expires_at - 300:
        try:
            client = StravaClient()
            token_data = client.refresh_access_token(strava_refresh_token)
            
            # Update the database with new tokens
            # The refresh endpoint might not return athlete info, so preserve existing athlete_id
            strava_athlete_id = link_record.get("strava_athlete_id")
            if "athlete" in token_data and "id" in token_data["athlete"]:
                strava_athlete_id = token_data["athlete"]["id"]
            
            updated_link = update_strava_tokens(
                tg_user_id=link_record["tg_user_id"],
                strava_access_token=token_data["access_token"],
                strava_refresh_token=token_data["refresh_token"],
                strava_token_expires_at=str(token_data["expires_at"]),
                strava_athlete_id=strava_athlete_id
            )
            
            return updated_link
        except Exception as e:
            LOGGER.error("Failed to refresh Strava token for user %s: %s", link_record["tg_user_id"], e)
            return None
    
    return link_record
