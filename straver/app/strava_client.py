"""Lightweight Strava API client used by Straver."""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

log = logging.getLogger(__name__)

STRAVA_BASE_URL = "https://www.strava.com/api/v3"


class StravaClient:
    """Client for interacting with Strava."""

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        access_token: Optional[str] = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.access_token = access_token
        self.session = requests.Session()
        if access_token:
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    def get_authorization_url(self, state: str = "") -> str:
        """Generate the Strava authorization URL."""
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": "activity:write",
            "state": state,
        }
        return f"https://www.strava.com/oauth/authorize?{urlencode(params)}"

    def exchange_code_for_token(self, code: str) -> Dict[str, Any]:
        """Exchange authorization code for access and refresh tokens."""
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": code,
            "grant_type": "authorization_code",
        }
        response = requests.post("https://www.strava.com/oauth/token", data=data)
        response.raise_for_status()
        return response.json()

    def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh an expired access token."""
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
        response = requests.post("https://www.strava.com/oauth/token", data=data)
        response.raise_for_status()
        return response.json()

    def upload_activity(
        self,
        file_obj,
        *,
        filename: str = "activity.fit",
        name: str = "WattAttack Activity",
        description: str = "",
    ) -> Dict[str, Any]:
        """Upload a FIT file to Strava."""
        if not self.access_token:
            raise ValueError("Access token is required for uploading activities")
        files = {"file": (filename, file_obj, "application/octet-stream")}
        data = {
            "name": name,
            "description": description,
            "data_type": "fit",
            "private": 0,
        }
        response = self.session.post(f"{STRAVA_BASE_URL}/uploads", files=files, data=data)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def is_token_expired(expires_at_timestamp: int, *, skew_seconds: int = 300) -> bool:
        """Check if the access token is expired."""
        return time.time() > expires_at_timestamp - skew_seconds
