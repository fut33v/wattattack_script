"""Lightweight Intervals.icu API client."""
from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import base64

import requests
from requests.auth import HTTPBasicAuth

log = logging.getLogger(__name__)


def _base_url() -> str:
    return (os.environ.get("INTERVALS_BASE_URL") or "https://intervals.icu").rstrip("/")


class IntervalsClient:
    """HTTP client for Intervals.icu using personal API key (Basic auth)."""

    def __init__(self, api_key: str, athlete_id: Optional[str] = None, timeout: float = 30.0):
        self.api_key = api_key.strip()
        self.athlete_id = str(athlete_id or "0")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth("API_KEY", self.api_key)

    def _url(self, path: str) -> str:
        return f"{_base_url()}{path}"

    def upload_activity(
        self,
        *,
        file_path: Path,
        name: Optional[str] = None,
        description: Optional[str] = None,
        device_name: Optional[str] = None,
        external_id: Optional[str] = None,
        paired_workout_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Upload a FIT/GPX/TCX activity file."""
        params: Dict[str, Any] = {}
        if name:
            params["name"] = name
        if description:
            params["description"] = description
        if device_name:
            params["device_name"] = device_name
        if external_id:
            params["external_id"] = external_id
        if paired_workout_id is not None:
            params["paired_event_id"] = paired_workout_id

        with file_path.open("rb") as fp:
            files = {"file": (file_path.name, fp, "application/octet-stream")}
            response = self.session.post(
                self._url(f"/api/v1/athlete/{self.athlete_id}/activities"),
                params=params,
                files=files,
                timeout=self.timeout,
            )
        response.raise_for_status()
        return response.json()

    def list_activities(
        self,
        *,
        oldest: str,
        newest: Optional[str] = None,
        limit: Optional[int] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch activities for a date range."""
        params: Dict[str, Any] = {"oldest": oldest}
        if newest:
            params["newest"] = newest
        if limit is not None:
            params["limit"] = limit
        if fields:
            params["fields"] = ",".join(fields)

        response = self.session.get(
            self._url(f"/api/v1/athlete/{self.athlete_id}/activities"),
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json() or []

    def get_training_plan(self) -> Dict[str, Any]:
        """Return the athlete's training plan."""
        response = self.session.get(
            self._url(f"/api/v1/athlete/{self.athlete_id}/training-plan"),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def fetch_events(
        self,
        *,
        category: str,
        oldest: str,
        newest: Optional[str] = None,
        resolve: bool = True,
    ) -> List[Dict[str, Any]]:
        """Fetch events (e.g., planned workouts) for a date range."""
        params: Dict[str, Any] = {
            "category": category,
            "oldest": oldest,
        }
        if newest:
            params["newest"] = newest
        if resolve:
            params["resolve"] = "true"

        response = self.session.get(
            self._url(f"/api/v1/athlete/{self.athlete_id}/events"),
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json() or []

    def create_workouts_bulk(self, workouts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create multiple workouts in the athlete's library."""
        response = self.session.post(
            self._url(f"/api/v1/athlete/{self.athlete_id}/workouts/bulk"),
            json=workouts,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json() or []

    def download_workout_as_zwo(self, workout: Dict[str, Any]) -> bytes:
        """Convert a workout JSON to ZWO using /download-workout.zwo."""
        response = self.session.post(
            self._url(f"/api/v1/athlete/{self.athlete_id}/download-workout.zwo"),
            json=workout,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.content

    def download_event_zwo(self, *, oldest: str, newest: Optional[str] = None) -> Optional[bytes]:
        """Fetch events with ext=zwo to get workout_file_base64."""
        params: Dict[str, Any] = {
            "category": "WORKOUT",
            "oldest": oldest,
            "ext": "zwo",
        }
        if newest:
            params["newest"] = newest
        response = self.session.get(
            self._url(f"/api/v1/athlete/{self.athlete_id}/events"),
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        events = response.json() or []
        if not events:
            return None
        first = events[0]
        encoded = first.get("workout_file_base64")
        if not encoded:
            return None
        try:
            return base64.b64decode(encoded)
        except Exception:
            return None

    def auth_header_value(self) -> str:
        """Return the Basic auth header value (for debugging/logging)."""
        token = base64.b64encode(f"API_KEY:{self.api_key}".encode("utf-8")).decode("utf-8")
        return f"Basic {token}"
