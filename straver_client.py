"""Small helper for talking to the Straver microservice."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

log = logging.getLogger(__name__)


class StraverClient:
    """HTTP client for Straver internal endpoints."""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        secret: Optional[str] = None,
        timeout: float = 15.0,
    ):
        self.base_url = (base_url or os.environ.get("STRAVER_BASE_URL") or "").rstrip("/")
        self.secret = secret or os.environ.get("STRAVER_INTERNAL_SECRET") or ""
        self.timeout = timeout
        self.session = requests.Session()
        if self.secret:
            self.session.headers.update({"X-Internal-Secret": self.secret})

    def is_configured(self) -> bool:
        return bool(self.base_url and self.secret)

    def build_authorize_url(self, state: str) -> str:
        if not self.base_url:
            raise RuntimeError("STRAVER_BASE_URL is not configured")
        return f"{self.base_url}/strava/authorize?state={state}"

    def connection_status(self, user_ids: Iterable[int]) -> Dict[int, Dict]:
        """Return Strava connection info for the provided users."""
        ids: List[int] = [int(uid) for uid in user_ids]
        if not ids:
            return {}
        if not self.is_configured():
            log.warning("Straver is not configured, returning empty statuses")
            return {}
        response = self.session.post(
            f"{self.base_url}/internal/strava/status",
            json={"user_ids": ids},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items", {}) or {}
        normalized = {}
        for key, value in items.items():
            try:
                normalized[int(key)] = value
            except (TypeError, ValueError):
                normalized[key] = value
        return normalized

    def upload_activity(self, *, tg_user_id: int, file_path: Path, name: str, description: str) -> Dict:
        """Ask Straver to upload an activity on behalf of the user."""
        if not self.is_configured():
            raise RuntimeError("Straver client is not configured")
        with file_path.open("rb") as fp:
            response = self.session.post(
                f"{self.base_url}/internal/strava/upload",
                data={"tg_user_id": tg_user_id, "name": name, "description": description},
                files={"file": (file_path.name, fp, "application/octet-stream")},
                timeout=self.timeout,
            )
        response.raise_for_status()
        return response.json()

    def disconnect(self, tg_user_id: int) -> bool:
        """Remove stored Strava tokens for the user."""
        if not self.is_configured():
            raise RuntimeError("Straver client is not configured")
        response = self.session.post(
            f"{self.base_url}/internal/strava/disconnect",
            json={"tg_user_id": tg_user_id},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        return bool(data.get("deleted") or data.get("status") == "ok")
