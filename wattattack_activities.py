#!/usr/bin/env python3
"""Download WattAttack activities list and optional FIT files via the web API."""
from __future__ import annotations

import argparse
import csv
import json
import sys
from getpass import getpass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import requests

DEFAULT_BASE_URL = "https://wattattack.com"
API_PREFIX = "/api/v1"


class WattAttackClient:
    """Thin wrapper around the WattAttack web API."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/119.0.0.0 Safari/537.36"
                ),
                "Referer": f"{self.base_url}/login",
                "Origin": self.base_url,
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

    def _api_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{API_PREFIX}{path}"

    def login(self, email: str, password: str, *, timeout: float | None = None) -> Dict[str, Any]:
        """Authenticate and return the user payload."""

        payload = {"email": email, "password": password}
        response = self.session.post(
            self._api_url("/auth/login"), json=payload, timeout=timeout
        )
        user_data = self._parse_json(response)

        if response.status_code != 200 or "user" not in user_data:
            message = user_data.get("message") if isinstance(user_data, dict) else None
            if not message:
                message = response.text.strip() or "unexpected response"
            raise RuntimeError(f"Login failed ({response.status_code}): {message}")

        return user_data["user"]

    def fetch_activities(self, *, timeout: float | None = None) -> Dict[str, Any]:
        """Return the full activities payload including totals."""

        response = self.session.get(
            self._api_url("/activities"), timeout=timeout
        )
        if response.status_code != 200:
            message: str
            try:
                data = response.json()
            except ValueError:
                data = None
            if isinstance(data, dict):
                message = data.get("message", "unexpected response")
            else:
                message = response.text.strip() or "unexpected response"
            raise RuntimeError(
                f"Failed to fetch activities ({response.status_code}): {message}"
            )

        return response.json()

    def fetch_profile(self, *, timeout: float | None = None) -> Dict[str, Any]:
        """Return the athlete profile details for the current session."""

        response = self.session.get(
            self._api_url("/athlete"), timeout=timeout
        )
        if response.status_code in {200, 404}:
            try:
                return response.json()
            except ValueError:
                return {}

        message = response.text.strip() or "unexpected response"
        raise RuntimeError(
            f"Failed to fetch profile ({response.status_code}): {message}"
        )

    def update_profile(
        self,
        payload: Dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> Dict[str, Any]:
        """Update athlete profile details via the official API."""

        response = self.session.put(
            self._api_url("/athlete/update"),
            json=payload,
            timeout=timeout,
        )

        if response.status_code in {200, 400}:
            try:
                return response.json()
            except ValueError:
                return {"raw": response.text.strip()}

        message = response.text.strip() or "unexpected response"
        raise RuntimeError(
            f"Failed to update profile ({response.status_code}): {message}"
        )

    def update_user(
        self,
        payload: Dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> Dict[str, Any]:
        """Update user account details (first name, last name, etc.)."""

        response = self.session.put(
            self._api_url("/user/update"),
            json=payload,
            timeout=timeout,
        )

        if response.status_code in {200, 400}:
            try:
                return response.json()
            except ValueError:
                return {"raw": response.text.strip()}

        message = response.text.strip() or "unexpected response"
        raise RuntimeError(
            f"Failed to update user ({response.status_code}): {message}"
        )

    def auth_check(self, *, timeout: float | None = None) -> Dict[str, Any]:
        """Return information about the authenticated user."""

        response = self.session.get(
            self._api_url("/auth/check"), timeout=timeout
        )
        if response.status_code in {200, 401}:
            try:
                return response.json()
            except ValueError:
                return {"raw": response.text.strip()}

        message = response.text.strip() or "unexpected response"
        raise RuntimeError(
            f"Failed to check auth ({response.status_code}): {message}"
        )


    def download_fit_file(
        self,
        fit_file_id: str,
        destination: Path,
        *,
        timeout: float | None = None,
    ) -> None:
        """Download a FIT file identified by *fit_file_id* into *destination*."""

        response = self.session.get(
            self._api_url(f"/activity/download/{fit_file_id}"),
            stream=True,
            timeout=timeout,
        )
        if response.status_code != 200:
            message = response.text.strip() or "unexpected response"
            raise RuntimeError(
                f"Failed to download FIT {fit_file_id} ({response.status_code}): {message}"
            )

        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)

    def upload_workout(
        self,
        payload: Dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> Dict[str, Any]:
        """Upload a parsed workout to the user's library."""

        response = self.session.post(
            self._api_url("/workouts/user-create"),
            json=payload,
            timeout=timeout,
        )

        if response.status_code == 201:
            try:
                return response.json()
            except ValueError:
                return {}

        data = self._parse_json(response)
        message = ""
        if isinstance(data, dict):
            message = data.get("message") or ""
        if not message:
            message = response.text.strip() or "unexpected response"

        if response.status_code in {400, 401, 500}:
            raise RuntimeError(
                f"Failed to upload workout ({response.status_code}): {message}"
            )

        raise RuntimeError(
            f"Failed to upload workout ({response.status_code}): {message}"
        )

    @staticmethod
    def _parse_json(response: requests.Response) -> Dict[str, Any]:
        try:
            return response.json()
        except ValueError:
            return {}


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download WattAttack activities as JSON or CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("email", help="Account email used on wattattack.com")
    parser.add_argument(
        "--password",
        help="Account password (omit to be prompted securely)",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Override the WattAttack base URL if needed",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=Path("activities.json"),
        type=Path,
        help="Where to store the downloaded data (use '-' for stdout)",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        help=(
            "Folder for per-activity FIT downloads; existing files are reused so "
            "only new activities are fetched"
        ),
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=("json", "csv"),
        default="json",
        help="Output format",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds",
    )
    return parser.parse_args(argv)


def prompt_password(existing: str | None) -> str:
    if existing:
        return existing
    return getpass("WattAttack password: ")


def write_json(payload: Dict[str, Any], destination: Path | None) -> None:
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    if destination is None:
        sys.stdout.write(serialized)
        if not serialized.endswith("\n"):
            sys.stdout.write("\n")
    else:
        destination.write_text(serialized, encoding="utf-8")


def write_csv(activities: List[Dict[str, Any]], destination: Path | None) -> None:
    if not activities:
        header: List[str] = []
    else:
        header = sorted({key for item in activities for key in item.keys()})

    def normalise(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    if destination is None:
        writer = csv.DictWriter(sys.stdout, fieldnames=header)
        writer.writeheader()
        for row in activities:
            writer.writerow({key: normalise(row.get(key)) for key in header})
    else:
        with destination.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=header)
            writer.writeheader()
            for row in activities:
                writer.writerow({key: normalise(row.get(key)) for key in header})


def resolve_output_path(raw: Path) -> Tuple[Path | None, bool]:
    if str(raw) == "-":
        return None, False
    return raw, not raw.exists()


def safe_filename_fragment(value: Any) -> str:
    """Return a filesystem-friendly fragment derived from *value*."""

    fragment = re.sub(r"[^A-Za-z0-9._-]", "_", str(value))
    return fragment or "unknown"


def download_fit_files(
    client: WattAttackClient,
    activities: List[Dict[str, Any]],
    download_dir: Path,
    *,
    timeout: float,
) -> Tuple[int, int, int]:
    """Download FIT files for *activities* into *download_dir*.

    Returns a tuple with counts of (downloaded, skipped, missing).
    """

    download_dir.mkdir(parents=True, exist_ok=True)

    downloaded = skipped = missing = 0
    for activity in activities:
        fit_id = activity.get("fitFileId")
        if not fit_id:
            missing += 1
            continue

        act_id = activity.get("id", "unknown")
        filename = (
            f"activity_{safe_filename_fragment(act_id)}_"
            f"{safe_filename_fragment(fit_id)}.fit"
        )
        destination = download_dir / filename

        if destination.exists():
            skipped += 1
            continue

        try:
            client.download_fit_file(str(fit_id), destination, timeout=timeout)
        except Exception as exc:  # noqa: BLE001 - surface per-activity errors
            print(f"Error downloading {fit_id}: {exc}", file=sys.stderr)
            missing += 1
            continue

        downloaded += 1

    return downloaded, skipped, missing


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    password = prompt_password(args.password)

    client = WattAttackClient(args.base_url)

    try:
        user = client.login(args.email, password, timeout=args.timeout)
    except Exception as exc:  # noqa: BLE001 - surface useful message
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    try:
        payload = client.fetch_activities(timeout=args.timeout)
    except Exception as exc:  # noqa: BLE001 - surface useful message
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    activities = payload.get("activities", [])
    output_path, is_new_file = resolve_output_path(args.output)

    if args.format == "json":
        write_json(payload, output_path)
    else:
        write_csv(activities, output_path)

    target_desc = "stdout" if output_path is None else str(output_path)

    summary = (
        f"Saved {len(activities)} activities for {user.get('firstName', '')} "
        f"{user.get('lastName', '')} -> {target_desc}"
    )

    if args.download_dir:
        downloaded, skipped, missing = download_fit_files(
            client,
            activities,
            args.download_dir,
            timeout=args.timeout,
        )
        summary += (
            f"; FIT files downloaded: {downloaded}, skipped: {skipped}, "
            f"missing: {missing}"
        )

    print(summary, file=sys.stderr)

    if output_path is not None and is_new_file:
        print(f"Wrote file: {output_path.resolve()}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
