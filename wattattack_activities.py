#!/usr/bin/env python3
"""Download WattAttack activities list and optional FIT files via the web API."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple

import requests

DEFAULT_BASE_URL = "https://wattattack.com"
API_PREFIX = "/api/v1"
LOGGER = logging.getLogger(__name__)


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

    def fetch_activity_feed(
        self,
        *,
        limit: int,
        timeout: float | None = None,
        page_size: int | None = None,
        max_pages: int = 5,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Return recent activities attempting to walk through paginated API responses.

        The first element of the returned tuple is the activities list (deduplicated).
        The second element contains non-activity fields from the initial payload
        (totals, aggregations, etc.) preserved for backwards compatibility.
        """

        if limit <= 0:
            return [], {}

        initial_payload = self.fetch_activities(timeout=timeout)
        activities = initial_payload.get("activities", [])
        if not isinstance(activities, list):
            LOGGER.debug("Activities payload is not a list, got %s", type(activities))
            activities = []

        metadata = {
            key: value for key, value in initial_payload.items() if key != "activities"
        }

        collected: List[Dict[str, Any]] = list(activities)
        seen_ids = {
            str(item.get("id"))
            for item in collected
            if isinstance(item, dict) and item.get("id") is not None
        }

        effective_page_size = page_size or max(limit, len(collected), 50)
        target = max(limit, effective_page_size)
        if len(collected) >= target:
            return collected[:target], metadata

        strategies = _build_pagination_strategies()
        for strategy in strategies:
            start_index = strategy.start
            page = start_index
            attempts = 0
            initial_count = len(collected)
            progress = False

            while len(collected) < target and attempts < max_pages:
                params = strategy.build(page, effective_page_size)
                if params is None:
                    break

                try:
                    response = self.session.get(
                        self._api_url("/activities"),
                        params=params,
                        timeout=timeout,
                    )
                except requests.RequestException as exc:
                    LOGGER.debug(
                        "Pagination strategy %s failed for page %s: %s",
                        strategy.name,
                        page,
                        exc,
                    )
                    break

                if response.status_code != 200:
                    LOGGER.debug(
                        "Pagination strategy %s returned HTTP %s for page %s",
                        strategy.name,
                        response.status_code,
                        page,
                    )
                    break

                payload = self._parse_json(response)
                page_activities = payload.get("activities")
                if not isinstance(page_activities, list) or not page_activities:
                    break

                new_items = 0
                for item in page_activities:
                    if not isinstance(item, dict):
                        continue
                    key = item.get("id")
                    key_str = str(key) if key is not None else None
                    if key_str and key_str not in seen_ids:
                        collected.append(item)
                        seen_ids.add(key_str)
                        new_items += 1

                if new_items:
                    progress = True

                attempts += 1
                if len(collected) >= target:
                    break

                if new_items == 0 and page > start_index:
                    break

                page += 1

            if progress:
                metadata = dict(metadata)
                metadata["_pagination_strategy"] = strategy.name
                metadata["_pagination_page_size"] = effective_page_size
                LOGGER.debug(
                    "Pagination strategy %s added %d new activities (total=%d)",
                    strategy.name,
                    len(collected) - initial_count,
                    len(collected),
                )
                break

        return collected[:target], metadata

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


@dataclass(frozen=True)
class _PaginationStrategy:
    name: str
    build: Callable[[int, int], Dict[str, int]]
    start: int = 1


def _build_pagination_strategies() -> List[_PaginationStrategy]:
    return [
        _PaginationStrategy(
            name="page_pageSize",
            build=lambda page, size: {"page": page, "pageSize": size},
        ),
        _PaginationStrategy(
            name="page_limit",
            build=lambda page, size: {"page": page, "limit": size},
        ),
        _PaginationStrategy(
            name="page_perPage",
            build=lambda page, size: {"page": page, "perPage": size},
        ),
        _PaginationStrategy(
            name="page_zero_pageSize",
            build=lambda page, size: {"page": page - 1, "pageSize": size},
        ),
        _PaginationStrategy(
            name="offset_limit",
            build=lambda page, size: {"offset": (page - 1) * size, "limit": size},
        ),
        _PaginationStrategy(
            name="skip_take",
            build=lambda page, size: {"skip": (page - 1) * size, "take": size},
        ),
    ]


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

    activity_limit = 500
    try:
        activities, metadata = client.fetch_activity_feed(
            limit=activity_limit,
            timeout=args.timeout,
        )
    except Exception as exc:  # noqa: BLE001 - surface useful message
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    payload = dict(metadata)
    payload["activities"] = activities
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
