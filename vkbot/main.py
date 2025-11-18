"""CLI entry point for the VK community bot using long poll."""
from __future__ import annotations

import logging
import os

from .bot import DEFAULT_GREETING, run_bot

log = logging.getLogger(__name__)

TOKEN_ENV = "VK_API_COMMUNITY_KEY"
GROUP_ID_ENV = "VK_GROUP_ID"
GREETING_ENV = "VK_BOT_GREETING"
API_VERSION_ENV = "VK_API_VERSION"


def configure_logging() -> None:
    """Set up basic logging for the bot process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> int:
    """Load settings from the environment and start the VK bot."""
    configure_logging()

    token = os.environ.get(TOKEN_ENV)
    if not token:
        log.critical("Environment variable %s must be set with a VK community token", TOKEN_ENV)
        return 1

    group_id_raw = os.environ.get(GROUP_ID_ENV)
    if not group_id_raw:
        log.critical("Environment variable %s must be set with your community group ID", GROUP_ID_ENV)
        return 1

    try:
        group_id = int(group_id_raw)
    except ValueError:
        log.critical("VK_GROUP_ID must be an integer; got %s", group_id_raw)
        return 1

    greeting = os.environ.get(GREETING_ENV, DEFAULT_GREETING)
    api_version = os.environ.get(API_VERSION_ENV)

    try:
        run_bot(
            token=token,
            group_id=group_id,
            greeting=greeting,
            api_version=api_version,
        )
    except KeyboardInterrupt:
        log.info("VK bot stopped by user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

