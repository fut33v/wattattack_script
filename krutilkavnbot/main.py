"""CLI entry point for the Krutilka VNB Telegram bot."""
from __future__ import annotations

import logging
import os
import sys

from .bot import DEFAULT_GREETING, create_application

BOT_TOKEN_ENV = "KRUTILKAVN_BOT_TOKEN"
GREETING_ENV = "KRUTILKAVN_GREETING"


def configure_logging() -> None:
    """Set up basic logging for the bot process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    """Run the bot with settings sourced from environment variables."""
    configure_logging()

    token = os.environ.get(BOT_TOKEN_ENV)
    if not token:
        logging.critical(
            "Environment variable %s must be set with a Telegram bot token",
            BOT_TOKEN_ENV,
        )
        raise SystemExit(1)

    greeting = os.environ.get(GREETING_ENV, DEFAULT_GREETING)
    application = create_application(token=token, greeting=greeting)

    logging.info("Starting Krutilka VNB bot")
    application.run_polling(close_loop=False)


if __name__ == "__main__":
    main()

