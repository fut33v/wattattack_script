#!/usr/bin/env python3
"""Run WattAttack notifier in a loop, checking for new activities every interval."""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Iterable

from .notifier import main as notifier_main

DEFAULT_INTERVAL = int(os.environ.get("WATTATTACK_INTERVAL_SECONDS", str(30 * 60)))

STOP_REQUESTED = False


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Periodically run wattattackscheduler.notifier without cron.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help="Interval between checks in seconds",
    )
    parser.add_argument(
        "--stop-file",
        type=Path,
        help="Optional path to a file; if it appears, the loop stops gracefully",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--notifier-args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the notifier (prefix with --notifier-args)",
    )
    return parser.parse_args(argv)


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def request_stop(*_args) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def main(argv: Iterable[str] | None = None) -> int:
    global STOP_REQUESTED
    args = parse_args(argv)
    setup_logging(args.verbose)

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    log = logging.getLogger("scheduler")
    log.info(
        "Starting WattAttack scheduler: interval=%ss, notifier_args=%s",
        args.interval,
        args.notifier_args,
    )

    iteration = 0
    while not STOP_REQUESTED:
        iteration += 1
        log.info("Iteration %d: invoking notifier", iteration)
        try:
            notifier_exit = notifier_main(args.notifier_args or [])
        except SystemExit as exit_info:
            notifier_exit = exit_info.code if isinstance(exit_info.code, int) else 1
        except Exception:
            log.exception("Notifier raised an unexpected exception")
            notifier_exit = 1

        if notifier_exit != 0:
            log.warning("Notifier exit code %s", notifier_exit)

        if args.stop_file and args.stop_file.exists():
            log.info("Stop file detected: %s", args.stop_file)
            break

        if STOP_REQUESTED:
            break

        log.debug("Sleeping for %s seconds", args.interval)
        for _ in range(args.interval):
            if STOP_REQUESTED:
                break
            if args.stop_file and args.stop_file.exists():
                log.info("Stop file detected during sleep: %s", args.stop_file)
                STOP_REQUESTED = True
                break
            time.sleep(1)

    log.info("Scheduler stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
