"""Allow `python -m wattattackscheduler` to run the scheduler loop."""

from .scheduler import main


if __name__ == "__main__":
    raise SystemExit(main())

