"""WattAttack scheduler and notifier package."""

from .notifier import main as notifier_main
from .scheduler import main as scheduler_main

__all__ = ["notifier_main", "scheduler_main"]

