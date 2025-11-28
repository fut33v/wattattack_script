from __future__ import annotations

from datetime import date, datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query

from repositories.db_utils import db_connection, dict_cursor
from repositories import client_balance_repository, client_subscription_repository, client_repository, schedule_repository
from ..dependencies import require_admin

router = APIRouter(prefix="/stats", tags=["stats"], dependencies=[Depends(require_admin)])


def _parse_month(month_str: str | None, *, now: date) -> date:
    """Return the first day of the requested month or of the current month."""
    if not month_str:
        return now.replace(day=1)

    for fmt in ("%Y-%m", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(month_str, fmt).date()
            return parsed.replace(day=1)
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail="month must be in YYYY-MM format")


def _next_month(day: date) -> date:
    """Return the first day of the next month."""
    return (day.replace(day=28) + timedelta(days=4)).replace(day=1)


@router.get("")
def api_stats(month: str | None = Query(default=None, description="Month in YYYY-MM format")):
    today = schedule_repository.datetime.now().date()
    month_start = _parse_month(month, now=today)
    next_month = _next_month(month_start)
    month_key = month_start.strftime("%Y-%m")

    # Sum balance inflows (positive adjustments)
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(delta_rub), 0) AS income_rub
            FROM client_balance_adjustments
            WHERE delta_rub > 0
            """
        )
        balance_row = cur.fetchone() or {}

    # Sum subscription purchases (price_rub) across all subscriptions
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(price_rub), 0) AS income_rub
            FROM client_subscriptions
            WHERE price_rub IS NOT NULL
            """
        )
        subs_row = cur.fetchone() or {}

    clients_total = client_repository.count_clients()

    with db_connection() as conn, dict_cursor(conn) as cur:
        now = schedule_repository.datetime.now()
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN slot_date >= %s THEN 1 ELSE 0 END) AS upcoming,
                SUM(CASE WHEN slot_date < %s THEN 1 ELSE 0 END) AS past
            FROM schedule_reservations
            INNER JOIN schedule_slots ON schedule_slots.id = schedule_reservations.slot_id
            WHERE schedule_reservations.client_id IS NOT NULL
              AND (schedule_reservations.status IS NULL OR LOWER(schedule_reservations.status) NOT IN ('cancelled', 'canceled'))
            """,
            (now.date(), now.date()),
        )
        reservations_row = cur.fetchone() or {}

    # Monthly balance inflows
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(delta_rub), 0) AS income_rub
            FROM client_balance_adjustments
            WHERE delta_rub > 0
              AND created_at >= %s
              AND created_at < %s
            """,
            (month_start, next_month),
        )
        balance_month_row = cur.fetchone() or {}

    # Monthly subscription purchases
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(price_rub), 0) AS income_rub
            FROM client_subscriptions
            WHERE price_rub IS NOT NULL
              AND created_at >= %s
              AND created_at < %s
            """,
            (month_start, next_month),
        )
        subs_month_row = cur.fetchone() or {}

    # Monthly reservations count
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS reservations
            FROM schedule_reservations
            INNER JOIN schedule_slots ON schedule_slots.id = schedule_reservations.slot_id
            WHERE schedule_slots.slot_date >= %s
              AND schedule_slots.slot_date < %s
              AND schedule_reservations.client_id IS NOT NULL
              AND (schedule_reservations.status IS NULL OR LOWER(schedule_reservations.status) NOT IN ('cancelled', 'canceled'))
            """,
            (month_start, next_month),
        )
        reservations_month_row = cur.fetchone() or {}

    # Weekly breakdown for the month
    weekly_map = {}
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DATE_TRUNC('week', created_at)::date AS week_start, COALESCE(SUM(delta_rub), 0) AS income_rub
            FROM client_balance_adjustments
            WHERE delta_rub > 0
              AND created_at >= %s
              AND created_at < %s
            GROUP BY week_start
            """,
            (month_start, next_month),
        )
        for row in cur.fetchall():
            week = row.get("week_start")
            weekly_map.setdefault(week, {"income_rub": 0, "reservations": 0})
            weekly_map[week]["income_rub"] += int(row.get("income_rub") or 0)

        cur.execute(
            """
            SELECT DATE_TRUNC('week', created_at)::date AS week_start, COALESCE(SUM(price_rub), 0) AS income_rub
            FROM client_subscriptions
            WHERE price_rub IS NOT NULL
              AND created_at >= %s
              AND created_at < %s
            GROUP BY week_start
            """,
            (month_start, next_month),
        )
        for row in cur.fetchall():
            week = row.get("week_start")
            weekly_map.setdefault(week, {"income_rub": 0, "reservations": 0})
            weekly_map[week]["income_rub"] += int(row.get("income_rub") or 0)

        cur.execute(
            """
            SELECT DATE_TRUNC('week', schedule_slots.slot_date)::date AS week_start, COUNT(*) AS reservations
            FROM schedule_reservations
            INNER JOIN schedule_slots ON schedule_slots.id = schedule_reservations.slot_id
            WHERE schedule_slots.slot_date >= %s
              AND schedule_slots.slot_date < %s
              AND schedule_reservations.client_id IS NOT NULL
              AND (schedule_reservations.status IS NULL OR LOWER(schedule_reservations.status) NOT IN ('cancelled', 'canceled'))
            GROUP BY week_start
            """,
            (month_start, next_month),
        )
        for row in cur.fetchall():
            week = row.get("week_start")
            weekly_map.setdefault(week, {"income_rub": 0, "reservations": 0})
            weekly_map[week]["reservations"] = int(row.get("reservations") or 0)

    first_week_start = month_start - timedelta(days=month_start.weekday())
    weeks = []
    cursor = first_week_start
    while cursor < next_month:
        week_data = weekly_map.get(cursor) or {"income_rub": 0, "reservations": 0}
        week_end = min(cursor + timedelta(days=6), next_month - timedelta(days=1))
        weeks.append(
            {
                "week_start": cursor.isoformat(),
                "week_end": week_end.isoformat(),
                "income_rub": int(week_data.get("income_rub") or 0),
                "reservations": int(week_data.get("reservations") or 0),
            }
        )
        cursor += timedelta(days=7)

    # Available months where we have any activity
    month_candidates = {month_key}
    with db_connection() as conn, dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', created_at)::date, 'YYYY-MM') AS month_key
            FROM client_balance_adjustments
            WHERE delta_rub > 0
            """
        )
        month_candidates.update(row["month_key"] for row in cur.fetchall() if row.get("month_key"))

        cur.execute(
            """
            SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', created_at)::date, 'YYYY-MM') AS month_key
            FROM client_subscriptions
            WHERE price_rub IS NOT NULL
            """
        )
        month_candidates.update(row["month_key"] for row in cur.fetchall() if row.get("month_key"))

        cur.execute(
            """
            SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', schedule_slots.slot_date)::date, 'YYYY-MM') AS month_key
            FROM schedule_reservations
            INNER JOIN schedule_slots ON schedule_slots.id = schedule_reservations.slot_id
            WHERE schedule_reservations.client_id IS NOT NULL
              AND (schedule_reservations.status IS NULL OR LOWER(schedule_reservations.status) NOT IN ('cancelled', 'canceled'))
            """
        )
        month_candidates.update(row["month_key"] for row in cur.fetchall() if row.get("month_key"))

    available_months = sorted(month_candidates, reverse=True)

    return {
        "balance_income_rub": int(balance_row.get("income_rub") or 0),
        "subscriptions_income_rub": int(subs_row.get("income_rub") or 0),
        "total_income_rub": int(balance_row.get("income_rub") or 0) + int(subs_row.get("income_rub") or 0),
        "clients_total": clients_total,
        "reservations_upcoming": int(reservations_row.get("upcoming") or 0),
        "reservations_past": int(reservations_row.get("past") or 0),
        "available_months": available_months,
        "monthly": {
            "month": month_key,
            "balance_income_rub": int(balance_month_row.get("income_rub") or 0),
            "subscriptions_income_rub": int(subs_month_row.get("income_rub") or 0),
            "total_income_rub": int(balance_month_row.get("income_rub") or 0) + int(subs_month_row.get("income_rub") or 0),
            "reservations": int(reservations_month_row.get("reservations") or 0),
            "weeks": weeks,
        },
    }
