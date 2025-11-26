from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from repositories import schedule_repository


log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter()


def _decorate_leaderboard_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    decorated: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        raw_last = item.get("last_activity_at")
        last_label = None
        if isinstance(raw_last, str):
            try:
                parsed = datetime.fromisoformat(raw_last)
                last_label = parsed.strftime("%d.%m.%Y")
            except ValueError:
                last_label = raw_last[:10]

        distance_value = float(item.get("distance_km") or item.get("distance") or 0)
        distance_label = f"{distance_value:,.1f}".replace(",", " ")

        decorated.append(
            {
                **item,
                "rank": idx,
                "distance_label": distance_label,
                "last_activity_label": last_label,
            }
        )
    return decorated


def _build_summary(summary_raw: Dict[str, Any]) -> Dict[str, Any]:
    total_distance_value = float(summary_raw.get("total_distance_km") or summary_raw.get("total_distance") or 0)
    return {
        "total_distance": total_distance_value,
        "athletes": int(summary_raw.get("athletes") or 0),
        "rides_with_distance": int(summary_raw.get("rides_with_distance") or 0),
        "total_distance_label": f"{total_distance_value:,.0f}".replace(",", " "),
    }


@router.get("/leaderboard", response_class=HTMLResponse)
def public_leaderboard_page(request: Request, limit: int = 100):
    try:
        data = schedule_repository.get_distance_leaderboard(limit=limit)
        items = data.get("items") or []
        summary_raw = data.get("summary") or {}
    except Exception:
        log.exception("Failed to load leaderboard")
        response = templates.TemplateResponse(
            "public_leaderboard.html",
            {
                "request": request,
                "items": [],
                "summary": {
                    "total_distance_label": "0",
                    "athletes": 0,
                    "rides_with_distance": 0,
                },
                "error_message": "Не удалось загрузить лидерборд. Попробуйте обновить страницу позже.",
            },
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    decorated_items = _decorate_leaderboard_items(items)
    summary_payload = _build_summary(summary_raw)

    top_entry = decorated_items[0] if decorated_items else None
    context = {
        "request": request,
        "items": decorated_items,
        "summary": summary_payload,
        "top_entry": top_entry,
        "share_url": str(request.url),
        "error_message": None,
    }

    response = templates.TemplateResponse("public_leaderboard.html", context)
    response.headers["Cache-Control"] = "no-store"
    return response
