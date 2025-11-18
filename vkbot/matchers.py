"""Helpers to match free-text selections to known entities during flows."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from .formatting import format_day_display, format_slot_label, format_reservation_label


def match_day_text(text: str, *, day_map: Dict[str, List[Dict[str, Any]]] | None, mode: str | None) -> Optional[str]:
    if mode != "day_select" or not day_map:
        return None
    t_clean = (text or "").strip().lower()
    for iso_date, slots in day_map.items():
        day_obj: Optional[date] = None
        if slots:
            candidate = slots[0].get("slot_date")
            if isinstance(candidate, date):
                day_obj = candidate
        if day_obj is None:
            try:
                day_obj = date.fromisoformat(iso_date)
            except Exception:
                day_obj = None
        if day_obj is None:
            continue
        label = format_day_display(day_obj).lower()
        short_label = day_obj.strftime("%d.%m").lower()
        if t_clean in {label, short_label, day_obj.isoformat().lower()}:
            return iso_date
    return None


def match_slot_text(text: str, *, slots_map: Dict[int, Dict[str, Any]] | None, mode: str | None) -> Optional[int]:
    if mode != "slot_select" or not slots_map:
        return None
    cleaned = (text or "").strip().lower()
    for slot_id, slot in slots_map.items():
        label = format_slot_label(slot).lower()
        if cleaned == label or cleaned == str(slot_id):
            return slot_id
    return None


def match_cancel_text(text: str, *, cancel_map: Dict[int, Dict[str, Any]] | None, mode: str | None) -> Optional[int]:
    if mode != "cancel_select" or not cancel_map:
        return None
    cleaned = (text or "").strip().lower()
    for res_id, res in cancel_map.items():
        label = format_reservation_label(res).lower()
        if cleaned == label or cleaned == str(res_id):
            return res_id
    return None


def match_booking_text(text: str, *, booking_map: Dict[int, Dict[str, Any]] | None, mode: str | None) -> Optional[int]:
    if mode not in {"booking_list", "booking_details"} or not booking_map:
        return None
    cleaned = (text or "").strip().lower()
    for res_id, res in booking_map.items():
        label = format_reservation_label(res).lower()
        if cleaned == label or cleaned == str(res_id):
            return res_id
    return None

