#!/usr/bin/env python3
"""Helpers for parsing ZWO workouts and preparing WattAttack payloads."""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

MAX_SEGMENTS = 1000
MAX_DURATION = 36000  # 10 hours
MAX_POWER = 10.0  # 1000% FTP
MAX_ENTITY_REFERENCES = 1000
MAX_TEXT_EVENTS_PER_SEGMENT = 50
MAX_TEXT_EVENT_LENGTH = 500
MAX_NAME_LENGTH = 1000
MAX_DESCRIPTION_LENGTH = 5000

KNOWN_SEGMENT_TAGS = {
    "warmup",
    "cooldown",
    "steadystate",
    "freeride",
    "ramp",
    "intervalst",
    "maxeffort",
}

ROOT_FIELD_MAP = {
    "uniqueid": "uniqueId",
    "legacyidhash": "legacyIdHash",
    "category": "category",
    "subcategory": "subcategory",
    "name": "name",
    "description": "description",
    "author": "author",
    "sporttype": "sportType",
}

STRING_FIELDS = {"name", "description", "author", "category", "subcategory", "sportType"}

SEGMENT_ATTRIBUTES: Dict[str, set[str]] = {
    "warmup": {
        "duration",
        "cadence",
        "cadencehigh",
        "cadencelow",
        "cadenceresting",
        "pace",
        "power",
        "powerhigh",
        "powerlow",
        "zone",
    },
    "cooldown": {
        "duration",
        "cadence",
        "cadencehigh",
        "cadencelow",
        "cadenceresting",
        "pace",
        "power",
        "powerhigh",
        "powerlow",
        "zone",
    },
    "freeride": {
        "duration",
        "cadence",
        "cadencehigh",
        "cadencelow",
        "flatroad",
        "ftptest",
        "power",
    },
    "ramp": {
        "duration",
        "cadence",
        "cadenceresting",
        "pace",
        "power",
        "powerhigh",
        "powerlow",
    },
    "intervalst": {
        "repeat",
        "onduration",
        "onpower",
        "poweronzone",
        "poweronhigh",
        "poweronlow",
        "offduration",
        "offpower",
        "poweroffzone",
        "poweroffhigh",
        "powerofflow",
        "cadence",
        "cadencehigh",
        "cadencelow",
        "cadenceresting",
        "flatroad",
        "overunder",
        "pace",
    },
    "maxeffort": {
        "duration",
    },
    "steadystate": {
        "duration",
        "cadence",
        "cadencehigh",
        "cadencelow",
        "cadenceresting",
        "offpower",
        "pace",
        "power",
        "powerhigh",
        "powerlow",
        "target",
        "zone",
    },
}

ATTR_CANONICAL = {
    "duration": "Duration",
    "cadence": "Cadence",
    "cadencehigh": "CadenceHigh",
    "cadencelow": "CadenceLow",
    "cadenceresting": "CadenceResting",
    "pace": "Pace",
    "power": "Power",
    "powerhigh": "PowerHigh",
    "powerlow": "PowerLow",
    "zone": "Zone",
    "flatroad": "FlatRoad",
    "ftptest": "ftptest",
    "repeat": "Repeat",
    "onduration": "OnDuration",
    "onpower": "OnPower",
    "poweronzone": "PowerOnZone",
    "poweronhigh": "PowerOnHigh",
    "poweronlow": "PowerOnLow",
    "offduration": "OffDuration",
    "offpower": "OffPower",
    "poweroffzone": "PowerOffZone",
    "poweroffhigh": "PowerOffHigh",
    "powerofflow": "PowerOffLow",
    "overunder": "OverUnder",
    "target": "Target",
}

POWER_ZONES: List[Dict[str, Any]] = [
    {"id": 1, "name": "Recovery", "color": "#bababa", "min": 0, "max": 0.55},
    {"id": 2, "name": "Endurance", "color": "#2d7cb5", "min": 0.55, "max": 0.75},
    {"id": 3, "name": "Tempo", "color": "#3ca23c", "min": 0.75, "max": 0.90},
    {"id": 4, "name": "Threshold", "color": "#f9e318", "min": 0.90, "max": 1.05},
    {"id": 5, "name": "VO2 Max", "color": "#ef8529", "min": 1.05, "max": 1.20},
    {"id": 6, "name": "Anaerobic", "color": "#d93951", "min": 1.20, "max": 1.50},
    {"id": 7, "name": "Neuromuscular", "color": "#7b1fa2", "min": 1.50, "max": math.inf},
]


def _basic_xml_safety_checks(xml_text: str, *, max_size: int = 2 * 1024 * 1024) -> None:
    if not xml_text:
        raise ValueError("Empty file")
    if len(xml_text) > max_size:
        raise ValueError("XML too large")
    lowered = xml_text.lower()
    if "<!doctype" in lowered:
        raise ValueError("DOCTYPE is not allowed")
    if "<!entity" in lowered:
        raise ValueError("ENTITY declarations are not allowed")
    if xml_text.count("&") > MAX_ENTITY_REFERENCES and "&amp;" not in xml_text:
        raise ValueError("Too many entity references")


def _capitalize_attr(attr: str) -> str:
    return attr[:1].upper() + attr[1:] if attr else attr


def _normalize_segment_type(tag: str) -> str:
    tag = tag.lower()
    if tag == "intervalst":
        return "IntervalsT"
    if tag == "steadystate":
        return "SteadyState"
    if tag == "freeride":
        return "FreeRide"
    if tag == "maxeffort":
        return "MaxEffort"
    return tag[:1].upper() + tag[1:]


def _try_parse_number(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_if_not_none(value: Optional[float]) -> Optional[int]:
    if value is None:
        return None
    return int(round(value))


def _sanitize_segment(element: ET.Element) -> Optional[Dict[str, Any]]:
    lower_type = element.tag.lower()
    if lower_type not in KNOWN_SEGMENT_TAGS:
        return None

    allowed = SEGMENT_ATTRIBUTES.get(lower_type, set())
    attrs: Dict[str, Any] = {}
    power_range: Optional[Dict[str, float]] = None
    power_on_range: Optional[Dict[str, float]] = None
    power_off_range: Optional[Dict[str, float]] = None

    for attr_name, raw_value in element.attrib.items():
        lower_attr = attr_name.lower()
        if lower_attr not in allowed:
            continue
        canon = ATTR_CANONICAL.get(lower_attr, _capitalize_attr(lower_attr))
        value = raw_value.strip() if isinstance(raw_value, str) else raw_value
        if isinstance(value, str) and value == "":
            continue
        numeric_value = _try_parse_number(value) if isinstance(value, str) else None

        processed: Any
        if numeric_value is not None:
            processed = numeric_value
        else:
            processed = value

        if isinstance(processed, (int, float)):
            if lower_attr in {"duration", "onduration", "offduration", "repeat", "timeoffset"}:
                processed = round(processed)
            elif lower_attr in {
                "power",
                "powerlow",
                "powerhigh",
                "onpower",
                "offpower",
                "poweronhigh",
                "poweronlow",
                "poweroffhigh",
                "powerofflow",
            }:
                processed = round(processed, 4)

        attrs[canon] = processed

        if isinstance(processed, (int, float)):
            if lower_attr == "powerlow":
                power_range = power_range or {"low": 0.0, "high": 0.0}
                power_range["low"] = float(processed)
            elif lower_attr == "powerhigh":
                power_range = power_range or {"low": 0.0, "high": 0.0}
                power_range["high"] = float(processed)
            elif lower_attr == "poweronlow":
                power_on_range = power_on_range or {"low": 0.0, "high": 0.0}
                power_on_range["low"] = float(processed)
            elif lower_attr == "poweronhigh":
                power_on_range = power_on_range or {"low": 0.0, "high": 0.0}
                power_on_range["high"] = float(processed)
            elif lower_attr == "powerofflow":
                power_off_range = power_off_range or {"low": 0.0, "high": 0.0}
                power_off_range["low"] = float(processed)
            elif lower_attr == "poweroffhigh":
                power_off_range = power_off_range or {"low": 0.0, "high": 0.0}
                power_off_range["high"] = float(processed)

    if lower_type == "intervalst":
        for key in [
            "Power",
            "PowerLow",
            "PowerHigh",
            "PowerOnLow",
            "PowerOnHigh",
            "PowerOffLow",
            "PowerOffHigh",
        ]:
            attrs.pop(key, None)
        if power_on_range and "OnPower" not in attrs:
            attrs["OnPower"] = round((power_on_range["low"] + power_on_range["high"]) / 2, 4)
        if power_off_range and "OffPower" not in attrs:
            attrs["OffPower"] = round((power_off_range["low"] + power_off_range["high"]) / 2, 4)
    elif lower_type in {"warmup", "cooldown"}:
        for key in ["OnPower", "OffPower", "PowerOnLow", "PowerOnHigh", "PowerOffLow", "PowerOffHigh"]:
            attrs.pop(key, None)
        if power_range and "Power" not in attrs:
            attrs["Power"] = round((power_range["low"] + power_range["high"]) / 2, 4)
    else:
        for key in [
            "OnPower",
            "OffPower",
            "PowerOnLow",
            "PowerOnHigh",
            "PowerOffLow",
            "PowerOffHigh",
            "PowerLow",
            "PowerHigh",
        ]:
            attrs.pop(key, None)
        if power_range and "Power" not in attrs:
            attrs["Power"] = round((power_range["low"] + power_range["high"]) / 2, 4)

    for key in ["Duration", "OnDuration", "OffDuration", "Repeat"]:
        if key in attrs and isinstance(attrs[key], (int, float)):
            attrs[key] = int(round(attrs[key]))

    for key in ["Power", "PowerLow", "PowerHigh", "OnPower", "OffPower"]:
        value = attrs.get(key)
        if isinstance(value, (int, float)):
            attrs[key] = round(float(value), 4)
            if not (0 <= attrs[key] <= MAX_POWER):
                raise ValueError(f"Invalid {key} value")

    duration = attrs.get("Duration")
    if isinstance(duration, (int, float)):
        duration = int(duration)
        if duration < 0 or duration > MAX_DURATION:
            raise ValueError("Invalid duration value")
        attrs["Duration"] = duration

    text_events: List[Dict[str, Any]] = []
    for child in element:
        if child.tag.lower() != "textevent":
            continue
        message = child.attrib.get("Message") or child.attrib.get("message", "")
        message = message.strip() if isinstance(message, str) else ""
        if not message:
            continue
        time_offset = _try_parse_number(child.attrib.get("TimeOffset") or child.attrib.get("timeoffset"))
        loc_index = _try_parse_number(child.attrib.get("LocIndex") or child.attrib.get("locindex"))
        text_events.append(
            {
                "message": message,
                "timeOffset": _round_if_not_none(time_offset) or 0,
                "locIndex": _round_if_not_none(loc_index) or 0,
            }
        )

    if len(text_events) > MAX_TEXT_EVENTS_PER_SEGMENT:
        raise ValueError("Too many text events in segment")
    for event in text_events:
        if len(event["message"]) > MAX_TEXT_EVENT_LENGTH:
            raise ValueError("Text event message too long")

    segment: Dict[str, Any] = {"type": _normalize_segment_type(lower_type), "attributes": attrs}
    if text_events:
        segment["textEvents"] = text_events
    return segment


def parse_zwo_workout(xml_text: str) -> Dict[str, Any]:
    _basic_xml_safety_checks(xml_text)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:  # noqa: B904
        raise ValueError(f"XML parse error: {exc}") from exc

    if root.tag.lower() != "workout_file":
        raise ValueError("workout_file root not found")

    workout: List[Dict[str, Any]] = []
    result: Dict[str, Any] = {}

    for node in root:
        key = node.tag.lower()
        if key == "workout":
            if workout:
                continue
            for segment_element in node:
                segment = _sanitize_segment(segment_element)
                if segment:
                    workout.append(segment)
            continue

        if key not in ROOT_FIELD_MAP:
            continue
        target = ROOT_FIELD_MAP[key]
        value = (node.text or "").strip()
        result[target] = value

    if not workout:
        raise ValueError("Workout must contain at least one segment")
    if len(workout) > MAX_SEGMENTS:
        raise ValueError("Too many workout segments")

    for field in STRING_FIELDS:
        value = result.get(field, "")
        if value is None:
            value = ""
        if field == "description" and len(value) > MAX_DESCRIPTION_LENGTH:
            raise ValueError("Description too long")
        if field != "description" and len(value) > MAX_NAME_LENGTH:
            raise ValueError(f"{field} too long")
        result[field] = value

    result.setdefault("uniqueId", "")
    result.setdefault("legacyIdHash", "")
    result["workout"] = workout
    return result


def _get_zone(zone_id: int) -> Optional[Dict[str, Any]]:
    for zone in POWER_ZONES:
        if zone["id"] == zone_id:
            return zone
    return None


def _average_power_for_zone(zone_id: int) -> float:
    zone = _get_zone(zone_id)
    if not zone:
        return 0.0
    maximum = zone["max"] if math.isfinite(zone["max"]) else 1.75
    return (zone["min"] + maximum) / 2.0


def zwo_to_chart_data(workout: Dict[str, Any]) -> List[Dict[str, Any]]:
    segments: Iterable[Dict[str, Any]] = workout.get("workout", [])
    result: List[Dict[str, Any]] = []
    current_time = 0

    def push_segment(duration: int, power: float, seg_type: str, power_low: Optional[float] = None, power_high: Optional[float] = None) -> None:
        nonlocal current_time
        duration = int(round(duration))
        if duration <= 0:
            return
        entry: Dict[str, Any] = {
            "start": current_time,
            "duration": duration,
            "power": int(round(power * 100)),
            "type": seg_type,
        }
        if power_low is not None:
            entry["powerLow"] = int(round(power_low * 100))
        if power_high is not None:
            entry["powerHigh"] = int(round(power_high * 100))
        result.append(entry)
        current_time += duration

    for segment in segments:
        if not isinstance(segment, dict):
            continue
        seg_type = segment.get("type")
        attrs = segment.get("attributes", {})
        if not seg_type or not isinstance(attrs, dict):
            continue

        lower = str(seg_type).lower()
        zone_id = attrs.get("Zone")
        try:
            zone_int = int(zone_id)
        except (TypeError, ValueError):
            zone_int = None

        if lower == "steadystate":
            duration = attrs.get("Duration", 0)
            power = attrs.get("Power", 0.0) or 0.0
            if zone_int and not power:
                power = _average_power_for_zone(zone_int)
            push_segment(duration, float(power), seg_type)
        elif lower == "maxeffort":
            duration = attrs.get("Duration", 0)
            power = attrs.get("Power", 0.0) or 2.0
            if zone_int and abs(power - 1.5) < 1e-6:
                power = _average_power_for_zone(zone_int)
            push_segment(duration, float(power), seg_type)
        elif lower in {"warmup", "cooldown", "ramp"}:
            duration = attrs.get("Duration", 0)
            power_low = attrs.get("PowerLow")
            power_high = attrs.get("PowerHigh")
            if zone_int and not power_low and not power_high:
                avg = _average_power_for_zone(zone_int)
                push_segment(duration, avg, "SteadyState" if lower == "ramp" else seg_type)
                continue
            power_low = float(power_low or attrs.get("Power") or 0.0)
            power_high = float(power_high or attrs.get("Power") or power_low or 0.0)
            if lower == "ramp":
                push_segment(duration, power_high, seg_type, power_low, power_high)
            else:
                push_segment(duration, (power_low + power_high) / 2.0, seg_type, power_low, power_high)
        else:
            duration = attrs.get("Duration", 0)
            power = attrs.get("Power", 0.0) or 0.0
            push_segment(duration, float(power), seg_type)

    return result


def _segment_has_power(attrs: Dict[str, Any]) -> bool:
    return any(
        attrs.get(key, 0) not in (0, None)
        for key in ("Power", "PowerLow", "PowerHigh", "OnPower", "OffPower")
    )


def _segment_has_cadence(attrs: Dict[str, Any]) -> bool:
    return any(
        attrs.get(key) not in (0, None)
        for key in ("Cadence", "CadenceHigh", "CadenceLow", "CadenceResting")
    )


def _generate_segment_seconds(segment: Dict[str, Any], ftp: float) -> List[float]:
    attrs = segment.get("attributes", {})
    seg_type = str(segment.get("type", "")).lower()
    seconds: List[float] = []

    def extend_constant(duration: int, power_ratio: float) -> None:
        for _ in range(max(duration, 0)):
            seconds.append(power_ratio * ftp)

    if seg_type in {"warmup", "cooldown"}:
        duration = int(round(attrs.get("Duration", 0) or 0))
        low = float(attrs.get("PowerLow") or 0.0) * ftp
        high = float(attrs.get("PowerHigh") or attrs.get("Power") or 0.0) * ftp
        if duration <= 1:
            extend_constant(duration, ((attrs.get("PowerLow") or 0.0) + (attrs.get("PowerHigh") or 0.0)) / 2)
        else:
            for i in range(duration):
                progress = i / max(duration - 1, 1)
                value = low + (high - low) * progress
                seconds.append(value)
    elif seg_type in {"steadystate", "freeride", "maxeffort"}:
        duration = int(round(attrs.get("Duration", 0) or 0))
        extend_constant(duration, float(attrs.get("Power") or 0.0))
    elif seg_type == "ramp":
        duration = int(round(attrs.get("Duration", 0) or 0))
        low = float(attrs.get("PowerLow") or attrs.get("Power") or 0.0) * ftp
        high = float(attrs.get("PowerHigh") or attrs.get("Power") or 0.0) * ftp
        for i in range(duration):
            progress = i / max(duration - 1, 1)
            seconds.append(low + (high - low) * progress)
    elif seg_type == "intervalst":
        repeat = int(round(attrs.get("Repeat", 1) or 1))
        on_duration = int(round(attrs.get("OnDuration", 0) or 0))
        off_duration = int(round(attrs.get("OffDuration", 0) or 0))
        on_power = float(attrs.get("OnPower") or 0.0) * ftp
        off_power = float(attrs.get("OffPower") or 0.0) * ftp
        for _ in range(max(repeat, 0)):
            for _ in range(max(on_duration, 0)):
                seconds.append(on_power)
            for _ in range(max(off_duration, 0)):
                seconds.append(off_power)
    else:
        duration = int(round(attrs.get("Duration", 0) or 0))
        extend_constant(duration, float(attrs.get("Power") or 0.0))

    return seconds


def _calculate_normalized_power(second_by_second: List[float]) -> float:
    if not second_by_second:
        return 0.0
    if len(second_by_second) < 30:
        return sum(second_by_second) / len(second_by_second)
    rolling: List[float] = []
    window_sum = sum(second_by_second[:30])
    rolling.append(window_sum / 30.0)
    for idx in range(30, len(second_by_second)):
        window_sum += second_by_second[idx] - second_by_second[idx - 30]
        rolling.append(window_sum / 30.0)
    if not rolling:
        return 0.0
    fourth_power_sum = sum(value ** 4 for value in rolling)
    average_fourth = fourth_power_sum / len(rolling)
    return average_fourth ** 0.25


def _calculate_tss(normalized_power: float, ftp: float, duration: int) -> float:
    if duration <= 0 or ftp <= 0:
        return 0.0
    intensity_factor = normalized_power / ftp
    raw_tss = (duration * normalized_power * intensity_factor) / (ftp * 3600.0) * 100.0
    return max(0.0, raw_tss)


def _calculate_zone_breakdown(second_by_second: List[float], ftp: float) -> Dict[str, int]:
    zones = {f"zone{zone['id']}": 0 for zone in POWER_ZONES}
    for power in second_by_second:
        intensity = power / ftp if ftp else 0.0
        zone_id = POWER_ZONES[-1]["id"]
        for zone in POWER_ZONES:
            if intensity >= zone["min"] and intensity < zone["max"]:
                zone_id = zone["id"]
                break
        zones[f"zone{zone_id}"] += 1
    return zones


def calculate_workout_metrics(workout: Dict[str, Any], ftp: Optional[float] = None) -> Dict[str, Any]:
    segments: List[Dict[str, Any]] = list(workout.get("workout", []))
    total_duration = 0
    has_power = False
    has_cadence = False

    for segment in segments:
        if not isinstance(segment, dict):
            continue
        attrs = segment.get("attributes", {})
        if not isinstance(attrs, dict):
            continue
        duration = int(round(attrs.get("Duration", 0) or 0))
        seg_type = str(segment.get("type", "")).lower()
        if seg_type == "intervalst":
            repeat = int(round(attrs.get("Repeat", 1) or 1))
            on_duration = int(round(attrs.get("OnDuration", 0) or 0))
            off_duration = int(round(attrs.get("OffDuration", 0) or 0))
            duration = (on_duration + off_duration) * max(repeat, 0)
        total_duration += max(duration, 0)
        has_power = has_power or _segment_has_power(attrs)
        has_cadence = has_cadence or _segment_has_cadence(attrs)

    metrics: Dict[str, Any] = {
        "totalDuration": int(round(total_duration)),
        "totalWorkSegments": len(segments),
        "hasPowerData": bool(has_power),
        "hasCadenceData": bool(has_cadence),
    }

    if ftp and ftp > 0 and has_power:
        seconds: List[float] = []
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            seconds.extend(_generate_segment_seconds(segment, ftp))
        if seconds:
            average_power = sum(seconds) / len(seconds)
            normalized_power = _calculate_normalized_power(seconds)
            variability_index = normalized_power / average_power if average_power else 1.0
            intensity_factor = normalized_power / ftp if ftp else 0.0
            tss = _calculate_tss(normalized_power, ftp, metrics["totalDuration"])
            total_kj = sum(seconds) / 1000.0
            metrics.update(
                {
                    "averagePower": int(round(average_power)) or None,
                    "normalizedPower": int(round(normalized_power)) or None,
                    "intensityFactor": round(intensity_factor, 2) or None,
                    "trainingStressScore": int(round(tss)) or None,
                    "variabilityIndex": round(variability_index, 2) or None,
                    "totalKj": int(round(total_kj)) or None,
                    "intensityZoneBreakdown": _calculate_zone_breakdown(seconds, ftp),
                }
            )

    return metrics


def build_workout_payload(
    workout: Dict[str, Any],
    chart_data: List[Dict[str, Any]],
    metrics: Dict[str, Any],
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    overrides = overrides or {}
    payload: Dict[str, Any] = {
        "name": overrides.get("name") or workout.get("name") or "Новая тренировка",
        "description": overrides.get("description") or workout.get("description") or "",
        "author": overrides.get("author") or workout.get("author") or "",
        "sportType": overrides.get("sportType") or workout.get("sportType") or "bike",
        "category": overrides.get("category") or workout.get("category") or "",
        "subcategory": overrides.get("subcategory") or workout.get("subcategory") or "",
        "uniqueId": str(overrides.get("uniqueId") or workout.get("uniqueId") or ""),
        "legacyIdHash": str(overrides.get("legacyIdHash") or workout.get("legacyIdHash") or ""),
        "workout": workout.get("workout", []),
        "chartData": chart_data,
    }
    payload.update(metrics)
    return payload


__all__ = [
    "POWER_ZONES",
    "parse_zwo_workout",
    "zwo_to_chart_data",
    "calculate_workout_metrics",
    "build_workout_payload",
]
