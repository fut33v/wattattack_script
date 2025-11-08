#!/usr/bin/env python3
"""Import legacy schedule XLSX workbook into the database schedule tables."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import zipfile
import xml.etree.ElementTree as ET

import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from repositories import (
    client_repository,
    instructors_repository,
    schedule_repository,
    trainers_repository,
)


BASE_DATE = datetime(1899, 12, 30)
NS_MAIN = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
NS_REL = {"r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}

def _normalize_yo(value: str) -> str:
    return value.replace("ё", "е").replace("Ё", "Е")


_CLIENT_ALIAS_ENTRIES: Dict[str, Optional[str]] = {
    "занято": None,
    "busy": None,
    "самокрутка": None,
    "бычковская": "наталья бычковская",
    "бычковская (педали лук)": "наталья бычковская",
    "наталья бычковская": "наталья бычковская",
    "самкова": "катя самкова",
    "самкова (кроссовки)": "катя самкова",
    "самкова катя": "катя самкова",
    "самкова екатерина": "катя самкова",
    "екатерина самкова": "катя самкова",
    "катя самкова": "катя самкова",
    "женя лукин": "евген лукин",
    "лукин женя": "евген лукин",
    "евгений лукин": "евген лукин",
    "лукин евгений": "евген лукин",
    "лера говер": "валерия говер",
    "валерия говер": "валерия говер",
    "дима говер": "дмитрий говер",
    "дмитрий говер": "дмитрий говер",
    "лора палмер": "алиса гаджиева",
    "палмер лора": "алиса гаджиева",
    "алиса гаджиева": "алиса гаджиева",
    "юлия филлипова": "юлия филиппова",
    "любовь петрова": "люба петрова",
    "люба петрова": "люба петрова",
    "петрова любовь": "люба петрова",
    "люба": "люба петрова",
    "ольга (фтп тест)": "ольга кубарева",
    "ольга кубарева": "ольга кубарева",
}

CLIENT_ALIASES: Dict[str, Optional[str]] = {
    _normalize_yo(key.strip().lower()): value for key, value in _CLIENT_ALIAS_ENTRIES.items()
}

BLOCKED_LABELS = {"занято", "busy"}

INSTRUCTOR_ALIAS_ENTRIES: Dict[str, str] = {
    "евгений балакин": "евгений балакин",
    "балакин евгений": "евгений балакин",
    "балакин": "евгений балакин",
    "евгений балакан": "евгений балакин",
    "константин гаврилов": "константин гаврилов",
    "гаврилов константин": "константин гаврилов",
    "костя гаврилов": "константин гаврилов",
    "наталья раскина": "наталья раскина",
    "раскина наталья": "наталья раскина",
}


@dataclass
class SlotReservation:
    stand_label: str
    client_text: str
    is_blocked: bool = False


@dataclass
class SlotEntry:
    slot_date: date
    time_label: str
    label: Optional[str]
    reservations: List[SlotReservation]


@dataclass
class WeekSheet:
    sheet_name: str
    week_start: date
    slots: List[SlotEntry]


@dataclass
class WeekImportSummary:
    week_start: date
    sheet: str
    created_slots: int
    reservations: int
    unmatched_clients: Counter[str]
    unmatched_stands: Counter[str]
    unmatched_instructors: Counter[str]


@dataclass
class ImportOutcome:
    summaries: List[WeekImportSummary]
    totals_clients: Counter[str]
    totals_stands: Counter[str]
    totals_instructors: Counter[str]
    replace_existing: bool
    dry_run: bool


def load_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: List[str] = []
    for si in root.findall("m:si", NS_MAIN):
        text = "".join(
            t.text or ""
            for t in si.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
        )
        strings.append(text)
    return strings


def workbook_sheets(zf: zipfile.ZipFile) -> List[Tuple[str, str]]:
    workbook_xml = ET.fromstring(zf.read("xl/workbook.xml"))
    rels_xml = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels_xml}

    sheets: List[Tuple[str, str]] = []
    for sheet in workbook_xml.findall("m:sheets/m:sheet", NS_MAIN):
        rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        target = rel_map.get(rel_id)
        if not target:
            continue
        sheets.append((sheet.attrib.get("name", f"sheet-{len(sheets)+1}"), f"xl/{target}"))
    return sheets


def _extract_week_sheets(zf: zipfile.ZipFile) -> List[WeekSheet]:
    shared_strings = load_shared_strings(zf)
    sheets = workbook_sheets(zf)

    week_sheets: List[WeekSheet] = []
    for sheet_name, sheet_path in sheets:
        if sheet_path not in zf.namelist():
            continue
        slot_entries = parse_sheet(zf, sheet_path, shared_strings)
        if not slot_entries:
            continue
        min_date = min(entry.slot_date for entry in slot_entries)
        week_start = min_date - timedelta(days=min_date.weekday())
        week_sheets.append(WeekSheet(sheet_name=sheet_name, week_start=week_start, slots=slot_entries))
    return week_sheets


def load_workbook_from_bytes(data: bytes) -> List[WeekSheet]:
    with zipfile.ZipFile(BytesIO(data)) as zf:
        return _extract_week_sheets(zf)


def excel_cell_to_indices(ref: str) -> Tuple[int, int]:
    col = 0
    row_chars: List[str] = []
    for ch in ref:
        if ch.isdigit():
            row_chars.append(ch)
        else:
            col = col * 26 + (ord(ch) - ord("A") + 1)
    row = int("".join(row_chars))
    return row, col


def try_parse_excel_date(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        number = float(value)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
    else:
        return (BASE_DATE + timedelta(days=number)).date()


def parse_sheet(zf: zipfile.ZipFile, sheet_path: str, shared_strings: Sequence[str]) -> List[SlotEntry]:
    sheet_root = ET.fromstring(zf.read(sheet_path))

    data: Dict[Tuple[int, int], str] = {}
    for cell in sheet_root.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"):
        ref = cell.attrib["r"]
        row, col = excel_cell_to_indices(ref)
        value_el = cell.find("m:v", NS_MAIN)
        if value_el is None or value_el.text is None:
            continue
        value = value_el.text
        if cell.attrib.get("t") == "s":
            index = int(value)
            if 0 <= index < len(shared_strings):
                value = shared_strings[index]
        data[(row, col)] = value

    col_to_date: Dict[int, date] = {}
    time_rows: Dict[int, str] = {}
    for (row, col), value in data.items():
        if row == 1:
            parsed_date = try_parse_excel_date(value)
            if parsed_date:
                col_to_date[col] = parsed_date
        if col == 1 and isinstance(value, str) and ":" in value and "-" in value:
            time_rows[row] = value.strip()

    if not col_to_date:
        return []

    rows = sorted({row for row, _ in data.keys()})
    slots: Dict[Tuple[date, str], SlotEntry] = {}
    current_time_label: Optional[str] = None

    max_col = max(col for _, col in data.keys())

    for row in rows:
        if row in time_rows:
            current_time_label = time_rows[row]
            continue
        if current_time_label is None:
            continue

        column1_value = data.get((row, 1))
        column1_trimmed = column1_value.strip() if isinstance(column1_value, str) else ""
        is_self_service_block = bool(column1_trimmed) and column1_trimmed.lower().startswith("самокрутка")

        stand_label: Optional[str] = None
        for col_idx in range(1, max_col + 1):
            candidate = data.get((row, col_idx))
            if isinstance(candidate, str) and candidate.strip().lower().startswith("станок"):
                stand_label = candidate.strip()
                break

        if stand_label:
            for col_idx, slot_date in col_to_date.items():
                text = data.get((row, col_idx))
                if not isinstance(text, str):
                    continue
                trimmed = text.strip()
                trimmed_lower = trimmed.lower()
                if not trimmed or trimmed_lower.startswith("станок"):
                    continue
                if trimmed_lower in {"самокрутка", "self"}:
                    continue
                is_blocked = trimmed_lower in BLOCKED_LABELS
                slot_key = (slot_date, current_time_label)
                entry = slots.setdefault(
                    slot_key,
                    SlotEntry(slot_date=slot_date, time_label=current_time_label, label=None, reservations=[]),
                )
                entry.reservations.append(
                    SlotReservation(stand_label=stand_label, client_text=trimmed, is_blocked=is_blocked)
                )
            continue

        if is_self_service_block:
            for col_idx, slot_date in col_to_date.items():
                slot_key = (slot_date, current_time_label)
                entry = slots.setdefault(
                    slot_key,
                    SlotEntry(slot_date=slot_date, time_label=current_time_label, label=None, reservations=[]),
                )
                if not entry.label:
                    entry.label = column1_trimmed

        for col_idx, slot_date in col_to_date.items():
            text = data.get((row, col_idx))
            if not isinstance(text, str):
                continue
            trimmed = text.strip()
            if not trimmed or trimmed.lower().startswith("станок"):
                continue
            if trimmed and trimmed.lower().startswith("самокрутка"):
                slot_key = (slot_date, current_time_label)
                entry = slots.setdefault(
                    slot_key,
                    SlotEntry(slot_date=slot_date, time_label=current_time_label, label=None, reservations=[]),
                )
                entry.label = trimmed
                continue
            slot_key = (slot_date, current_time_label)
            entry = slots.setdefault(
                slot_key,
                SlotEntry(slot_date=slot_date, time_label=current_time_label, label=None, reservations=[]),
            )
            if entry.label:
                if trimmed not in entry.label:
                    entry.label = f"{entry.label}; {trimmed}"
            else:
                entry.label = trimmed

    ordered = sorted(slots.values(), key=lambda s: (s.slot_date, parse_time_range(s.time_label)[0]))
    return ordered


def parse_time_range(label: str) -> Tuple[time, time]:
    raw = label.replace(" ", "")
    if "-" not in raw:
        raise ValueError(f"Неверный формат времени: {label!r}")
    start_raw, end_raw = raw.split("-", 1)
    start_time = datetime.strptime(start_raw, "%H:%M").time()
    end_time = datetime.strptime(end_raw, "%H:%M").time()
    return start_time, end_time


def load_client_lookup() -> Dict[str, List[Tuple[int, str]]]:
    lookup: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    batch_size = 500
    offset = 0
    while True:
        batch = client_repository.list_clients(limit=batch_size, offset=offset)
        if not batch:
            break
        for client in batch:
            full_name = client.get("full_name") or ""
            if not full_name and (client.get("first_name") or client.get("last_name")):
                full_name = f"{client.get('first_name', '').strip()} {client.get('last_name', '').strip()}".strip()
            if not full_name:
                continue
            normalized = normalize_client_name(full_name)
            if normalized:
                lookup[normalized].append((client["id"], full_name.strip()))
            tokens = normalized.split()
            if len(tokens) >= 2:
                reversed_key = " ".join(reversed(tokens))
                lookup[reversed_key].append((client["id"], full_name.strip()))
                rotated = " ".join(tokens[1:] + tokens[:1])
                lookup[rotated].append((client["id"], full_name.strip()))
        if len(batch) < batch_size:
            break
        offset += batch_size
    return lookup


def load_stand_lookup() -> Dict[str, int]:
    trainers_repository.ensure_trainers_table()
    stands = trainers_repository.list_trainers()
    mapping: Dict[str, int] = {}
    for stand in stands:
        for value in (
            stand.get("code"),
            stand.get("display_name"),
            stand.get("title"),
        ):
            if value:
                mapping[normalize_stand_label(value)] = stand["id"]
    return mapping


def load_instructor_lookup() -> Dict[str, Tuple[int, str]]:
    instructors_repository.ensure_instructors_table()
    instructors = instructors_repository.list_instructors()
    mapping: Dict[str, Tuple[int, str]] = {}
    for instructor in instructors:
        full_name = instructor.get("full_name")
        if not full_name:
            continue
        normalized = resolve_instructor_key(full_name)
        if normalized:
            mapping[normalized] = (instructor["id"], full_name)
    return mapping


def normalize_client_name(name: str) -> str:
    return " ".join(_normalize_yo(name.strip().lower()).split())


def normalize_stand_label(label: str) -> str:
    return label.strip().lower().replace(" ", "")


def normalize_instructor_name(name: str) -> str:
    cleaned = re.sub(r"[^\w\s]+", " ", name, flags=re.UNICODE)
    return " ".join(_normalize_yo(cleaned.strip().lower()).split())


def resolve_instructor_key(name: str) -> str:
    normalized = normalize_instructor_name(name)
    if not normalized:
        return ""
    target = INSTRUCTOR_ALIASES.get(normalized)
    if target:
        return normalize_instructor_name(target)
    return normalized


INSTRUCTOR_ALIASES: Dict[str, str] = {
    _normalize_yo(key.strip().lower()): normalize_instructor_name(value)
    for key, value in INSTRUCTOR_ALIAS_ENTRIES.items()
}


def extract_client_key(text: str) -> str:
    base = text.split("(")[0].strip()
    return normalize_client_name(base)


def resolve_client(
    client_text: str,
    lookup: Dict[str, List[Tuple[int, str]]],
) -> Tuple[Optional[int], Optional[str]]:
    raw = _normalize_yo(client_text.strip().lower())
    alias_target = CLIENT_ALIASES.get(raw)
    if raw in CLIENT_ALIASES and alias_target is None:
        return None, None

    if alias_target:
        normalized = normalize_client_name(alias_target)
    else:
        normalized = extract_client_key(client_text)
        alias_from_normalized = CLIENT_ALIASES.get(normalized)
        if normalized in CLIENT_ALIASES and alias_from_normalized is None:
            return None, None
        if alias_from_normalized:
            normalized = normalize_client_name(alias_from_normalized)

    if not normalized:
        return None, None
    candidates = [normalized]
    tokens = normalized.split()
    if len(tokens) >= 2:
        candidates.append(" ".join(reversed(tokens)))
        candidates.append(" ".join(tokens[1:] + tokens[:1]))
        candidates.append(" ".join([tokens[-1]] + tokens[:-1]))
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        matches = lookup.get(candidate)
        if matches:
            return matches[0]
    return None, None


def detect_instructor_from_label(
    label: str,
    lookup: Dict[str, Tuple[int, str]],
) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    normalized = resolve_instructor_key(label or "")
    if not normalized:
        return None, label, None
    match = lookup.get(normalized)
    if match:
        instructor_id, display_name = match
        return instructor_id, None, display_name
    for key, (instructor_id, display_name) in lookup.items():
        if key and key in normalized:
            return instructor_id, label, display_name
    return None, label, None


def ensure_week_title(week_id: int, title: str) -> None:
    existing = schedule_repository.get_week(week_id)
    if not existing:
        return
    current_title = existing.get("title")
    if current_title == title:
        return
    schedule_repository.update_week(week_id, title=title)


def import_schedule(
    *,
    week: WeekSheet,
    client_lookup: Dict[str, List[Tuple[int, str]]],
    stand_lookup: Dict[str, int],
    instructor_lookup: Dict[str, Tuple[int, str]],
    replace: bool,
    dry_run: bool,
) -> WeekImportSummary:
    schedule_repository.ensure_schedule_tables()
    week_record = schedule_repository.get_or_create_week(
        week_start_date=week.week_start,
        title=week.sheet_name,
    )
    ensure_week_title(week_record["id"], week.sheet_name)

    if replace and not dry_run:
        schedule_repository.clear_week_slots(week_record["id"])

    unmatched_clients: Counter[str] = Counter()
    unmatched_stands: Counter[str] = Counter()
    unmatched_instructors: Counter[str] = Counter()
    created_slots = 0
    populated_reservations = 0

    for slot_entry in week.slots:
        start_time, end_time = parse_time_range(slot_entry.time_label)

        session_kind = "self_service"
        original_label = slot_entry.label.strip() if slot_entry.label else None
        label = original_label
        instructor_id: Optional[int] = None
        if label:
            lower_label = label.lower()
            if lower_label.startswith("самокрутка"):
                session_kind = "self_service"
                label = None
            else:
                session_kind = "instructor"
                instructor_id, label, _ = detect_instructor_from_label(label, instructor_lookup)
                if instructor_id is None and original_label:
                    unmatched_instructors[original_label] += 1

        if dry_run:
            created_slots += 1
            for reservation in slot_entry.reservations:
                raw_client = reservation.client_text.strip().lower()
                normalized_raw = _normalize_yo(raw_client)
                alias_is_blocked = normalized_raw in CLIENT_ALIASES and CLIENT_ALIASES[normalized_raw] is None
                if reservation.is_blocked or alias_is_blocked:
                    continue
                normalized_stand = normalize_stand_label(reservation.stand_label)
                if normalized_stand not in stand_lookup:
                    unmatched_stands[reservation.stand_label.strip()] += 1
                client_id, client_display = resolve_client(reservation.client_text, client_lookup)
                if not client_id and reservation.client_text.strip():
                    unmatched_clients[reservation.client_text.strip()] += 1
            continue

        slot_row = schedule_repository.create_slot(
            week_id=week_record["id"],
            slot_date=slot_entry.slot_date,
            start_time=start_time,
            end_time=end_time,
            label=label,
            session_kind=session_kind,
            instructor_id=instructor_id,
        )
        created_slots += 1

        for reservation in slot_entry.reservations:
            client_text = reservation.client_text.strip()
            raw_client = _normalize_yo(client_text.lower())
            alias_value = CLIENT_ALIASES.get(raw_client)
            alias_is_blocked = raw_client in CLIENT_ALIASES and alias_value is None
            is_blocked = reservation.is_blocked or alias_is_blocked

            client_id = None
            client_display = None
            if not is_blocked and client_text:
                client_id, client_display = resolve_client(client_text, client_lookup)
                if not client_id:
                    unmatched_clients[client_text] += 1

            normalized_stand = normalize_stand_label(reservation.stand_label)
            stand_id = stand_lookup.get(normalized_stand)
            if stand_id is None:
                unmatched_stands[reservation.stand_label] += 1

            placeholder = (
                schedule_repository.get_reservation_for_stand(slot_row["id"], stand_id)
                if stand_id is not None
                else None
            )
            payload = {
                "client_id": client_id if not is_blocked else None,
                "client_name": (client_display or client_text or "Занято") if not is_blocked else (client_text or "Занято"),
                "status": "blocked" if is_blocked else "booked",
                "source": "import-xlsx",
            }
            if placeholder:
                schedule_repository.update_reservation(placeholder["id"], **payload)
            else:
                schedule_repository.create_reservation(
                    slot_id=slot_row["id"],
                    stand_id=stand_id,
                    stand_code=reservation.stand_label,
                    client_id=client_id,
                    client_name=client_display or client_text,
                    status="booked",
                    source="import-xlsx",
                )
            populated_reservations += 1

    if not dry_run:
        schedule_repository.sync_week_capacity(week_record["id"])

    return WeekImportSummary(
        week_start=week.week_start,
        sheet=week.sheet_name,
        created_slots=created_slots,
        reservations=populated_reservations,
        unmatched_clients=unmatched_clients,
        unmatched_stands=unmatched_stands,
        unmatched_instructors=unmatched_instructors,
    )


def run_schedule_import(
    weeks: List[WeekSheet],
    *,
    keep_existing: bool,
    dry_run: bool,
) -> ImportOutcome:
    if not weeks:
        raise ValueError("Нет подходящих недель для импорта.")

    client_lookup = load_client_lookup()
    stand_lookup = load_stand_lookup()
    instructor_lookup = load_instructor_lookup()
    replace = not keep_existing

    summaries: List[WeekImportSummary] = []
    totals_clients: Counter[str] = Counter()
    totals_stands: Counter[str] = Counter()
    totals_instructors: Counter[str] = Counter()

    for week in weeks:
        summary = import_schedule(
            week=week,
            client_lookup=client_lookup,
            stand_lookup=stand_lookup,
            instructor_lookup=instructor_lookup,
            replace=replace,
            dry_run=dry_run,
        )
        summaries.append(summary)
        totals_clients.update(summary.unmatched_clients)
        totals_stands.update(summary.unmatched_stands)
        totals_instructors.update(summary.unmatched_instructors)

    return ImportOutcome(
        summaries=summaries,
        totals_clients=totals_clients,
        totals_stands=totals_stands,
        totals_instructors=totals_instructors,
        replace_existing=replace,
        dry_run=dry_run,
    )


def _filter_weeks(
    weeks: List[WeekSheet],
    week_filters: Optional[Iterable[date]] = None,
    sheet_filters: Optional[Iterable[str]] = None,
) -> List[WeekSheet]:
    filtered = weeks
    if week_filters:
        desired = set(week_filters)
        filtered = [week for week in filtered if week.week_start in desired]
    if sheet_filters:
        wanted = {name.lower() for name in sheet_filters}
        filtered = [week for week in filtered if week.sheet_name.lower() in wanted]
    return sorted(filtered, key=lambda w: w.week_start)


def run_schedule_import_from_bytes(
    data: bytes,
    *,
    keep_existing: bool,
    dry_run: bool,
    week_filters: Optional[Iterable[date]] = None,
    sheet_filters: Optional[Iterable[str]] = None,
) -> ImportOutcome:
    weeks = load_workbook_from_bytes(data)
    if not weeks:
        raise ValueError("Не удалось извлечь расписание из файла.")
    weeks = _filter_weeks(weeks, week_filters, sheet_filters)
    if not weeks:
        raise ValueError("Нет подходящих недель для импорта (проверьте фильтр).")
    return run_schedule_import(weeks, keep_existing=keep_existing, dry_run=dry_run)


def _format_counter_section(
    lines: List[str],
    counter: Counter[str],
    title: str,
) -> None:
    if not counter:
        return
    lines.append(f"  {title}:")
    for name, count in counter.most_common():
        lines.append(f"    · {name} × {count}")


def format_import_report(outcome: ImportOutcome) -> str:
    replace_text = "yes" if outcome.replace_existing else "no"
    lines: List[str] = [
        f"Найдено недель: {len(outcome.summaries)} (replace={replace_text}, dry_run={outcome.dry_run})"
    ]

    for summary in outcome.summaries:
        lines.append(
            f"- {summary.week_start.isoformat()} (лист '{summary.sheet}'): "
            f"слотов={summary.created_slots} назначений={summary.reservations}"
        )
        _format_counter_section(lines, summary.unmatched_clients, "Клиенты без совпадений")
        _format_counter_section(lines, summary.unmatched_stands, "Не сопоставлены станки")
        _format_counter_section(
            lines,
            summary.unmatched_instructors,
            "Не сопоставлены инструкторы",
        )

    if outcome.totals_clients:
        lines.append("\nКлиенты без совпадений (итого):")
        for name, count in outcome.totals_clients.most_common():
            lines.append(f"  · {name} × {count}")
    if outcome.totals_stands:
        lines.append("\nНе сопоставлены станки (итого):")
        for name, count in outcome.totals_stands.most_common():
            lines.append(f"  · {name} × {count}")
    if outcome.totals_instructors:
        lines.append("\nИнструкторы без совпадений (итого):")
        for name, count in outcome.totals_instructors.most_common():
            lines.append(f"  · {name} × {count}")

    if outcome.dry_run:
        lines.append("\nDRY RUN завершён. В базу изменения не вносились.")

    return "\n".join(lines)


def load_workbook(path: Path) -> List[WeekSheet]:
    return load_workbook_from_bytes(path.read_bytes())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--file",
        default="data/СЕТКА РАСПИСАНИЯ _ КРУТИЛКА.xlsx",
        type=Path,
        help="Путь к XLSX файлу расписания.",
    )
    parser.add_argument(
        "--week-start",
        action="append",
        help="Импортировать только указанную дату понедельника (YYYY-MM-DD). Можно указать несколько.",
    )
    parser.add_argument(
        "--sheet",
        action="append",
        help="Импортировать только листы с указанными именами.",
    )
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Не очищать существующие недели перед импортом (по умолчанию заменяем).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только показать планируемые изменения, без записи в базу.",
    )
    args = parser.parse_args()

    if not args.file.exists():
        raise SystemExit(f"Файл {args.file} не найден.")

    try:
        data = args.file.read_bytes()
    except FileNotFoundError:
        raise SystemExit(f"Файл {args.file} не найден.")

    week_filters = None
    if args.week_start:
        week_filters = []
        for value in args.week_start:
            dt = datetime.strptime(value, "%Y-%m-%d").date()
            week_filters.append(dt - timedelta(days=dt.weekday()))

    sheet_filters = args.sheet if args.sheet else None

    try:
        outcome = run_schedule_import_from_bytes(
            data,
            keep_existing=args.keep_existing,
            dry_run=args.dry_run,
            week_filters=week_filters,
            sheet_filters=sheet_filters,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    report = format_import_report(outcome)
    print(report)


if __name__ == "__main__":
    main()
