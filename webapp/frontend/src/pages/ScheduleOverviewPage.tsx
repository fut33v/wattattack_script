import { useEffect, useMemo, useState, useRef } from "react";
import classNames from "classnames";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { InstructorRow, ScheduleReservation, ScheduleSlot, ScheduleWeekDetailResponse, ScheduleWeekListResponse } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

import "../styles/schedule.css";

function formatDayHeader(dateIso: string): { label: string; weekday: string } {
  const dateObj = new Date(`${dateIso}T00:00:00`);
  return {
    label: dateObj.toLocaleDateString("ru-RU", { day: "2-digit", month: "short" }),
    weekday: dateObj.toLocaleDateString("ru-RU", { weekday: "short" })
  };
}

function isoFromDate(date: Date): string {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function countOccupied(reservations: ScheduleReservation[]): { occupied: number; free: number } {
  const occupied = reservations.filter((reservation) => reservation.status !== "available").length;
  return {
    occupied,
    free: Math.max(reservations.length - occupied, 0)
  };
}

interface DayColumn {
  dateIso: string;
  label: string;
  weekday: string;
  slots: ScheduleSlot[];
  totals: { occupied: number; free: number; totalSlots: number };
}

export default function ScheduleOverviewPage() {
  const { session } = useAppContext();
  const isAdmin = session.isAdmin;
  const weeksQuery = useQuery<ScheduleWeekListResponse>({
    queryKey: ["schedule-weeks"],
    queryFn: () => apiFetch<ScheduleWeekListResponse>("/api/schedule/weeks?page=1&page_size=100")
  });

  const [selectedIndex, setSelectedIndex] = useState(0);
  const initializedRef = useRef(false);

  useEffect(() => {
    if (!weeksQuery.data) return;
    const items = weeksQuery.data.items;
    if (items.length === 0) {
      setSelectedIndex(0);
      return;
    }
    if (!initializedRef.current) {
      const today = new Date();
      const currentMonday = new Date(today);
      currentMonday.setHours(0, 0, 0, 0);
      currentMonday.setDate(today.getDate() - today.getDay() + 1); // 0 Sunday -> +1 Monday
      const currentIso = isoFromDate(currentMonday);
      const matchIdx = items.findIndex((week) => week.week_start_date === currentIso);
      setSelectedIndex(matchIdx >= 0 ? matchIdx : 0);
      initializedRef.current = true;
      return;
    }
    if (selectedIndex >= items.length) {
      setSelectedIndex(0);
    }
  }, [weeksQuery.data, selectedIndex]);

  const weeks = weeksQuery.data?.items ?? [];
  const selectedWeek = weeks[selectedIndex] ?? null;

  const weekDetailQuery = useQuery<ScheduleWeekDetailResponse>({
    queryKey: ["schedule-week-overview", selectedWeek?.id ?? 0],
    queryFn: () => apiFetch<ScheduleWeekDetailResponse>(`/api/schedule/weeks/${selectedWeek?.id}`),
    enabled: Boolean(selectedWeek?.id)
  });

  const slots = weekDetailQuery.data?.slots ?? [];
  const instructors = weekDetailQuery.data?.instructors ?? [];

  const [expandedSlots, setExpandedSlots] = useState<Record<number, boolean>>({});

  useEffect(() => {
    setExpandedSlots((prev) => {
      const next: Record<number, boolean> = {};
      slots.forEach((slot) => {
        next[slot.id] = prev[slot.id] ?? false;
      });
      return next;
    });
  }, [slots]);

  const dayColumns: DayColumn[] = useMemo(() => {
    if (!selectedWeek) return [];
    const start = new Date(`${selectedWeek.week_start_date}T00:00:00`);
    const map = new Map<string, ScheduleSlot[]>();
    slots.forEach((slot) => {
      map.set(slot.slot_date, [...(map.get(slot.slot_date) ?? []), slot]);
    });
    const result: DayColumn[] = [];
    for (let i = 0; i < 7; i += 1) {
      const dayDate = new Date(start);
      dayDate.setDate(start.getDate() + i);
      const iso = isoFromDate(dayDate);
      const slotList = (map.get(iso) ?? []).sort((a, b) => a.start_time.localeCompare(b.start_time));
      const totals = slotList.reduce(
        (acc, slot) => {
          const { occupied, free } = countOccupied(slot.reservations);
          acc.occupied += occupied;
          acc.free += free;
          return acc;
        },
        { occupied: 0, free: 0 }
      );
      result.push({
        dateIso: iso,
        label: formatDayHeader(iso).label,
        weekday: formatDayHeader(iso).weekday,
        slots: slotList,
        totals: { ...totals, totalSlots: slotList.length }
      });
    }
    return result;
  }, [slots, selectedWeek?.week_start_date]);

  function toggleSlot(slotId: number) {
    setExpandedSlots((prev) => ({
      ...prev,
      [slotId]: !prev[slotId]
    }));
  }

  const canGoPrev = selectedIndex < weeks.length - 1;
  const canGoNext = selectedIndex > 0;

  function goPrev() {
    if (canGoPrev) {
      setSelectedIndex((prev) => Math.min(prev + 1, weeks.length - 1));
    }
  }

  function goNext() {
    if (canGoNext) {
      setSelectedIndex((prev) => Math.max(prev - 1, 0));
    }
  }

  const headerExtra = (
    <div className="schedule-toolbar-actions">
      <button type="button" className="btn ghost" onClick={goPrev} disabled={!canGoPrev}>
        ← Предыдущая
      </button>
      <button type="button" className="btn ghost" onClick={goNext} disabled={!canGoNext}>
        Следующая →
      </button>
      {isAdmin ? (
        <Link to="/schedule/manage" className="btn primary">
          Открыть редактор
        </Link>
      ) : null}
    </div>
  );

  const renderReservationList = (slot: ScheduleSlot) => {
    if (slot.reservations.length === 0) {
      return <div className="schedule-reservation empty">Нет записей</div>;
    }
    return slot.reservations.map((reservation) => {
      const standLabel = reservation.stand_code || "Станок";
      const clientName = reservation.client_name ? reservation.client_name.trim() : "";
      const primaryLabel = clientName || standLabel;
      const statusLabel =
        reservation.status === "available"
          ? "Свободно"
          : reservation.status === "booked"
          ? "Занято"
          : reservation.status;
      return (
        <div key={reservation.id} className="schedule-reservation">
          <span className="schedule-reservation-stand">{primaryLabel}</span>
          <span className="schedule-reservation-name">{clientName ? standLabel : ""}</span>
          <span className={classNames("schedule-reservation-status", reservation.status)}>{statusLabel}</span>
        </div>
      );
    });
  };

  function renderSlotCard(slot: ScheduleSlot) {
    const { occupied, free } = countOccupied(slot.reservations);
    const isExpanded = expandedSlots[slot.id] ?? false;
    const instructorName =
      slot.instructorName ??
      (slot.session_kind === "instructor"
        ? instructors.find((item: InstructorRow) => item.id === slot.instructorId)?.full_name ?? null
        : null);
    const slotLabel =
      (slot.label ?? "").trim() || (slot.session_kind === "instructor" ? "С инструктором" : "Самокрутка");

    return (
      <div key={slot.id} className={classNames("schedule-grid-slot", { expanded: isExpanded })}>
        <button type="button" className="schedule-grid-slot-summary" onClick={() => toggleSlot(slot.id)}>
          <span className="schedule-grid-slot-arrow">{isExpanded ? "▼" : "▶"}</span>
          <div className="schedule-grid-slot-info">
            <div className="schedule-grid-slot-time">
              {slot.start_time}
              {"-"}
              {slot.end_time}
            </div>
            <div className="schedule-grid-slot-meta">{slotLabel}</div>
            {instructorName ? (
              <div className="schedule-grid-slot-instructor">Инструктор: {instructorName}</div>
            ) : null}
            <div className="schedule-grid-slot-stats">
              <span className="occupied">Занято {occupied}</span>
              <span className="free">Свободно {free}</span>
            </div>
          </div>
        </button>
        {isExpanded ? <div className="schedule-grid-slot-body">{renderReservationList(slot)}</div> : null}
      </div>
    );
  }

  const body = (() => {
    if (weeksQuery.isLoading || weekDetailQuery.isLoading) {
      return <div className="schedule-empty">Загружаем расписание…</div>;
    }

    if (weeksQuery.isError || weekDetailQuery.isError) {
      return <div className="schedule-error">Не удалось загрузить расписание.</div>;
    }

    if (!selectedWeek) {
      return <div className="schedule-empty">Недели не найдены.</div>;
    }

    return (
      <div className="schedule-grid">
        {dayColumns.map((day) => (
          <div key={day.dateIso} className="schedule-grid-column">
            <div className="schedule-grid-day-header">
              <div className="schedule-grid-day-title">
                <span>{day.weekday}</span>
                <strong>{day.label}</strong>
              </div>
              <div className="schedule-grid-day-meta">
                <span>Слотов {day.totals.totalSlots}</span>
                <span>Занято {day.totals.occupied}</span>
                <span>Свободно {day.totals.free}</span>
              </div>
            </div>
            <div className="schedule-grid-day-body">
              {day.slots.length === 0 ? (
                <div className="schedule-empty small">Нет слотов</div>
              ) : (
                day.slots.map((slot) => renderSlotCard(slot))
              )}
            </div>
          </div>
        ))}
      </div>
    );
  })();

  return (
    <Panel
      title="Расписание"
      subtitle={
        selectedWeek
          ? `Неделя ${selectedWeek.week_start_date}${selectedWeek.title ? ` · ${selectedWeek.title}` : ""}`
          : undefined
      }
      headerExtra={headerExtra}
    >
      {body}
    </Panel>
  );
}
