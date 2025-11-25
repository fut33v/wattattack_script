import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import classNames from "classnames";
import { useQuery } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { ScheduleSlot, ScheduleWeekDetailResponse, ScheduleWeekListResponse } from "../lib/types";

import "../styles/schedule.css";

function formatDateLabel(value: string) {
  const dateObj = new Date(value);
  if (Number.isNaN(dateObj.getTime())) return value;
  const weekday = ["Вс", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"][dateObj.getDay()] ?? "";
  return `${value} (${weekday})`;
}

function groupSlotsByDate(slots: ScheduleSlot[]) {
  const sorted = [...slots].sort((a, b) => {
    if (a.slot_date === b.slot_date) return a.start_time.localeCompare(b.start_time);
    return a.slot_date.localeCompare(b.slot_date);
  });
  const groups: { slotDate: string; slots: ScheduleSlot[] }[] = [];
  sorted.forEach((slot) => {
    const existing = groups.find((g) => g.slotDate === slot.slot_date);
    if (existing) {
      existing.slots.push(slot);
    } else {
      groups.push({ slotDate: slot.slot_date, slots: [slot] });
    }
  });
  return groups;
}

export default function ScheduleViewPage() {
  const { slug } = useParams();
  const navigate = useNavigate();
  const [selectedWeekId, setSelectedWeekId] = useState<number | null>(null);
  const [userChangedWeek, setUserChangedWeek] = useState<boolean>(false);
  const currentWeekStart = useMemo(() => {
    const today = new Date();
    const day = today.getDay();
    const diff = day === 0 ? -6 : 1 - day; // Monday as start
    const monday = new Date(today);
    monday.setDate(today.getDate() + diff);
    return monday.toISOString().slice(0, 10);
  }, []);

  const weeksQuery = useQuery<ScheduleWeekListResponse>({
    queryKey: ["schedule-weeks-view"],
    queryFn: () => apiFetch<ScheduleWeekListResponse>("/api/schedule/weeks?page=1&page_size=50"),
    staleTime: 60_000
  });

  const defaultWeekId = useMemo(() => {
    const items = weeksQuery.data?.items ?? [];
    if (!items.length) return null;
    const slugWeekStart = slug ? parseWeekSlug(slug) : null;
    if (slugWeekStart) {
        const slugMatch = items.find((week) => week.week_start_date === slugWeekStart);
        if (slugMatch) return slugMatch.id;
    }
    const exact = items.find((week) => week.week_start_date === currentWeekStart);
    if (exact) return exact.id;

    const pastOrCurrent = items
      .filter((week) => week.week_start_date <= currentWeekStart)
      .sort((a, b) => (a.week_start_date > b.week_start_date ? -1 : 1));
    if (pastOrCurrent.length) return pastOrCurrent[0].id;

    const future = items.filter((week) => week.week_start_date > currentWeekStart).sort((a, b) => (a.week_start_date > b.week_start_date ? 1 : -1));
    if (future.length) return future[0].id;

    return items[0].id;
  }, [weeksQuery.data?.items, currentWeekStart]);

  useEffect(() => {
    if (defaultWeekId && (!userChangedWeek || selectedWeekId === null) && selectedWeekId !== defaultWeekId) {
      setSelectedWeekId(defaultWeekId);
    }
  }, [selectedWeekId, defaultWeekId, userChangedWeek]);

  const weekQuery = useQuery<ScheduleWeekDetailResponse>({
    queryKey: ["schedule-week-view", selectedWeekId],
    enabled: selectedWeekId != null,
    queryFn: () => apiFetch<ScheduleWeekDetailResponse>(`/api/schedule/weeks/${selectedWeekId}`)
  });

  function shiftWeek(delta: number) {
    if (!weeksQuery.data?.items?.length || selectedWeekId == null) return;
    const items = weeksQuery.data.items;
    const idx = items.findIndex((week) => week.id === selectedWeekId);
    if (idx === -1) return;
    const next = items[idx + delta];
    if (next) {
      setSelectedWeekId(next.id);
      setUserChangedWeek(true);
    }
  }

  const groups = useMemo(() => groupSlotsByDate(weekQuery.data?.slots ?? []), [weekQuery.data?.slots]);
  const standsCount = weekQuery.data?.stands?.length ?? 0;

  function formatWeekSlug(dateStr: string) {
    const parsed = new Date(dateStr);
    if (Number.isNaN(parsed.getTime())) return null;
    const dd = String(parsed.getDate()).padStart(2, "0");
    const mm = String(parsed.getMonth() + 1).padStart(2, "0");
    const yy = String(parsed.getFullYear()).slice(-2);
    return `week_${dd}_${mm}_${yy}`;
  }

  function parseWeekSlug(slugValue: string) {
    if (!slugValue) return null;
    const normalized = slugValue.trim().toLowerCase();
    if (!normalized.startsWith("week_")) return null;
    const rest = normalized.replace("week_", "");
    const [dd, mm, yy] = rest.split("_");
    if (!(dd && mm && yy)) return null;
    const fullYear = Number(`20${yy}`);
    const iso = `${fullYear}-${mm}-${dd}`;
    const parsed = new Date(iso);
    if (Number.isNaN(parsed.getTime())) return null;
    return iso;
  }

  useEffect(() => {
    if (!weeksQuery.data?.items?.length || !selectedWeekId) return;
    const currentSlug = slug ?? "";
    const selectedWeek = weeksQuery.data.items.find((week) => week.id === selectedWeekId);
    if (selectedWeek) {
      const formattedSlug = formatWeekSlug(selectedWeek.week_start_date);
      if (formattedSlug && formattedSlug !== currentSlug) {
        navigate(`/schedule/${formattedSlug}`, { replace: true });
      }
    }
  }, [weeksQuery.data?.items, selectedWeekId, slug, navigate]);

  return (
    <Panel
      title="Расписание"
      subtitle="Просмотр расписания с быстрым переходом в редактирование слота"
      headerExtra={
        <div className="schedule-toolbar-actions">
          <Link to="/schedule/manage" className="btn ghost">
            Редактор недели
          </Link>
          <a href="/schedule/current_week" className="btn ghost" target="_blank" rel="noreferrer">
            Публичное расписание
          </a>
        </div>
      }
    >
      <div className="schedule-toolbar">
        <div className="schedule-week-info">
          <div className="schedule-week-title">Недели</div>
          <div className="schedule-week-subtitle">Выберите неделю для просмотра</div>
        </div>
        <div className="schedule-toolbar-actions">
          <div className="schedule-week-nav">
            <button type="button" className="btn ghost" onClick={() => shiftWeek(1)} disabled={!weeksQuery.data?.items?.length}>
              ← Назад
            </button>
            <button type="button" className="btn ghost" onClick={() => shiftWeek(-1)} disabled={!weeksQuery.data?.items?.length}>
              Вперёд →
            </button>
          </div>
          <select
            value={selectedWeekId ?? ""}
            onChange={(event) => {
              setSelectedWeekId(event.target.value ? Number(event.target.value) : null);
              setUserChangedWeek(true);
            }}
          >
            {weeksQuery.data?.items?.map((week) => (
              <option key={week.id} value={week.id}>{`${week.week_start_date} (#${week.id})`}</option>
            ))}
          </select>
        </div>
      </div>

      {weeksQuery.isLoading || weekQuery.isLoading ? <div className="schedule-empty">Загружаем расписание…</div> : null}
      {weekQuery.isError ? <div className="schedule-error">Не удалось загрузить неделю.</div> : null}
      {!weekQuery.isLoading && groups.length === 0 ? <div className="schedule-empty">В неделе нет слотов.</div> : null}

      <div className="schedule-grid">
        {groups.map((group) => (
          <div key={group.slotDate} className="schedule-grid-column">
            <div className="schedule-grid-day-header">
              <div className="schedule-grid-day-title">
                <span>{formatDateLabel(group.slotDate)}</span>
                <span className="schedule-day-count">{group.slots.length}</span>
              </div>
              <div className="schedule-grid-day-meta">Слотов на день</div>
            </div>
            <div className="schedule-grid-day-body">
              {group.slots.map((slot) => {
                const booked = slot.reservations.filter((res) => res.client_id != null).length;
                const capacity = standsCount || slot.reservations.length || "?";
                const free = standsCount ? Math.max(standsCount - booked, 0) : null;
                const assigned = slot.reservations
                  .filter((res) => res.client_id != null)
                  .map((res) => res.client_name || `Клиент #${res.client_id}`)
                  .filter(Boolean);
                return (
                  <div key={slot.id} className="schedule-grid-slot">
                    <div className="schedule-grid-slot-summary">
                      <div className="schedule-grid-slot-info">
                        <div className="schedule-grid-slot-time">
                          {slot.start_time}-{slot.end_time}
                        </div>
                        <div className="schedule-grid-slot-meta">
                          Инструктор: {slot.instructorName ?? "—"}
                        </div>
                        {slot.label ? <div className="schedule-grid-slot-tag">{slot.label}</div> : null}
                        <div className="schedule-grid-slot-stats">
                          <span className="free">Свободно: {free ?? "?"} / {capacity}</span>
                        </div>
                        {assigned.length > 0 ? (
                          <div className="schedule-grid-slot-clients">
                            {assigned.map((name, idx) => (
                              <span key={`${slot.id}-client-${idx}`} className="schedule-grid-slot-client">
                                {name}
                              </span>
                            ))}
                          </div>
                        ) : null}
                      </div>
                    </div>
                    <div className="schedule-grid-slot-body">
                      <Link to={`/schedule/slot/${slot.id}`} className="btn primary">
                        Редактировать
                      </Link>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </Panel>
  );
}
