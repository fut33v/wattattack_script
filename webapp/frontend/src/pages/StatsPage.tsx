import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import Panel from "../components/Panel";
import StateScreen from "../components/StateScreen";
import { apiFetch } from "../lib/api";
import type { StatsResponse } from "../lib/types";

const currentMonthKey = formatMonthKey(new Date());

export default function StatsPage() {
  const [selectedMonth, setSelectedMonth] = useState<string>(currentMonthKey);
  const statsQuery = useQuery<StatsResponse>({
    queryKey: ["stats", selectedMonth],
    queryFn: () => apiFetch<StatsResponse>(`/api/stats?month=${selectedMonth}`)
  });

  useEffect(() => {
    if (statsQuery.data?.available_months?.length) {
      const firstAvailable = statsQuery.data.available_months[0];
      if (firstAvailable && !statsQuery.data.available_months.includes(selectedMonth)) {
        setSelectedMonth(firstAvailable);
      }
    }
  }, [selectedMonth, statsQuery.data?.available_months]);

  if (statsQuery.isLoading) {
    return <StateScreen title="Загрузка статистики" message="Получаем данные…" />;
  }

  if (statsQuery.isError || !statsQuery.data) {
    return <StateScreen title="Ошибка" message="Не удалось загрузить статистику." />;
  }

  const stats = statsQuery.data;
  const monthStats = stats.monthly;
  const monthLabel = formatMonthLabel(monthStats.month);
  const monthIndex = stats.available_months.indexOf(monthStats.month);
  const prevMonth = monthIndex < stats.available_months.length - 1 ? stats.available_months[monthIndex + 1] : null;
  const nextMonth = monthIndex > 0 ? stats.available_months[monthIndex - 1] : null;

  return (
    <Panel title="Статистика" subtitle="Доход по пополнениям и абонементам">
      <section className="stats-section">
        <div className="stat-grid">
          <StatCard label="Всего доход" value={stats.total_income_rub} tone="primary" currency />
          <StatCard label="Пополнения счета" value={stats.balance_income_rub} currency />
          <StatCard label="Продажи абонементов" value={stats.subscriptions_income_rub} currency />
          <StatCard label="Клиенты" value={stats.clients_total} />
          <StatCard label="Будущие бронирования" value={stats.reservations_upcoming} />
          <StatCard label="Прошедшие бронирования" value={stats.reservations_past} />
        </div>
        <p className="meta-hint">
          Доход складывается из всех пополнений баланса (включая автопополнения при учете тренировок) и стоимости созданных
          абонементов.
        </p>
      </section>

      <section className="stats-section">
        <div className="month-switcher">
          <div className="month-switcher__label">
            <div className="month-label">Месяц</div>
            <div className="month-title">{monthLabel}</div>
          </div>
          <div className="month-switcher__controls">
            <button className="button ghost icon-only" disabled={!prevMonth} onClick={() => prevMonth && setSelectedMonth(prevMonth)}>
              ←
            </button>
            <select
              className="month-select"
              value={monthStats.month}
              onChange={(e) => setSelectedMonth(e.target.value)}
              aria-label="Выбор месяца"
            >
              {stats.available_months.map((monthKey) => (
                <option key={monthKey} value={monthKey}>
                  {formatMonthLabel(monthKey)}
                </option>
              ))}
            </select>
            <button className="button ghost icon-only" disabled={!nextMonth} onClick={() => nextMonth && setSelectedMonth(nextMonth)}>
              →
            </button>
          </div>
        </div>

        <div className="stat-grid">
          <StatCard label="Доход за месяц" value={monthStats.total_income_rub} tone="primary" currency />
          <StatCard label="Пополнения" value={monthStats.balance_income_rub} currency />
          <StatCard label="Абонементы" value={monthStats.subscriptions_income_rub} currency />
          <StatCard label="Бронирования за месяц" value={monthStats.reservations} />
        </div>

        <h4>Недели выбранного месяца</h4>
        <div className="week-grid">
          {monthStats.weeks.map((week) => (
            <WeekCard key={week.week_start} week={week} />
          ))}
        </div>
      </section>
    </Panel>
  );
}

interface StatCardProps {
  label: string;
  value: number;
  tone?: "primary" | "muted";
  currency?: boolean;
}

function StatCard({ label, value, tone = "muted", currency = false }: StatCardProps) {
  return (
    <div className={`stat-card stat-card--${tone}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">
        {value.toLocaleString("ru-RU")}
        {currency ? " ₽" : ""}
      </div>
    </div>
  );
}

interface WeekCardProps {
  week: StatsResponse["monthly"]["weeks"][number];
}

function WeekCard({ week }: WeekCardProps) {
  const weekLabel = useMemo(() => formatWeekLabel(week.week_start, week.week_end), [week.week_end, week.week_start]);
  return (
    <div className="week-card">
      <div className="week-label">{weekLabel}</div>
      <div className="week-metrics">
        <div className="week-metric">
          <span>Доход</span>
          <strong>{week.income_rub.toLocaleString("ru-RU")} ₽</strong>
        </div>
        <div className="week-metric">
          <span>Бронирования</span>
          <strong>{week.reservations.toLocaleString("ru-RU")}</strong>
        </div>
      </div>
    </div>
  );
}

function formatMonthKey(date: Date) {
  return `${date.getFullYear()}-${`${date.getMonth() + 1}`.padStart(2, "0")}`;
}

function formatMonthLabel(monthKey: string) {
  const [yearStr, monthStr] = monthKey.split("-");
  const year = Number(yearStr);
  const month = Number(monthStr);
  const formatted = new Intl.DateTimeFormat("ru-RU", { month: "long", year: "numeric", timeZone: "UTC" }).format(
    new Date(Date.UTC(year, month - 1, 1))
  );
  return formatted.charAt(0).toUpperCase() + formatted.slice(1);
}

function formatWeekLabel(weekStart: string, weekEnd: string) {
  const startDate = parseDate(weekStart);
  const endDate = parseDate(weekEnd);
  const startDay = startDate.toLocaleDateString("ru-RU", { day: "numeric", month: "short", timeZone: "UTC" });
  const endDay = endDate.toLocaleDateString("ru-RU", { day: "numeric", month: "short", timeZone: "UTC" });
  return `${startDay} — ${endDay}`;
}

function parseDate(dateStr: string) {
  const [year, month, day] = dateStr.split("-").map(Number);
  return new Date(Date.UTC(year, month - 1, day));
}
