import { useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import classNames from "classnames";

import Panel from "../components/Panel";
import { apiFetch, ApiError } from "../lib/api";
import type { RaceSummaryResponse, RaceRegistration, BikeRow } from "../lib/types";

import "../styles/schedule.css";

const STATUS_LABELS: Record<string, string> = {
  pending: "Ожидание",
  approved: "Подтверждена",
  rejected: "Отклонена"
};

function formatHeight(value?: number | null): string {
  return value != null ? `${value} см` : "—";
}

function formatWeight(value?: number | null): string {
  return value != null ? `${value} кг` : "—";
}

function formatFtp(value?: number | null): string {
  return value != null ? `${value}` : "—";
}

export default function RaceSummaryPage() {
  const { id } = useParams();
  const raceId = id ? Number(id) : NaN;
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const summaryQuery = useQuery<RaceSummaryResponse>({
    queryKey: ["race-summary", raceId],
    enabled: Number.isFinite(raceId),
    queryFn: () => apiFetch<RaceSummaryResponse>(`/api/races/${raceId}/summary`)
  });

  const updateBikeMutation = useMutation<
    { item: RaceRegistration },
    ApiError,
    { registrationId: number; bikeId: number | null; bringOwnBike?: boolean }
  >({
    mutationFn: ({ registrationId, bikeId, bringOwnBike }) =>
      apiFetch<{ item: RaceRegistration }>(`/api/races/${raceId}/registrations/${registrationId}`, {
        method: "PATCH",
        body: JSON.stringify({
          bikeId,
          bringOwnBike
        })
      }),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ["race-summary", raceId] });
      queryClient.setQueryData<RaceSummaryResponse>(["race-summary", raceId], (prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          registrations: prev.registrations.map((reg) => (reg.id === data.item.id ? { ...reg, ...data.item } : reg))
        };
      });
    }
  });

  const summary = summaryQuery.data;
  const bikes = summary?.bikes ?? [];

  function timeToMinutes(value?: string | null): number {
    if (!value) return Number.POSITIVE_INFINITY;
    const parts = value.split(":").map((v) => Number.parseInt(v, 10));
    if (parts.length < 2 || Number.isNaN(parts[0]) || Number.isNaN(parts[1])) {
      return Number.POSITIVE_INFINITY;
    }
    const [hours, minutes] = parts;
    return hours * 60 + minutes;
  }

  const rows = useMemo(() => {
    const regs = summary?.registrations ?? [];
    return regs.slice().sort((a, b) => {
      const startA = timeToMinutes(a.cluster_start_time);
      const startB = timeToMinutes(b.cluster_start_time);
      if (startA !== startB) return startA - startB;
      const clusterA = (a.cluster_label || a.cluster_code || "").toLowerCase();
      const clusterB = (b.cluster_label || b.cluster_code || "").toLowerCase();
      if (clusterA !== clusterB) return clusterA.localeCompare(clusterB);
      const standA = typeof a.stand_order === "number" ? a.stand_order : Number.POSITIVE_INFINITY;
      const standB = typeof b.stand_order === "number" ? b.stand_order : Number.POSITIVE_INFINITY;
      if (standA !== standB) return standA - standB;
      const standLabelA = (a.stand_label || "").toLowerCase();
      const standLabelB = (b.stand_label || "").toLowerCase();
      if (standLabelA !== standLabelB) return standLabelA.localeCompare(standLabelB);
      return (a.client_name || "").localeCompare(b.client_name || "");
    });
  }, [summary?.registrations]);

  function bikeValue(reg: RaceRegistration): string {
    if (reg.bring_own_bike) return "own";
    if (reg.bike_id) return String(reg.bike_id);
    return "";
  }

  function bikeLabel(bike: BikeRow): string {
    const owner = bike.owner ? ` (${bike.owner})` : "";
    return `${bike.title}${owner}`;
  }

  const bikeOptions = [
    { value: "", label: "Не выбран" },
    { value: "own", label: "Свой велосипед" },
    ...bikes.map((bike) => ({ value: String(bike.id), label: bikeLabel(bike) }))
  ];

  if (!Number.isFinite(raceId)) {
    return (
      <Panel title="Сводка гонки" subtitle="Некорректный идентификатор">
        <div className="schedule-error">Проверьте ссылку и попробуйте снова.</div>
      </Panel>
    );
  }

  if (summaryQuery.isLoading) {
    return (
      <Panel title="Сводка гонки" subtitle="Загружаем данные">
        <div className="schedule-empty">Загружаем…</div>
      </Panel>
    );
  }

  if (summaryQuery.isError || !summary) {
    return (
      <Panel title="Сводка гонки" subtitle="Ошибка">
        <div className="schedule-error">Не удалось загрузить сводку.</div>
      </Panel>
    );
  }

  return (
    <Panel title="Сводка гонки" subtitle={summary.race.title}>
      <div className="slot-seating-header">
        <div className="slot-seating-meta">
          <div className="slot-seating-title">{summary.race.title}</div>
          <div className="slot-seating-subtitle">
            Дата: {summary.race.race_date}
            {summary.race.notes ? ` · ${summary.race.notes}` : ""}
          </div>
        </div>
        <div className="slot-seating-actions">
          <button type="button" className="btn ghost" onClick={() => navigate(-1)}>
            Назад
          </button>
        </div>
      </div>

      <div className="table-container">
        <table className="data-table">
          <thead>
            <tr>
              <th>Кластер</th>
              <th>Станок</th>
              <th>Имя</th>
              <th>Рост</th>
              <th>Вес</th>
              <th>FTP</th>
              <th>Велосипед</th>
              <th>Статус</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={8}>Нет участников</td>
              </tr>
            ) : (
              rows.map((reg) => (
                <tr key={reg.id}>
                  <td>{reg.cluster_label || reg.cluster_code || "—"}</td>
                  <td>{reg.stand_label || "—"}</td>
                  <td>{reg.client_name || `#${reg.client_id}`}</td>
                  <td>{formatHeight(reg.client_height)}</td>
                  <td>{formatWeight(reg.client_weight)}</td>
                  <td>{formatFtp(reg.client_ftp)}</td>
                  <td>
                    <select
                      value={bikeValue(reg)}
                      onChange={(event) => {
                        const value = event.target.value;
                        if (value === "own") {
                          updateBikeMutation.mutate({
                            registrationId: reg.id,
                            bikeId: null,
                            bringOwnBike: true
                          });
                        } else if (value === "") {
                          updateBikeMutation.mutate({
                            registrationId: reg.id,
                            bikeId: null,
                            bringOwnBike: false
                          });
                        } else {
                          updateBikeMutation.mutate({
                            registrationId: reg.id,
                            bikeId: Number(value),
                            bringOwnBike: false
                          });
                        }
                      }}
                      disabled={updateBikeMutation.isPending}
                    >
                      {bikeOptions.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <span className={classNames("status-chip", reg.status)}>
                      {STATUS_LABELS[reg.status] ?? reg.status}
                    </span>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}
