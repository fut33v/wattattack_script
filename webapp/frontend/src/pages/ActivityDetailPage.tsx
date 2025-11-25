import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { ActivityDetailResponse, ActivityIdRecord } from "../lib/types";

function formatDateTime(value?: string | null) {
  if (!value) return "—";
  try {
    return dayjs(value).format("DD.MM.YYYY HH:mm");
  } catch {
    return value;
  }
}

function formatDuration(seconds?: number | null) {
  if (seconds === null || seconds === undefined) return "—";
  const total = Number(seconds);
  if (Number.isNaN(total)) return "—";
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = Math.floor(total % 60);
  if (hours > 0) {
    return `${hours}ч ${minutes.toString().padStart(2, "0")}м`;
  }
  return `${minutes}м ${secs.toString().padStart(2, "0")}с`;
}

function formatDistance(meters?: number | null) {
  if (meters === null || meters === undefined) return "—";
  const km = Number(meters) / 1000;
  if (Number.isNaN(km)) return "—";
  return `${km.toFixed(1)} км`;
}

function formatNumber(value?: number | null, suffix = "") {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (Number.isNaN(num)) return "—";
  return `${num}${suffix}`;
}

function Flag({ label, value }: { label: string; value?: boolean | null }) {
  const icon = value ? "✅" : "✖️";
  return (
    <div className="flag-chip">
      <span className="flag-icon">{icon}</span>
      <span className="flag-label">{label}</span>
    </div>
  );
}

function ClientLink({ activity }: { activity: ActivityIdRecord }) {
  const { client_id, scheduled_name, profile_name } = activity;
  const displayName =
    scheduled_name || profile_name || (client_id ? `Клиент #${client_id}` : "Не найден");

  return (
    <div className="client-link-block">
      <div className="meta-label">Клиент</div>
      {client_id ? (
        <Link className="client-link" to={`/clients/${client_id}`}>
          {displayName}
        </Link>
      ) : (
        <div className="meta-value">{displayName}</div>
      )}
      <div className="meta-hint">
        {scheduled_name
          ? "Определен по расписанию"
          : profile_name
            ? "Определен по имени в WattAttack"
            : "Клиент не сопоставлен"}
      </div>
    </div>
  );
}

function MetaField({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="meta-card">
      <div className="meta-label">{label}</div>
      <div className="meta-value">{value}</div>
    </div>
  );
}

export default function ActivityDetailPage() {
  const { accountId = "", activityId = "" } = useParams();

  const detailQuery = useQuery<ActivityDetailResponse>({
    queryKey: ["activity-detail", accountId, activityId],
    queryFn: () => apiFetch<ActivityDetailResponse>(`/api/activities/${accountId}/${activityId}`),
    enabled: Boolean(accountId && activityId),
    staleTime: 0,
  });

  const activity: ActivityIdRecord | undefined = detailQuery.data?.item;

  return (
    <Panel
      title={`Активность ${activityId}`}
      subtitle={`Аккаунт ${accountId}`}
      headerExtra={
        <Link className="button" to="/activities">
          ⟵ К списку
        </Link>
      }
    >
      {detailQuery.isLoading && <div className="empty-state">Загружаем активность…</div>}
      {detailQuery.isError && (
        <div className="empty-state">Не удалось загрузить данные об активности.</div>
      )}
      {!detailQuery.isLoading && !activity && !detailQuery.isError && (
        <div className="empty-state">Активность не найдена.</div>
      )}
      {activity && (
        <div className="activity-detail">
          <ClientLink activity={activity} />

          <div className="activity-meta-grid">
            <MetaField label="Account ID" value={activity.account_id} />
            <MetaField label="Activity ID" value={activity.activity_id} />
            <MetaField label="Дата активности" value={formatDateTime(activity.start_time)} />
            <MetaField label="Дата добавления" value={formatDateTime(activity.created_at)} />
            <MetaField label="Клиент ID" value={activity.client_id ?? "—"} />
            <MetaField label="Имя по расписанию" value={activity.scheduled_name || "—"} />
            <MetaField label="Имя в WattAttack" value={activity.profile_name || "—"} />
          </div>

          <div className="flag-row">
            <Flag label="БОТ" value={activity.sent_clientbot} />
            <Flag label="Strava" value={activity.sent_strava} />
            <Flag label="Intervals" value={activity.sent_intervals} />
          </div>

          <div className="activity-stats-grid">
            <div className="stat-card">
              <div className="stat-label">Дистанция</div>
              <div className="stat-value">{formatDistance(activity.distance)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Время</div>
              <div className="stat-value">{formatDuration(activity.elapsed_time)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Набор высоты</div>
              <div className="stat-value">{formatNumber(activity.elevation_gain, " м")}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Средняя мощность</div>
              <div className="stat-value">{formatNumber(activity.average_power, " Вт")}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Средний каденс</div>
              <div className="stat-value">{formatNumber(activity.average_cadence, " об/мин")}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Средний пульс</div>
              <div className="stat-value">{formatNumber(activity.average_heartrate, " уд/мин")}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">FIT-файл</div>
              <div className="stat-value">
                {activity.fit_path ? (
                  <a className="client-link" href={activity.fit_path} download>
                    Скачать
                  </a>
                ) : (
                  "—"
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </Panel>
  );
}
