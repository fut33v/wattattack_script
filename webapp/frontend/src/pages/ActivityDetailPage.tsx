import { Link, useParams } from "react-router-dom";
import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import { ApiError, apiFetch } from "../lib/api";
import type {
  ActivityDetailResponse,
  ActivityIdRecord,
  ActivityFitDownloadResponse,
  ActivityStravaUploadResponse,
  ClientListResponse,
  ClientRow,
} from "../lib/types";

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
  const { client_id, manual_client_id, manual_client_name, scheduled_name, profile_name } = activity;
  const preferredId = manual_client_id || client_id;
  const displayName =
    manual_client_name ||
    scheduled_name ||
    profile_name ||
    (preferredId ? `Клиент #${preferredId}` : "Не найден");

  return (
    <div className="client-link-block">
      <div className="meta-label">Клиент</div>
      {preferredId ? (
        <Link className="client-link" to={`/clients/${preferredId}`}>
          {displayName}
        </Link>
      ) : (
        <div className="meta-value">{displayName}</div>
      )}
      <div className="meta-hint">
        {manual_client_id
          ? "Выбран вручную"
          : scheduled_name
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
  const queryClient = useQueryClient();
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionStatus, setActionStatus] = useState<"success" | "error" | null>(null);
  const [clientInput, setClientInput] = useState<string>("");
  const [clientSearch, setClientSearch] = useState<string>("");

  const detailQuery = useQuery<ActivityDetailResponse>({
    queryKey: ["activity-detail", accountId, activityId],
    queryFn: () => apiFetch<ActivityDetailResponse>(`/api/activities/${accountId}/${activityId}`),
    enabled: Boolean(accountId && activityId),
    staleTime: 0,
  });

  const activity: ActivityIdRecord | undefined = detailQuery.data?.item;

  useEffect(() => {
    setActionMessage(null);
    setActionStatus(null);
  }, [accountId, activityId]);

  useEffect(() => {
    setClientInput(activity?.client_id ? String(activity.client_id) : "");
  }, [activity?.client_id]);

  const handleSuccess = (message?: string | null) => {
    setActionStatus("success");
    setActionMessage(message || "Готово");
    queryClient.invalidateQueries({ queryKey: ["activity-detail", accountId, activityId] });
  };

  const handleError = (error: unknown, fallback: string) => {
    const message =
      error instanceof ApiError ? error.message || fallback : fallback;
    setActionStatus("error");
    setActionMessage(message);
  };

  const stravaUpload = useMutation({
    mutationFn: () =>
      apiFetch<ActivityStravaUploadResponse>(
        `/api/activities/${accountId}/${activityId}/strava`,
        { method: "POST" },
      ),
    onSuccess: (data) => {
      handleSuccess(data?.message || "Активность отправлена в Strava");
    },
    onError: (error: unknown) => {
      handleError(error, "Не удалось отправить в Strava");
    },
  });

  const sendToBot = useMutation({
    mutationFn: () =>
      apiFetch<ActivityStravaUploadResponse>(
        `/api/activities/${accountId}/${activityId}/clientbot`,
        { method: "POST" },
      ),
    onSuccess: (data) => {
      handleSuccess(data?.message || "Отправлено в бота");
    },
    onError: (error: unknown) => {
      handleError(error, "Не удалось отправить в бота");
    },
  });

  const sendToIntervals = useMutation({
    mutationFn: () =>
      apiFetch<ActivityStravaUploadResponse>(
        `/api/activities/${accountId}/${activityId}/intervals`,
        { method: "POST" },
      ),
    onSuccess: (data) => {
      handleSuccess(data?.message || "Отправлено в Intervals");
    },
    onError: (error: unknown) => {
      handleError(error, "Не удалось отправить в Intervals");
    },
  });

  const downloadFit = useMutation({
    mutationFn: () =>
      apiFetch<ActivityFitDownloadResponse>(`/api/activities/${accountId}/${activityId}/fit`, {
        method: "POST",
      }),
    onSuccess: (data) => {
      handleSuccess(data?.message || "FIT-файл скачан");
    },
    onError: (error: unknown) => {
      handleError(error, "Не удалось скачать FIT-файл");
    },
  });

  const updateClient = useMutation({
    mutationFn: () =>
      apiFetch<ActivityDetailResponse>(`/api/activities/${accountId}/${activityId}/client`, {
        method: "PATCH",
        body: { client_id: clientInput.trim() || null },
      }),
    onSuccess: () => {
      handleSuccess("Клиент обновлен");
      detailQuery.refetch();
    },
    onError: (error: unknown) => {
      handleError(error, "Не удалось обновить клиента");
    },
  });

  const anyPending =
    stravaUpload.isPending ||
    sendToBot.isPending ||
    sendToIntervals.isPending ||
    downloadFit.isPending ||
    updateClient.isPending;

  const clientsQuery = useQuery<ClientListResponse>({
    queryKey: ["client-search", clientSearch],
    enabled: clientSearch.trim().length >= 2,
    queryFn: () =>
      apiFetch<ClientListResponse>(
        `/api/clients?search=${encodeURIComponent(clientSearch.trim())}&page=1&sort=last_name&direction=asc`,
      ),
    staleTime: 30_000,
  });

  const formatClientName = (client: ClientRow) =>
    client.full_name?.trim() ||
    [client.first_name, client.last_name].filter(Boolean).join(" ").trim() ||
    `Клиент #${client.id}`;

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
          <div className="activity-meta-grid">
            <MetaField label="Account ID" value={activity.account_id} />
            <MetaField label="Activity ID" value={activity.activity_id} />
            <MetaField label="Дата активности" value={formatDateTime(activity.start_time)} />
            <MetaField label="Дата добавления" value={formatDateTime(activity.created_at)} />
            <MetaField label="Клиент ID" value={activity.client_id ?? "—"} />
            <MetaField label="Ручной клиент ID" value={activity.manual_client_id ?? "—"} />
            <MetaField label="Имя по расписанию" value={activity.scheduled_name || "—"} />
            <MetaField label="Имя в WattAttack" value={activity.profile_name || "—"} />
          </div>

          <div className="flag-row">
            <Flag label="БОТ" value={activity.sent_clientbot} />
            <Flag label="Strava" value={activity.sent_strava} />
            <Flag label="ICU" value={activity.sent_intervals} />
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
                  <button
                    type="button"
                    className="button"
                    onClick={() => downloadFit.mutate()}
                    disabled={anyPending}
                  >
                    {downloadFit.isPending ? "⏳ Скачиваем…" : "⬇️ Запросить из WattAttack"}
                  </button>
                )}
              </div>
            </div>
          </div>

          <ClientLink activity={activity} />

          <div className="activity-actions">
            <button
              type="button"
              className="button primary action-button"
              onClick={() => stravaUpload.mutate()}
              disabled={!activity || anyPending}
            >
              {stravaUpload.isPending ? "⏳ Загрузка…" : "🚴‍♂️ Загрузить в Strava"}
            </button>
            <button
              type="button"
              className="button action-button"
              onClick={() => sendToBot.mutate()}
              disabled={!activity || anyPending}
            >
              {sendToBot.isPending ? "⏳ Отправляем…" : "🤖 Отправить в бота"}
            </button>
            <button
              type="button"
              className="button action-button"
              onClick={() => sendToIntervals.mutate()}
              disabled={!activity || anyPending}
            >
              {sendToIntervals.isPending ? "⏳ Отправляем…" : "📊 Отправить в ICU"}
            </button>
            {actionMessage && (
              <span
                className={`action-hint ${
                  actionStatus === "error" ? "action-hint--error" : "action-hint--success"
                }`}
              >
                {actionMessage}
              </span>
            )}
          </div>

          <div className="activity-client-edit">
            <label>
              Client ID
              <input
                type="number"
                min="1"
                value={clientInput}
                onChange={(e) => setClientInput(e.target.value)}
                placeholder="ID клиента"
              />
            </label>
            <button
              type="button"
              className="button"
              onClick={() => updateClient.mutate()}
              disabled={updateClient.isPending || !accountId || !activityId}
            >
              {updateClient.isPending ? "Сохраняем…" : "Обновить клиента"}
            </button>
          </div>
          <div className="activity-client-search">
            <label>
              Поиск клиента
              <input
                type="text"
                value={clientSearch}
                onChange={(e) => setClientSearch(e.target.value)}
                placeholder="ФИО или часть имени"
              />
            </label>
            {clientSearch.trim().length >= 2 && (
              <div className="activity-client-search-results">
                {clientsQuery.isLoading && <div className="meta-hint">Ищем…</div>}
                {clientsQuery.isError && <div className="form-error">Ошибка поиска.</div>}
                {!clientsQuery.isLoading && !clientsQuery.isError && (
                  <>
                    {(clientsQuery.data?.items.length ?? 0) === 0 && (
                      <div className="meta-hint">Ничего не найдено.</div>
                    )}
                    {(clientsQuery.data?.items ?? []).map((item) => (
                      <button
                        type="button"
                        key={item.id}
                        className="client-chip"
                        onClick={() => {
                          setClientInput(String(item.id));
                          setClientSearch(formatClientName(item));
                        }}
                      >
                        <span className="client-chip-name">{formatClientName(item)}</span>
                        <span className="client-chip-id">#{item.id}</span>
                      </button>
                    ))}
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </Panel>
  );
}
