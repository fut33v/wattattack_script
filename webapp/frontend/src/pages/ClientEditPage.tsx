import { useMemo } from "react";
import { useNavigate, useParams, Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import { apiFetch, ApiError } from "../lib/api";
import type {
  BikeListResponse,
  BikeRow,
  ClientRow,
  ClientActivitiesResponse,
  ClientActivityItem,
} from "../lib/types";
import StateScreen from "../components/StateScreen";

const PEDAL_OPTIONS = [
  "топталки (под кроссовки)",
  "контакты шоссе Look",
  "контакты шоссе Shimano",
  "контакты MTB Shimano",
  "принесу свои"
] as const;

const GENDER_LABELS: Record<string, string> = {
  male: "Мужской",
  female: "Женский"
};

const GENDER_OPTIONS = [
  { value: "", label: "Не указано" },
  { value: "male", label: "Мужской" },
  { value: "female", label: "Женский" }
] as const;

function formatGender(value: string | null | undefined): string {
  if (!value) return "—";
  const key = value.toLowerCase();
  return GENDER_LABELS[key] ?? value;
}

function formatDistance(meters?: number | null): string {
  if (meters === undefined || meters === null) return "—";
  const km = meters / 1000;
  return `${km.toFixed(1)} км`;
}

function formatElevation(meters?: number | null): string {
  if (meters === undefined || meters === null) return "—";
  return `${Math.round(meters)} м`;
}

function formatDuration(seconds?: number | null): string {
  if (seconds === undefined || seconds === null) return "—";
  const total = Number(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  if (hours > 0) return `${hours}ч ${minutes.toString().padStart(2, "0")}м`;
  return `${minutes}м`;
}

interface ClientResponse {
  item: ClientRow;
}

export default function ClientEditPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const clientId = useMemo(() => Number(id), [id]);

  const isIdValid = Number.isInteger(clientId) && clientId > 0;

  const clientQuery = useQuery<ClientResponse>({
    queryKey: ["client", clientId],
    queryFn: () => apiFetch<ClientResponse>(`/api/clients/${clientId}`),
    enabled: isIdValid
  });

  const activitiesQuery = useQuery<ClientActivitiesResponse>({
    queryKey: ["client-activities", clientId],
    queryFn: () => apiFetch<ClientActivitiesResponse>(`/api/clients/${clientId}/activities`),
    enabled: isIdValid
  });

  const bikesQuery = useQuery<BikeListResponse>({
    queryKey: ["bikes"],
    queryFn: () => apiFetch<BikeListResponse>("/api/bikes")
  });

  const updateMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch(`/api/clients/${clientId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["clients"], exact: false });
      queryClient.invalidateQueries({ queryKey: ["client", clientId] });
    }
  });

  if (!isIdValid) {
    return <StateScreen title="Некорректный ID клиента" message="Проверьте ссылку и попробуйте снова." />;
  }

  if (clientQuery.isLoading) {
    return <StateScreen title="Загрузка клиента" message="Получаем данные…" />;
  }

  if (clientQuery.isError || !clientQuery.data) {
    return <StateScreen title="Ошибка" message="Не удалось загрузить данные клиента." action={<Link className="button" to="/clients">Назад к списку</Link>} />;
  }

  const client = clientQuery.data.item;
  const bikes = bikesQuery.data?.items ?? [];
  const favoriteBikeValue = client.favorite_bike ?? "";
  const isCustomFavoriteBike =
    favoriteBikeValue !== "" && !bikes.some((bike: BikeRow) => bike.title === favoriteBikeValue);
  const pedalsValue = client.pedals ?? "";
  const isCustomPedals = pedalsValue !== "" && !PEDAL_OPTIONS.includes(pedalsValue as (typeof PEDAL_OPTIONS)[number]);

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};

    const numericFields = ["weight", "ftp", "height"] as const;
    numericFields.forEach((field) => {
      const value = formData.get(field);
      if (value === null || value === "") {
        payload[field] = null;
        return;
      }
      const num = Number(value);
      if (!Number.isNaN(num)) {
        payload[field] = num;
      }
    });

    const textFields = ["first_name", "last_name", "favorite_bike", "pedals", "goal", "gender", "saddle_height"] as const;
    textFields.forEach((field) => {
      const value = formData.get(field);
      if (typeof value === "string") {
        payload[field] = value.trim() || null;
      }
    });

    updateMutation.mutate(payload);
  }

  return (
    <Panel
      title={`Клиент #${client.id}`}
      subtitle={client.full_name || [client.first_name, client.last_name].filter(Boolean).join(" ") || "Без имени"}
      headerExtra={
        <button className="button" type="button" onClick={() => navigate(-1)}>
          ← Назад
        </button>
      }
    >
      <div className="detail-grid">
        <section className="detail-card">
          <h3>Основные данные</h3>
          <dl>
            <div>
              <dt>Полное имя</dt>
              <dd>{client.full_name ?? "—"}</dd>
            </div>
            <div>
              <dt>Имя</dt>
              <dd>{client.first_name ?? "—"}</dd>
            </div>
            <div>
              <dt>Фамилия</dt>
              <dd>{client.last_name ?? "—"}</dd>
            </div>
            <div>
              <dt>Пол</dt>
              <dd>{formatGender(client.gender)}</dd>
            </div>
            <div>
              <dt>Рост</dt>
              <dd>{client.height ? `${client.height} см` : "—"}</dd>
            </div>
            <div>
              <dt>Седло</dt>
              <dd>{client.saddle_height ?? "—"}</dd>
            </div>
            <div>
              <dt>Цель</dt>
              <dd>{client.goal ?? "—"}</dd>
            </div>
            <div>
              <dt>Анкета заполнена</dt>
              <dd>{client.submitted_at ? dayjs(client.submitted_at).format("DD.MM.YYYY HH:mm") : "—"}</dd>
            </div>
          </dl>
        </section>

        <section className="detail-card">
          <h3>Редактирование</h3>
          <form className="form-grid" onSubmit={handleSubmit}>
            <label>
              Вес (кг)
              <input type="number" step="0.1" name="weight" defaultValue={client.weight ?? ""} />
            </label>
            <label>
              FTP
              <input type="number" step="1" name="ftp" defaultValue={client.ftp ?? ""} />
            </label>
            <label>
              Рост (см)
              <input type="number" step="1" min={0} name="height" defaultValue={client.height ?? ""} />
            </label>
            <label>
              Имя
              <input type="text" name="first_name" defaultValue={client.first_name ?? ""} />
            </label>
            <label>
              Фамилия
              <input type="text" name="last_name" defaultValue={client.last_name ?? ""} />
            </label>
            <label>
              Пол
              <select name="gender" defaultValue={client.gender ?? ""}>
                {GENDER_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Любимый велосипед
              <select name="favorite_bike" defaultValue={favoriteBikeValue} disabled={bikesQuery.isLoading}>
                <option value="">— Не выбран —</option>
                {bikes.map((bike) => (
                  <option key={bike.id} value={bike.title}>
                    {bike.title}
                    {bike.owner ? ` (${bike.owner})` : ""}
                  </option>
                ))}
                {isCustomFavoriteBike && <option value={favoriteBikeValue}>Другой: {favoriteBikeValue}</option>}
              </select>
              {bikesQuery.isError && <span className="trainer-hint">Не удалось загрузить список велосипедов.</span>}
            </label>
            <label>
              Педали
              <select name="pedals" defaultValue={pedalsValue}>
                <option value="">— Не выбрано —</option>
                {PEDAL_OPTIONS.map((label) => (
                  <option key={label} value={label}>
                    {label}
                  </option>
                ))}
                {isCustomPedals && <option value={pedalsValue}>Другие: {pedalsValue}</option>}
              </select>
            </label>
            <label>
              Высота седла
              <input type="text" name="saddle_height" defaultValue={client.saddle_height ?? ""} />
            </label>
            <label>
              Цель
              <textarea name="goal" rows={3} defaultValue={client.goal ?? ""} />
            </label>
            <div className="form-actions">
              <button type="submit" className="button" disabled={updateMutation.isPending}>
                {updateMutation.isPending ? "Сохраняем…" : "Сохранить"}
              </button>
              <Link className="button" to="/clients">
                К списку
              </Link>
            </div>
            {updateMutation.isError && (
              <div className="form-error">
                {(updateMutation.error as ApiError)?.message ?? "Не удалось сохранить изменения."}
              </div>
            )}
            {updateMutation.isSuccess && <div className="muted">Изменения сохранены.</div>}
          </form>
        </section>
      </div>

      <section className="detail-card">
        <div className="detail-card__header">
          <h3>Активности WattAttack</h3>
          {activitiesQuery.isLoading && <span className="meta-hint">Загружаем…</span>}
          {activitiesQuery.isError && <span className="form-error">Не удалось загрузить активности.</span>}
        </div>
        {activitiesQuery.data?.stats && (
          <div className="activity-stats-grid">
            <div className="stat-card">
              <div className="stat-label">Дистанция</div>
              <div className="stat-value">{formatDistance(activitiesQuery.data.stats.distance)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Набор высоты</div>
              <div className="stat-value">{formatElevation(activitiesQuery.data.stats.elevation_gain)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Время</div>
              <div className="stat-value">{formatDuration(activitiesQuery.data.stats.elapsed_time)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Количество</div>
              <div className="stat-value">{activitiesQuery.data.stats.count}</div>
            </div>
          </div>
        )}
        <div className="table-container compact-table">
          <table className="data-table">
            <thead>
              <tr>
                <th>Account</th>
                <th>Activity</th>
                <th>Дата</th>
                <th>Имя по расписанию</th>
                <th>Имя в WattAttack</th>
                <th>Дистанция</th>
                <th>Набор</th>
                <th>Время</th>
              </tr>
            </thead>
            <tbody>
              {(activitiesQuery.data?.items ?? []).length === 0 ? (
                <tr>
                  <td colSpan={8}>{activitiesQuery.isLoading ? "Загрузка…" : "Активностей пока нет."}</td>
                </tr>
              ) : (
                (activitiesQuery.data?.items ?? []).map((item: ClientActivityItem) => (
                  <tr key={`${item.account_id}-${item.activity_id}`}>
                    <td>{item.account_id}</td>
                    <td>
                      <Link to={`/activities/${encodeURIComponent(item.account_id)}/${encodeURIComponent(item.activity_id)}`}>
                        {item.activity_id}
                      </Link>
                    </td>
                    <td>{item.start_time ? dayjs(item.start_time).format("DD.MM.YYYY HH:mm") : "—"}</td>
                    <td>{item.scheduled_name || "—"}</td>
                    <td>{item.profile_name || "—"}</td>
                    <td>{formatDistance(item.distance)}</td>
                    <td>{formatElevation(item.elevation_gain)}</td>
                    <td>{formatDuration(item.elapsed_time)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </Panel>
  );
}
