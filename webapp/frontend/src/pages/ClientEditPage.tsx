import { useMemo, useState } from "react";
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
  ClientReservationsResponse,
  ClientReservation,
  ClientLinkListResponse,
  ClientLinkRow,
  VkClientLinkListResponse,
  VkClientLinkRow,
  IntervalsLinkListResponse,
  IntervalsLinkRow
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
  const [editExpanded, setEditExpanded] = useState(false);
  const [reservationsExpanded, setReservationsExpanded] = useState(false);
  const [activitiesExpanded, setActivitiesExpanded] = useState(false);
  const [intervalsMessage, setIntervalsMessage] = useState<string | null>(null);
  const toggleEdit = () => setEditExpanded((prev) => !prev);
  const toggleReservations = () => setReservationsExpanded((prev) => !prev);
  const toggleActivities = () => setActivitiesExpanded((prev) => !prev);

  const isIdValid = Number.isInteger(clientId) && clientId > 0;

  const clientQuery = useQuery<ClientResponse>({
    queryKey: ["client", clientId],
    queryFn: () => apiFetch<ClientResponse>(`/api/clients/${clientId}`),
    enabled: isIdValid
  });

  const activitiesQuery = useQuery<ClientActivitiesResponse>({
    queryKey: ["client-activities", clientId],
    queryFn: () => apiFetch<ClientActivitiesResponse>(`/api/clients/${clientId}/activities`),
    enabled: isIdValid && activitiesExpanded
  });

  const reservationsQuery = useQuery<ClientReservationsResponse>({
    queryKey: ["client-reservations", clientId],
    queryFn: () => apiFetch<ClientReservationsResponse>(`/api/clients/${clientId}/reservations`),
    enabled: isIdValid && reservationsExpanded
  });

  const clientLinksQuery = useQuery<ClientLinkListResponse>({
    queryKey: ["client-links"],
    queryFn: () => apiFetch<ClientLinkListResponse>("/api/client-links"),
    enabled: isIdValid
  });

  const vkLinksQuery = useQuery<VkClientLinkListResponse>({
    queryKey: ["vk-client-links"],
    queryFn: () => apiFetch<VkClientLinkListResponse>("/api/vk-client-links"),
    enabled: isIdValid
  });

  const intervalsLinksQuery = useQuery<IntervalsLinkListResponse>({
    queryKey: ["intervals-links"],
    queryFn: () => apiFetch<IntervalsLinkListResponse>("/api/intervals-links"),
    enabled: isIdValid
  });

  const bikesQuery = useQuery<BikeListResponse>({
    queryKey: ["bikes"],
    queryFn: () => apiFetch<BikeListResponse>("/api/bikes")
  });

  const tgLink = useMemo(
    () => clientLinksQuery.data?.items.find((link: ClientLinkRow) => link.client_id === clientId),
    [clientLinksQuery.data?.items, clientId]
  );
  const vkLink = useMemo(
    () => vkLinksQuery.data?.items.find((link: VkClientLinkRow) => link.client_id === clientId),
    [vkLinksQuery.data?.items, clientId]
  );
  const intervalsLink = useMemo(
    () => intervalsLinksQuery.data?.items.find((link: IntervalsLinkRow) => link.client_id === clientId),
    [intervalsLinksQuery.data?.items, clientId]
  );

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

  const intervalsMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch("/api/intervals-links", {
        method: "POST",
        body: payload
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["intervals-links"] });
      setIntervalsMessage("Intervals сохранены.");
    },
    onError: (error: unknown) => {
      const message = (error as ApiError)?.message || "Не удалось сохранить Intervals.";
      setIntervalsMessage(message);
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
  const vkProfileUrl = vkLink
    ? vkLink.vk_username
      ? `https://vk.com/${vkLink.vk_username}`
      : `https://vk.com/id${vkLink.vk_user_id}`
    : null;
  const avatarUrl = tgLink?.tg_username ? `https://t.me/i/userpic/320/${tgLink.tg_username}.jpg` : null;
  const avatarFallback = (client.full_name || client.first_name || client.last_name || "?").trim().charAt(0) || "?";
  const displayName = client.full_name || [client.first_name, client.last_name].filter(Boolean).join(" ") || "Без имени";

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
      title={client.full_name || [client.first_name, client.last_name].filter(Boolean).join(" ") || "Без имени"}
      subtitle={`Клиент #${client.id}`}
      headerExtra={
        <button className="button" type="button" onClick={() => navigate(-1)}>
          ← Назад
        </button>
      }
    >
      <div className="detail-grid">
        <section className="detail-card">
          <h3>Основные данные</h3>
          <div className="profile-identity">
            <div className="profile-avatar">
              {avatarUrl ? <img src={avatarUrl} alt={displayName} /> : <span>{avatarFallback}</span>}
            </div>
            <div className="profile-meta">
              <div className="profile-name">{displayName}</div>
              <div className="profile-links">
                {tgLink && (
                  <a
                    className="profile-link"
                    href={`https://t.me/${tgLink.tg_username ?? tgLink.tg_user_id}`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    Telegram {tgLink.tg_username ? `@${tgLink.tg_username}` : `(ID ${tgLink.tg_user_id})`}
                  </a>
                )}
                {vkProfileUrl && (
                  <a className="profile-link" href={vkProfileUrl} target="_blank" rel="noreferrer">
                    VK {vkLink?.vk_username ? `@${vkLink.vk_username}` : `(id${vkLink?.vk_user_id})`}
                  </a>
                )}
              </div>
            </div>
          </div>
          <dl>
            <div>
              <dt>Полное имя</dt>
              <dd>{client.full_name ?? "—"}</dd>
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
            <div>
              <dt>Telegram</dt>
              <dd>
                {tgLink ? (
                  <>
                    {tgLink.tg_username ? (
                      <a className="client-link" href={`https://t.me/${tgLink.tg_username}`} target="_blank" rel="noreferrer">
                        @{tgLink.tg_username}
                      </a>
                    ) : (
                      <span className="meta-value">ID: {tgLink.tg_user_id}</span>
                    )}
                    {tgLink.tg_full_name && <span className="meta-hint"> · {tgLink.tg_full_name}</span>}
                  </>
                ) : (
                  "—"
                )}
              </dd>
            </div>
            <div>
              <dt>Привязки</dt>
              <dd className="meta-value">
                <span className="pill pill-muted">Telegram {tgLink ? "✅" : "✖"}</span>
                <span className="pill pill-muted">VK {vkLink ? "✅" : "✖"}</span>
                <span className="pill pill-muted">
                  Strava {tgLink?.strava_connected ? "✅" : "✖"}
                  {tgLink?.strava_athlete_name ? ` (${tgLink.strava_athlete_name})` : ""}
                </span>
                <span className="pill pill-muted">
                  Intervals {intervalsLink ? "✅" : "✖"}
                  {intervalsLink?.intervals_athlete_id ? ` (#${intervalsLink.intervals_athlete_id})` : ""}
                </span>
              </dd>
            </div>
          </dl>
        </section>
      </div>

      <section className="detail-card">
        <div className="detail-card__header detail-card__header--clickable" onClick={toggleEdit}>
          <h3>Редактирование</h3>
          <div className="detail-card__controls detail-card__controls--right">
            <button
              type="button"
              className="button secondary chevron-button"
              onClick={(event) => {
                event.stopPropagation();
                toggleEdit();
              }}
              aria-label={editExpanded ? "Свернуть редактирование" : "Показать редактирование"}
            >
              {editExpanded ? "▲" : "▼"}
            </button>
          </div>
        </div>
        {editExpanded && (
          <form className="form-grid edit-form-grid" onSubmit={handleSubmit}>
            <div className="form-column">
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
            </div>

            <div className="form-column">
              <div className="form-subsection">
                <div className="form-subsection__header">
                  <div>
                    <div className="form-subsection__title">Intervals.icu</div>
                    <div className="form-subsection__hint">API ключ хранится на сервере. Требуется привязка Telegram.</div>
                  </div>
                  {intervalsMessage && <div className="form-subsection__message">{intervalsMessage}</div>}
                </div>
                <div className="form-subgrid">
                  <label>
                    Intervals API ключ
                    <input
                      type="text"
                      name="intervals_api_key"
                      defaultValue={intervalsLink?.intervals_api_key ?? ""}
                      disabled={!tgLink}
                    />
                  </label>
                  <label>
                    Intervals Athlete ID
                    <input
                      type="text"
                      name="intervals_athlete_id"
                      defaultValue={intervalsLink?.intervals_athlete_id ?? ""}
                      disabled={!tgLink}
                    />
                  </label>
                </div>
                <div className="form-actions form-actions--inline">
                  <button
                    type="button"
                    className="button"
                    disabled={!tgLink || intervalsMutation.isPending}
                    onClick={(event) => {
                      event.preventDefault();
                      if (!tgLink) {
                        setIntervalsMessage("Сначала привяжите Telegram.");
                        return;
                      }
                      const form = event.currentTarget.form;
                      if (!form) return;
                      const formData = new FormData(form);
                      const apiKey = (formData.get("intervals_api_key") as string | null)?.trim() ?? "";
                      const athleteId = (formData.get("intervals_athlete_id") as string | null)?.trim() ?? "";
                      if (!apiKey) {
                        setIntervalsMessage("Укажите API ключ Intervals.");
                        return;
                      }
                      setIntervalsMessage(null);
                      intervalsMutation.mutate({
                        tg_user_id: tgLink.tg_user_id,
                        intervals_api_key: apiKey,
                        intervals_athlete_id: athleteId || "0"
                      });
                    }}
                  >
                    {intervalsMutation.isPending ? "Сохраняем…" : "Сохранить Intervals"}
                  </button>
                  {!tgLink && <span className="form-error">Нужна привязка Telegram, чтобы сохранить Intervals.</span>}
                </div>
              </div>
            </div>

            <div className="form-actions">
              <button type="submit" className="button" disabled={updateMutation.isPending}>
                {updateMutation.isPending ? "Сохраняем…" : "Сохранить"}
              </button>
            </div>
            {updateMutation.isError && (
              <div className="form-error">
                {(updateMutation.error as ApiError)?.message ?? "Не удалось сохранить изменения."}
              </div>
            )}
            {updateMutation.isSuccess && <div className="muted">Изменения сохранены.</div>}
          </form>
        )}
      </section>

      <section className="detail-card">
        <div className="detail-card__header detail-card__header--clickable" onClick={toggleReservations}>
          <h3>Бронирования</h3>
          <div className="detail-card__controls detail-card__controls--right">
            <button
              type="button"
              className="button secondary chevron-button"
              onClick={(event) => {
                event.stopPropagation();
                toggleReservations();
              }}
              aria-label={reservationsExpanded ? "Свернуть бронирования" : "Показать бронирования"}
            >
              {reservationsExpanded ? "▲" : "▼"}
            </button>
            {reservationsExpanded && reservationsQuery.isLoading && <span className="meta-hint">Загружаем…</span>}
            {reservationsExpanded && reservationsQuery.isError && (
              <span className="form-error">Не удалось загрузить бронирования.</span>
            )}
          </div>
        </div>
        {reservationsExpanded ? (
          <>
            {reservationsQuery.data?.stats && (
              <div className="reservations-stats">
                <div className="reservations-stat">
                  <div className="meta-label">Всего</div>
                  <div className="meta-value">{reservationsQuery.data.stats.total}</div>
                </div>
                <div className="reservations-stat">
                  <div className="meta-label">Будущие</div>
                  <div className="meta-value">{reservationsQuery.data.stats.upcoming}</div>
                </div>
                <div className="reservations-stat">
                  <div className="meta-label">Прошедшие</div>
                  <div className="meta-value">{reservationsQuery.data.stats.past}</div>
                </div>
              </div>
            )}
            <div className="reservations-grid">
              <div>
                <div className="meta-label">Будущие</div>
                {renderReservationsTable(reservationsQuery.data?.upcoming ?? [], "upcoming")}
              </div>
              <div>
                <div className="meta-label">Прошедшие</div>
                {renderReservationsTable(reservationsQuery.data?.past ?? [], "past")}
              </div>
            </div>
          </>
        ) : (
          <p className="meta-hint">Секция скрыта для ускорения загрузки. Нажмите «Показать», чтобы запросить бронирования.</p>
        )}
      </section>

      <section className="detail-card">
        <div className="detail-card__header detail-card__header--clickable" onClick={toggleActivities}>
          <h3>Активности WattAttack</h3>
          <div className="detail-card__controls detail-card__controls--right">
            <button
              type="button"
              className="button secondary chevron-button"
              onClick={(event) => {
                event.stopPropagation();
                toggleActivities();
              }}
              aria-label={activitiesExpanded ? "Свернуть активности" : "Показать активности"}
            >
              {activitiesExpanded ? "▲" : "▼"}
            </button>
            {activitiesExpanded && activitiesQuery.isLoading && <span className="meta-hint">Загружаем…</span>}
            {activitiesExpanded && activitiesQuery.isError && (
              <span className="form-error">Не удалось загрузить активности.</span>
            )}
          </div>
        </div>
        {activitiesExpanded ? (
          <>
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
          </>
        ) : (
          <p className="meta-hint">Секция скрыта для ускорения загрузки. Нажмите «Показать», чтобы подгрузить активности.</p>
        )}
      </section>

    </Panel>
  );
}

function renderReservationsTable(items: ClientReservation[], keyPrefix: string) {
  if (!items.length) {
    return <div className="empty-state">Нет записей.</div>;
  }
  return (
    <div className="table-container compact-table">
      <table className="data-table">
        <thead>
          <tr>
            <th>Дата</th>
            <th>Время</th>
            <th>Слот</th>
            <th>Стойка</th>
            <th>Тип</th>
            <th>Инструктор</th>
            <th>Велосипед</th>
            <th>Статус</th>
            <th>Заметки</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item) => {
            const slotDate = item.slot_date ? dayjs(item.slot_date).format("DD.MM.YYYY") : "—";
            const slotTime = item.start_time || "—";
            const stand = item.stand_display_name || item.stand_title || item.stand_code || "—";
            const status = (item.status || "").toLowerCase() || "—";
            const sessionKind = item.session_kind || "—";
            return (
              <tr key={`${keyPrefix}-${item.id}`}>
                <td>{slotDate}</td>
                <td>{slotTime}</td>
                <td>{item.label || "Слот"}</td>
                <td>{stand}</td>
                <td>{sessionKind}</td>
                <td>{item.instructor_name || "—"}</td>
                <td>{item.bike_title || "—"}</td>
                <td>{status}</td>
                <td>{item.notes || "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
