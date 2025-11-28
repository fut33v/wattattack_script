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
  IntervalsLinkRow,
  ClientSubscriptionsResponse,
  ClientSubscription,
  ClientBalanceResponse
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

const SUBSCRIPTION_PLAN_OPTIONS = [
  { code: "pack_4", label: "Абонемент на 4 занятия", sessions: 4, price: 2500 },
  { code: "pack_8", label: "Абонемент на 8 занятий", sessions: 8, price: 4500 },
  { code: "pack_12", label: "Абонемент на 12 занятий", sessions: 12, price: 6000 },
  { code: "unlimited", label: "Безлимит на месяц", sessions: null, price: 7500 },
  { code: "unlimited_self", label: "Безлимит на месяц самокрутки", sessions: null, price: 5500 }
] as const;

function findPlanDefaults(code: string) {
  return SUBSCRIPTION_PLAN_OPTIONS.find((plan) => plan.code === code);
}

function formatPriceRub(value?: number | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) return "—";
  return `${value.toLocaleString("ru-RU")} ₽`;
}

function formatDate(value?: string | null): string {
  if (!value) return "—";
  return dayjs(value).format("DD.MM.YYYY");
}

function formatAdjustmentReason(reason?: string | null): string {
  if (!reason) return "—";
  const map: Record<string, string> = {
    purchase: "Покупка",
    spend: "Списание",
    "top-up": "Пополнение",
    adjustment: "Корректировка"
  };
  return map[reason] ?? reason;
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
  const [subscriptionsExpanded, setSubscriptionsExpanded] = useState(false);
  const [reservationsExpanded, setReservationsExpanded] = useState(false);
  const [activitiesExpanded, setActivitiesExpanded] = useState(false);
  const [intervalsMessage, setIntervalsMessage] = useState<string | null>(null);
  const [subscriptionMessage, setSubscriptionMessage] = useState<string | null>(null);
  const [balanceExpanded, setBalanceExpanded] = useState(false);
  const [balanceTrainingExpanded, setBalanceTrainingExpanded] = useState(false);
  const [selectedPastReservations, setSelectedPastReservations] = useState<number[]>([]);
  const [planCode, setPlanCode] = useState<string>(SUBSCRIPTION_PLAN_OPTIONS[0].code);
  const planDefaults = useMemo(() => findPlanDefaults(planCode), [planCode]);
  const defaultStartDate = useMemo(() => dayjs().format("YYYY-MM-DD"), []);
  const defaultEndDate = useMemo(() => dayjs().add(30, "day").format("YYYY-MM-DD"), []);
  const toggleEdit = () => setEditExpanded((prev) => !prev);
  const toggleSubscriptions = () => setSubscriptionsExpanded((prev) => !prev);
  const toggleReservations = () => setReservationsExpanded((prev) => !prev);
  const toggleActivities = () => setActivitiesExpanded((prev) => !prev);
  const toggleBalance = () => setBalanceExpanded((prev) => !prev);
  const toggleBalanceTraining = () => setBalanceTrainingExpanded((prev) => !prev);

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
    enabled: isIdValid && (reservationsExpanded || subscriptionsExpanded || balanceExpanded)
  });

  const subscriptionsQuery = useQuery<ClientSubscriptionsResponse>({
    queryKey: ["client-subscriptions", clientId],
    queryFn: () => apiFetch<ClientSubscriptionsResponse>(`/api/clients/${clientId}/subscriptions`),
    enabled: isIdValid && subscriptionsExpanded
  });

  const balanceQuery = useQuery<ClientBalanceResponse>({
    queryKey: ["client-balance", clientId],
    queryFn: () => apiFetch<ClientBalanceResponse>(`/api/clients/${clientId}/balance`),
    enabled: isIdValid
  });
  const totalSessionsRemaining = subscriptionsQuery.data?.totals?.sessions_remaining ?? 0;
  const balanceRub = balanceQuery.data?.balance?.balance_rub ?? 0;
  const clientIncomeTotalRub = balanceQuery.data?.total_income_rub ?? 0;
  const selectedPastSet = useMemo(() => new Set(selectedPastReservations), [selectedPastReservations]);

  const reservationOptions = useMemo(() => {
    const options: { id: number; label: string; slotDate?: string | null }[] = [];
    const pushOption = (item: ClientReservation) => {
      const dateLabel = item.slot_date ? dayjs(item.slot_date).format("DD.MM") : "—";
      const timeLabel = item.start_time || "";
      const title = item.label || item.session_kind || "Тренировка";
      const stand = item.stand_display_name || item.stand_title || item.stand_code;
      const pieces = [dateLabel, timeLabel, title, stand].filter(Boolean).join(" · ");
      options.push({ id: item.id, label: pieces, slotDate: item.slot_date });
    };
    reservationsQuery.data?.upcoming?.forEach(pushOption);
    reservationsQuery.data?.past?.forEach(pushOption);
    return options;
  }, [reservationsQuery.data]);

  const pastReservations = reservationsQuery.data?.past ?? [];

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

  const addSubscriptionMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch(`/api/clients/${clientId}/subscriptions`, {
        method: "POST",
        body: payload
      }),
    onSuccess: () => {
      setSubscriptionMessage("Абонемент добавлен.");
      queryClient.invalidateQueries({ queryKey: ["client-subscriptions", clientId] });
    },
    onError: (error: unknown) => {
      const message = (error as ApiError)?.message ?? "Не удалось добавить абонемент.";
      setSubscriptionMessage(message);
    }
  });

  const adjustSubscriptionMutation = useMutation({
    mutationFn: (payload: { subscriptionId: number; delta: number; reason?: string; reservationId?: number }) =>
      apiFetch(`/api/clients/${clientId}/subscriptions/${payload.subscriptionId}/adjust`, {
        method: "POST",
        body: { delta_sessions: payload.delta, reason: payload.reason, reservation_id: payload.reservationId }
      }),
    onSuccess: () => {
      setSubscriptionMessage("Баланс абонемента обновлен.");
      queryClient.invalidateQueries({ queryKey: ["client-subscriptions", clientId] });
    },
    onError: (error: unknown) => {
      const message = (error as ApiError)?.message ?? "Не удалось обновить абонемент.";
      setSubscriptionMessage(message);
    }
  });

  const deleteSubscriptionMutation = useMutation({
    mutationFn: (subscriptionId: number) =>
      apiFetch(`/api/clients/${clientId}/subscriptions/${subscriptionId}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      setSubscriptionMessage("Абонемент удален.");
      queryClient.invalidateQueries({ queryKey: ["client-subscriptions", clientId] });
    },
    onError: (error: unknown) => {
      const message = (error as ApiError)?.message ?? "Не удалось удалить абонемент.";
      setSubscriptionMessage(message);
    }
  });

  const balanceMutation = useMutation({
    mutationFn: (payload: { delta_rub: number; reason?: string; reservation_id?: number | null; created_at?: string }) =>
      apiFetch(`/api/clients/${clientId}/balance`, {
        method: "POST",
        body: payload
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["client-balance", clientId] });
    }
  });

  const deleteBalanceMutation = useMutation({
    mutationFn: (adjustmentId: number) =>
      apiFetch(`/api/clients/${clientId}/balance/${adjustmentId}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["client-balance", clientId] });
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

  function parseIntField(value: FormDataEntryValue | null): number | null {
    if (value === null) return null;
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (!trimmed) return null;
      const parsed = Number(trimmed);
      return Number.isNaN(parsed) ? null : parsed;
    }
    const parsed = Number(value);
    return Number.isNaN(parsed) ? null : parsed;
  }

  function slugifyPlanCode(code: string) {
    const trimmed = code.trim().toLowerCase();
    if (!trimmed) return "";
    return trimmed
      .replace(/\s+/g, "_")
      .replace(/[^a-z0-9_]/g, "_")
      .replace(/_+/g, "_")
      .replace(/^_+|_+$/g, "");
  }

  function handleCreateSubscription(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubscriptionMessage(null);
    const formData = new FormData(event.currentTarget);
    const planName = (formData.get("plan_name") as string | null)?.trim() || planDefaults?.label || planCode;
    const derivedPlanCode = planCode || slugifyPlanCode(planName || "");
    if (!derivedPlanCode) {
      setSubscriptionMessage("Укажите название или выберите тип абонемента — нужен код плана.");
      return;
    }

    const payload: Record<string, unknown> = {
      plan_code: derivedPlanCode,
      plan_name: planName || derivedPlanCode,
      sessions_total: parseIntField(formData.get("sessions_total")),
      price_rub: parseIntField(formData.get("price_rub")),
      valid_from: (formData.get("valid_from") as string | null) || null,
      valid_until: (formData.get("valid_until") as string | null) || null,
      notes: (formData.get("notes") as string | null)?.trim() ?? ""
    };
    const customRemaining = parseIntField(formData.get("sessions_remaining"));
    if (customRemaining !== null) {
      payload.sessions_remaining = customRemaining;
    }
    addSubscriptionMutation.mutate(payload);
  }

  function handleAdjustSubscription(subscriptionId: number, delta: number, reason?: string, reservationId?: number) {
    setSubscriptionMessage(null);
    adjustSubscriptionMutation.mutate({ subscriptionId, delta, reason, reservationId });
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
            <div className="profile-main">
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
            <div className="profile-stats">
              <div className="profile-stat">
                <div className="meta-label">Остаток занятий</div>
                <div className="profile-stat__value">{totalSessionsRemaining}</div>
              </div>
              <div className="profile-stat">
                <div className="meta-label">Баланс ₽</div>
                <div className="profile-stat__value">{balanceRub.toLocaleString("ru-RU")}</div>
              </div>
              <div className="profile-stat">
                <div className="meta-label">Сколько принес денег</div>
                <div className="profile-stat__value">{clientIncomeTotalRub.toLocaleString("ru-RU")} ₽</div>
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
            {client.saddle_height && (
              <div>
                <dt>Седло</dt>
                <dd>{client.saddle_height}</dd>
              </div>
            )}
            {client.goal && (
              <div>
                <dt>Цель</dt>
                <dd>{client.goal}</dd>
              </div>
            )}
            {client.favorite_bike && (
              <div>
                <dt>Любимый велосипед</dt>
                <dd>{client.favorite_bike}</dd>
              </div>
            )}
            {client.pedals && (
              <div>
                <dt>Педали</dt>
                <dd>{client.pedals}</dd>
              </div>
            )}
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
            <div>
              <dt>Телеграм</dt>
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
        <div className="detail-card__header detail-card__header--clickable" onClick={toggleSubscriptions}>
          <h3>Абонементы</h3>
          <div className="detail-card__controls detail-card__controls--right">
            <button
              type="button"
              className="button secondary chevron-button"
              onClick={(event) => {
                event.stopPropagation();
                toggleSubscriptions();
              }}
              aria-label={subscriptionsExpanded ? "Свернуть абонементы" : "Показать абонементы"}
            >
              {subscriptionsExpanded ? "▲" : "▼"}
            </button>
            {subscriptionsExpanded && subscriptionsQuery.isLoading && <span className="meta-hint">Загружаем…</span>}
            {subscriptionsExpanded && subscriptionsQuery.isError && (
              <span className="form-error">Не удалось загрузить абонементы.</span>
            )}
          </div>
        </div>
        {subscriptionsExpanded ? (
          <>
            <div className="subscription-stats">
              <div className="stat-card">
                <div className="stat-label">Остаток занятий</div>
                <div className="stat-value">
                  {subscriptionsQuery.data?.totals?.sessions_remaining ?? 0}
                </div>
              </div>
              <div className="stat-card">
                <div className="stat-label">Абонементов в истории</div>
                <div className="stat-value">{subscriptionsQuery.data?.items.length ?? 0}</div>
              </div>
            </div>

            {subscriptionMessage && <div className="form-subsection__message">{subscriptionMessage}</div>}

            <div className="subscription-layout">
              <form className="subscription-form" onSubmit={handleCreateSubscription}>
                <div className="form-grid">
                  <label>
                    Тип абонемента
                    <select value={planCode} onChange={(event) => setPlanCode(event.target.value)}>
                      {SUBSCRIPTION_PLAN_OPTIONS.map((plan) => (
                        <option key={plan.code} value={plan.code}>
                          {plan.label}
                          {plan.price ? ` · ${plan.price.toLocaleString("ru-RU")} ₽` : ""}
                        </option>
                      ))}
                      <option value="">Свой вариант</option>
                    </select>
                  </label>
                  <label>
                    Название
                    <input
                      key={`plan-name-${planCode}`}
                      type="text"
                      name="plan_name"
                      placeholder="Например, Абонемент на 4 занятия"
                      defaultValue={planDefaults?.label ?? ""}
                    />
                  </label>
                </div>
                <div className="form-grid">
                  <label>
                    Занятий в пакете
                    <input
                      key={`sessions-total-${planCode}`}
                      type="number"
                      name="sessions_total"
                      min={0}
                      placeholder="4"
                      disabled={planDefaults?.sessions === null}
                      defaultValue={planDefaults?.sessions ?? ""}
                    />
                  </label>
                  <label>
                    Текущий остаток (опционально)
                    <input
                      key={`sessions-remaining-${planCode}`}
                      type="number"
                      name="sessions_remaining"
                      min={0}
                      placeholder={planDefaults?.sessions === null ? "Безлимит" : `${planDefaults?.sessions ?? ""}`}
                      disabled={planDefaults?.sessions === null}
                    />
                  </label>
                </div>
                <div className="form-grid">
                  <label>
                    Стоимость, ₽
                    <input
                      key={`price-${planCode}`}
                      type="number"
                      name="price_rub"
                      min={0}
                      placeholder="2500"
                      defaultValue={planDefaults?.price ?? ""}
                    />
                  </label>
                  <label>
                    Период действия
                    <div className="form-subgrid">
                      <input type="date" name="valid_from" defaultValue={defaultStartDate} />
                      <input type="date" name="valid_until" defaultValue={defaultEndDate} />
                    </div>
                  </label>
                </div>
                <label>
                  Заметка
                  <textarea name="notes" rows={2} placeholder="Например, оплата наличными / декабрьский абонемент" />
                </label>
                <div className="form-actions">
                  <button type="submit" className="button" disabled={addSubscriptionMutation.isPending}>
                    {addSubscriptionMutation.isPending ? "Сохраняем…" : "Добавить абонемент"}
                  </button>
                  {addSubscriptionMutation.isError && (
                    <span className="form-error">
                      {(addSubscriptionMutation.error as ApiError)?.message ?? "Не удалось сохранить абонемент."}
                    </span>
                  )}
                </div>
              </form>

              <div className="subscription-list">
                {(subscriptionsQuery.data?.items ?? []).length === 0 ? (
                  <div className="empty-state">
                    {subscriptionsQuery.isLoading ? "Загрузка…" : "Для клиента еще нет абонементов."}
                  </div>
                ) : (
                  (subscriptionsQuery.data?.items ?? []).map((sub: ClientSubscription) => (
                    <div className="subscription-card" key={sub.id}>
                      <div className="subscription-card__header">
                        <div>
                          <div className="subscription-title">{sub.plan_name}</div>
                          <div className="meta-hint">
                            #{sub.id} · {sub.created_at ? dayjs(sub.created_at).format("DD.MM.YYYY HH:mm") : "—"}
                          </div>
                        </div>
                        <div className="pill pill-muted">{sub.plan_code}</div>
                      </div>
                      <div className="subscription-meta">
                        <div>
                          <div className="meta-label">Остаток</div>
                          <div className="meta-value">
                            {sub.sessions_remaining !== null && sub.sessions_remaining !== undefined
                              ? sub.sessions_remaining
                              : "Безлимит"}
                          </div>
                        </div>
                        <div>
                          <div className="meta-label">Всего</div>
                          <div className="meta-value">
                            {sub.sessions_total !== null && sub.sessions_total !== undefined
                              ? sub.sessions_total
                              : "Безлимит"}
                          </div>
                        </div>
                        <div>
                          <div className="meta-label">Стоимость</div>
                          <div className="meta-value">{formatPriceRub(sub.price_rub)}</div>
                        </div>
                        <div>
                          <div className="meta-label">Период</div>
                          <div className="meta-value">
                            {formatDate(sub.valid_from)} — {formatDate(sub.valid_until)}
                          </div>
                        </div>
                      </div>
                      {sub.notes && <div className="meta-hint">Заметка: {sub.notes}</div>}
                      {sub.sessions_remaining !== null && sub.sessions_remaining !== undefined ? (
                        <div className="subscription-actions">
                          <div className="subscription-actions__quick">
                            <button
                              type="button"
                              className="button secondary"
                              disabled={adjustSubscriptionMutation.isPending}
                              onClick={() => handleAdjustSubscription(sub.id, 1, "top-up")}
                            >
                              +1 к балансу
                            </button>
                          </div>
                          <form
                            className="subscription-adjust-form"
                            onSubmit={(event) => {
                              event.preventDefault();
                              const formData = new FormData(event.currentTarget);
                              const slotValue = Number(formData.get("slot_id") || 0);
                              if (!slotValue) {
                                setSubscriptionMessage("Выберите слот для списания.");
                                return;
                              }
                              handleAdjustSubscription(sub.id, -1, undefined, slotValue);
                              event.currentTarget.reset();
                            }}
                          >
                            <label>
                              Слот
                              <select name="slot_id" defaultValue="">
                                <option value="">— выберите слот —</option>
                                {reservationOptions
                                  .filter((option) => {
                                    const slotDate = option.slotDate ? dayjs(option.slotDate) : null;
                                    const validFrom = sub.valid_from ? dayjs(sub.valid_from) : null;
                                    const validUntil = sub.valid_until ? dayjs(sub.valid_until) : null;
                                    if (slotDate && validFrom && slotDate.isBefore(validFrom, "day")) return false;
                                    if (slotDate && validUntil && slotDate.isAfter(validUntil, "day")) return false;
                                    return true;
                                  })
                                  .map((option) => (
                                    <option key={option.id} value={option.id}>
                                      {option.label}
                                    </option>
                                  ))}
                                {reservationOptions.length === 0 && (
                                  <option value="" disabled>
                                    Нет доступных записей
                                  </option>
                                )}
                              </select>
                            </label>
                            <button type="submit" className="button" disabled={adjustSubscriptionMutation.isPending}>
                              {adjustSubscriptionMutation.isPending ? "Списываем…" : "Списать тренировку"}
                            </button>
                          </form>
                        </div>
                      ) : (
                        <div className="pill pill-muted">Безлимитный доступ</div>
                      )}
                      <div className="subscription-adjustments">
                        <div className="meta-label">История списаний</div>
                        {sub.adjustments && sub.adjustments.length ? (
                          <div className="table-container compact-table">
                            <table className="data-table">
                              <thead>
                                <tr>
                                  <th>Δ занятий</th>
                                  <th>Слот</th>
                                  <th>Причина</th>
                                  <th>Когда</th>
                                </tr>
                              </thead>
                              <tbody>
                                {sub.adjustments.map((adj) => (
                                  <tr key={adj.id}>
                                    <td className={adj.delta_sessions < 0 ? "negative" : "positive"}>
                                      {adj.delta_sessions > 0 ? `+${adj.delta_sessions}` : adj.delta_sessions}
                                    </td>
                                    <td>{adj.reservation_label || "—"}</td>
                                    <td>{formatAdjustmentReason(adj.reason)}</td>
                                    <td>{adj.created_at ? dayjs(adj.created_at).format("DD.MM.YYYY HH:mm") : "—"}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <div className="empty-state">Списаний пока нет.</div>
                        )}
                        <div className="subscription-actions__quick">
                          <button
                            type="button"
                            className="button danger"
                            disabled={deleteSubscriptionMutation.isPending}
                            onClick={() => {
                              if (!window.confirm("Удалить этот абонемент?")) return;
                              deleteSubscriptionMutation.mutate(sub.id);
                            }}
                          >
                            {deleteSubscriptionMutation.isPending ? "Удаляем…" : "Удалить абонемент"}
                          </button>
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </>
        ) : null}
      </section>

      <section className="detail-card">
        <div className="detail-card__header detail-card__header--clickable" onClick={toggleBalance}>
          <h3>Баланс</h3>
          <div className="detail-card__controls detail-card__controls--right">
            <button
              type="button"
              className="button secondary chevron-button"
              onClick={(event) => {
                event.stopPropagation();
                toggleBalance();
              }}
              aria-label={balanceExpanded ? "Свернуть баланс" : "Показать баланс"}
            >
              {balanceExpanded ? "▲" : "▼"}
            </button>
          </div>
        </div>
        {balanceExpanded ? (
          <>
            <div className="subscription-stats">
              <div className="stat-card">
                <div className="stat-label">Баланс ₽</div>
                <div className="stat-value">{balanceRub.toLocaleString("ru-RU")}</div>
              </div>
            </div>
            <div className="balance-training-wrapper">
              <div className="detail-card__header detail-card__header--clickable" onClick={toggleBalanceTraining}>
                <h4>Учёт прошедших бронирований</h4>
                <div className="detail-card__controls detail-card__controls--right">
                  <button
                    type="button"
                    className="button secondary chevron-button"
                    onClick={(event) => {
                      event.stopPropagation();
                      toggleBalanceTraining();
                    }}
                    aria-label={balanceTrainingExpanded ? "Свернуть учёт тренировок" : "Показать учёт тренировок"}
                  >
                    {balanceTrainingExpanded ? "▲" : "▼"}
                  </button>
                </div>
              </div>
              {balanceTrainingExpanded ? (
                <div className="balance-training">
                  <form
                    className="subscription-adjust-form"
                    onSubmit={async (event) => {
                      event.preventDefault();
                      const formData = new FormData(event.currentTarget);
                      const amount = Number(formData.get("delta_rub") || 0);
                      if (!amount) {
                        setSubscriptionMessage("Укажите сумму списания за тренировку.");
                        return;
                      }
                      if (!selectedPastReservations.length) {
                        setSubscriptionMessage("Выберите хотя бы одно прошедшее бронирование.");
                        return;
                      }
                      try {
                        await Promise.all(
                          selectedPastReservations.map((reservationId) => {
                            const reservation = pastReservations.find((item) => item.id === reservationId);
                            const dateIso = reservation?.slot_date
                              ? dayjs(
                                  `${reservation.slot_date}${reservation.start_time ? `T${reservation.start_time}` : ""}`
                                ).toISOString()
                              : undefined;
                            const absAmount = Math.abs(amount);
                            return Promise.all([
                              balanceMutation.mutateAsync({
                                delta_rub: absAmount,
                                reason: "Пополнение за тренировку",
                                reservation_id: reservationId,
                                created_at: dateIso
                              }),
                              balanceMutation.mutateAsync({
                                delta_rub: -absAmount,
                                reason: "Списание за тренировку",
                                reservation_id: reservationId,
                                created_at: dateIso
                              })
                            ]);
                          })
                        );
                        setSubscriptionMessage("Учли выбранные бронирования в балансе.");
                        setSelectedPastReservations([]);
                        event.currentTarget.reset();
                        queryClient.invalidateQueries({ queryKey: ["client-balance", clientId] });
                      } catch (error) {
                        const message = (error as ApiError)?.message ?? "Не удалось учесть бронирование.";
                        setSubscriptionMessage(message);
                      }
                    }}
                  >
                    <label>
                      Сумма за тренировку (₽)
                      <input type="number" name="delta_rub" step="100" placeholder="500" />
                    </label>
                    <div className="balance-training__list">
                      {pastReservations.length === 0 ? (
                        <div className="empty-state">Нет прошедших бронирований.</div>
                      ) : (
                        pastReservations.map((item) => {
                          const label = [
                            item.slot_date ? dayjs(item.slot_date).format("DD.MM.YYYY") : "—",
                            item.start_time || "",
                            item.label || item.session_kind || "Тренировка",
                            item.stand_display_name || item.stand_title || item.stand_code
                          ]
                            .filter(Boolean)
                            .join(" · ");
                          const isChecked = selectedPastSet.has(item.id);
                          return (
                            <label key={item.id} className="checkbox-row">
                              <input
                                type="checkbox"
                                checked={isChecked}
                                onChange={(event) => {
                                  setSelectedPastReservations((prev) => {
                                    if (event.target.checked) return [...prev, item.id];
                                    return prev.filter((id) => id !== item.id);
                                  });
                                }}
                              />
                              <span>{label}</span>
                            </label>
                          );
                        })
                      )}
                    </div>
                    <button type="submit" className="button" disabled={balanceMutation.isPending}>
                      {balanceMutation.isPending ? "Сохраняем…" : "Учесть тренировку"}
                    </button>
                  </form>
                </div>
              ) : null}
            </div>
            <form
              className="subscription-adjust-form"
              onSubmit={(event) => {
                event.preventDefault();
                const formData = new FormData(event.currentTarget);
                const deltaValue = Number(formData.get("delta_rub") || 0);
                if (!deltaValue) {
                  setSubscriptionMessage("Укажите сумму изменения.");
                  return;
                }
                const reasonValue = (formData.get("reason") as string | null)?.trim() || undefined;
                const reservationId = Number(formData.get("slot_id") || 0) || undefined;
                balanceMutation.mutate({ delta_rub: deltaValue, reason: reasonValue, reservation_id: reservationId });
                event.currentTarget.reset();
              }}
            >
              <label>
                Сумма (₽)
                <input type="number" name="delta_rub" step="100" placeholder="500" />
              </label>
              <label>
                Причина
                <input type="text" name="reason" placeholder="пополнение / списание" />
              </label>
              <label>
                Слот (для списания)
                <select name="slot_id" defaultValue="">
                  <option value="">— без слота —</option>
                  {reservationOptions.map((option) => (
                    <option key={option.id} value={option.id}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <button type="submit" className="button" disabled={balanceMutation.isPending}>
                {balanceMutation.isPending ? "Сохраняем…" : "Применить"}
              </button>
            </form>
            <div className="subscription-adjustments">
              <div className="meta-label">История операций</div>
              {balanceQuery.data?.adjustments?.length ? (
                <div className="table-container compact-table">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Δ ₽</th>
                        <th>Причина</th>
                        <th>Слот</th>
                        <th>Когда</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {balanceQuery.data.adjustments.map((adj) => (
                        <tr key={adj.id}>
                          <td className={adj.delta_rub < 0 ? "negative" : "positive"}>
                            {adj.delta_rub > 0 ? `+${adj.delta_rub}` : adj.delta_rub}
                          </td>
                          <td>{adj.reason || "—"}</td>
                          <td>{adj.reservation_id ? `#${adj.reservation_id}` : "—"}</td>
                          <td>{adj.created_at ? dayjs(adj.created_at).format("DD.MM.YYYY HH:mm") : "—"}</td>
                          <td>
                            <button
                              type="button"
                              className="icon-button"
                              disabled={deleteBalanceMutation.isPending}
                              onClick={() => {
                                if (!window.confirm("Удалить операцию?")) return;
                                deleteBalanceMutation.mutate(adj.id);
                              }}
                              aria-label="Удалить операцию"
                            >
                              ×
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="empty-state">Операций пока нет.</div>
              )}
            </div>
          </>
        ) : null}
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
