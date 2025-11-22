import { FormEvent, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type {
  RaceCluster,
  RaceDetailResponse,
  RaceListResponse,
  RaceRegistration,
  RaceRow,
  ClientRow
} from "../lib/types";

const STATUS_OPTIONS = [
  { value: "pending", label: "Ожидание" },
  { value: "approved", label: "Подтверждена" },
  { value: "rejected", label: "Отклонена" }
] as const;

function formatDate(value?: string | null) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric" });
}

function formatDateTime(value?: string | null) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

type ClusterDraft = {
  label: string;
  code?: string;
  start_time?: string;
  end_time?: string;
};

type RaceSlotsResponse = {
  created: number;
  slot_ids: number[];
  race_date?: string;
  week_id?: number;
  week_start_date?: string;
  skipped_missing_time: string[];
  duplicates: string[];
  errors: string[];
};

type RaceSeatClusterResult = {
  cluster: string;
  code?: string | null;
  slot_id?: number | null;
  slot_label?: string | null;
  start_time?: string | null;
  requested: number;
  placed: number;
  already?: number;
  unplaced: string[];
};

type RaceSeatResponse = {
  placed: number;
  total: number;
  cluster_results: RaceSeatClusterResult[];
  missing_slots: string[];
  unplaced_clients: string[];
  skipped_online: number;
  skipped_missing_cluster: number;
  skipped_unknown_cluster: number;
  already_assigned: number;
  race_date?: string;
  week_id?: number;
  slot_ids?: number[];
};

function buildClustersPayload(drafts: ClusterDraft[]): RaceCluster[] {
  return drafts
    .map((draft) => ({
      label: draft.label.trim(),
      code: (draft.code ?? "").trim() || undefined,
      start_time: (draft.start_time ?? "").trim() || undefined,
      end_time: (draft.end_time ?? "").trim() || undefined
    }))
    .filter((item) => item.label);
}

function formatPrice(value?: number | null) {
  if (!value && value !== 0) return "";
  return new Intl.NumberFormat("ru-RU").format(value);
}

function formatClientLabel(client: ClientRow) {
  const first = (client.first_name ?? "").trim();
  const last = (client.last_name ?? "").trim();
  const full = (client.full_name ?? "").trim();
  const display = full || [first, last].filter(Boolean).join(" ").trim() || `ID ${client.id}`;
  return `${display} (ID ${client.id})`;
}

type RacePayload = {
  title: string;
  race_date: string;
  price_rub: number;
  sbp_phone: string;
  payment_instructions?: string | null;
  notes?: string | null;
  description?: string | null;
  is_active: boolean;
  clusters: RaceCluster[];
  slug?: string | null;
};

function readRacePayload(form: HTMLFormElement, clusters: RaceCluster[]): RacePayload {
  const formData = new FormData(form);
  const title = String(formData.get("title") ?? "").trim();
  const raceDate = String(formData.get("race_date") ?? "").trim();
  const priceValue = formData.get("price_rub");
  const price = typeof priceValue === "string" ? Number(priceValue) : Number(priceValue ?? 0);
  const sbp = String(formData.get("sbp_phone") ?? "").trim();
  const paymentInstructions = String(formData.get("payment_instructions") ?? "").trim();
  const notes = String(formData.get("notes") ?? "").trim();
  const description = String(formData.get("description") ?? "").trim();
  const slug = String(formData.get("slug") ?? "").trim();
  const isActive = formData.get("is_active") === "on";
  return {
    title,
    race_date: raceDate,
    price_rub: Number.isNaN(price) ? 0 : price,
    sbp_phone: sbp,
    payment_instructions: paymentInstructions || null,
    notes: notes || null,
    description: description || null,
    is_active: isActive,
    clusters,
    slug: slug || null
  };
}

export default function RacesPage() {
  const queryClient = useQueryClient();
  const [selectedRaceId, setSelectedRaceId] = useState<number | "new" | null>(null);
  const [updatingRegistrationId, setUpdatingRegistrationId] = useState<number | null>(null);
  const [deletingRegistrationId, setDeletingRegistrationId] = useState<number | null>(null);
  const [clusterDrafts, setClusterDrafts] = useState<ClusterDraft[]>([]);
  const [slotCreationResult, setSlotCreationResult] = useState<RaceSlotsResponse | null>(null);
  const [seatResult, setSeatResult] = useState<RaceSeatResponse | null>(null);

  const racesQuery = useQuery<RaceListResponse>({
    queryKey: ["races"],
    queryFn: () => apiFetch<RaceListResponse>("/api/races")
  });
  const races = racesQuery.data?.items ?? [];
  const [clientSearch, setClientSearch] = useState<string>("");
  const [selectedClientId, setSelectedClientId] = useState<number | null>(null);

  useEffect(() => {
    if (selectedRaceId === "new") {
      return;
    }
    if (typeof selectedRaceId === "number") {
      const exists = races.some((race) => race.id === selectedRaceId);
      if (!exists) {
        setSelectedRaceId(races[0]?.id ?? null);
      }
      return;
    }
    if (races.length > 0) {
      setSelectedRaceId(races[0].id);
    }
  }, [races, selectedRaceId]);

  const detailQuery = useQuery<RaceDetailResponse>({
    queryKey: ["races", selectedRaceId],
    queryFn: () => apiFetch<RaceDetailResponse>(`/api/races/${selectedRaceId}`),
    enabled: typeof selectedRaceId === "number"
  });

  const clientsQuery = useQuery<{ items: ClientRow[] }>({
    queryKey: ["clients", "search", clientSearch],
    queryFn: () => apiFetch<{ items: ClientRow[] }>(`/api/clients?page=1&search=${encodeURIComponent(clientSearch || "")}`),
    enabled: typeof selectedRaceId === "number"
  });

  const clientOptions = useMemo(() => {
    const items = clientsQuery.data?.items ?? [];
    return items.map((client) => ({
      value: client.id,
      label: formatClientLabel(client)
    }));
  }, [clientsQuery.data]);

  const selectedRace = useMemo<RaceRow | null>(() => {
    if (typeof selectedRaceId !== "number") return null;
    return races.find((race) => race.id === selectedRaceId) ?? null;
  }, [selectedRaceId, races]);

  const selectedRaceDetail = detailQuery.data?.item ?? null;

  useEffect(() => {
    if (selectedRaceId === "new") {
      setClusterDrafts([]);
      setSlotCreationResult(null);
      setSeatResult(null);
      return;
    }
    setSlotCreationResult(null);
    setSeatResult(null);
    setClusterDrafts([]);
  }, [selectedRaceId]);

  useEffect(() => {
    if (selectedRaceId === "new") {
      return;
    }
    if (selectedRaceDetail?.clusters) {
      setClusterDrafts(
        (selectedRaceDetail.clusters ?? []).map((cluster) => ({
          label: cluster.label,
          code: cluster.code ?? undefined,
          start_time: cluster.start_time ?? undefined,
          end_time: cluster.end_time ?? undefined
        }))
      );
    }
  }, [selectedRaceDetail, selectedRaceId]);

  const shareUrl = useMemo(() => {
    if (!selectedRaceDetail?.slug) return "";
    if (typeof window !== "undefined" && window.location?.origin) {
      return `${window.location.origin}/race/${selectedRaceDetail.slug}`;
    }
    return `/race/${selectedRaceDetail.slug}`;
  }, [selectedRaceDetail]);

  const createMutation = useMutation({
    mutationFn: (payload: RacePayload) =>
      apiFetch<{ item: RaceRow }>("/api/races", {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ["races"] });
      const newId = data?.item?.id;
      if (newId) {
        setSelectedRaceId(newId);
      } else {
        setSelectedRaceId(null);
      }
    }
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: Partial<RacePayload> }) =>
      apiFetch<{ item: RaceRow }>(`/api/races/${id}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ["races"] });
      queryClient.invalidateQueries({ queryKey: ["races", variables.id] });
    }
  });

  const createSlotsMutation = useMutation({
    mutationFn: (raceId: number) =>
      apiFetch<RaceSlotsResponse>(`/api/races/${raceId}/schedule/slots`, {
        method: "POST"
      }),
    onSuccess: (result) => {
      setSlotCreationResult(result);
      setSeatResult(null);
    },
    onError: () => {
      setSlotCreationResult(null);
    }
  });

  const seatParticipantsMutation = useMutation({
    mutationFn: (raceId: number) =>
      apiFetch<RaceSeatResponse>(`/api/races/${raceId}/schedule/seat`, {
        method: "POST"
      }),
    onSuccess: (result) => {
      setSeatResult(result);
    },
    onError: () => {
      setSeatResult(null);
    }
  });

  const registrationMutation = useMutation({
    mutationFn: ({
      raceId,
      registrationId,
      payload
    }: {
      raceId: number;
      registrationId: number;
      payload: { status?: string; cluster_code?: string | null; notes?: string | null };
    }) =>
      apiFetch(`/api/races/${raceId}/registrations/${registrationId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onMutate: (variables) => {
      setUpdatingRegistrationId(variables.registrationId);
    },
    onSettled: (_, __, variables) => {
      setUpdatingRegistrationId(null);
      if (variables?.raceId) {
        queryClient.invalidateQueries({ queryKey: ["races", variables.raceId] });
        queryClient.invalidateQueries({ queryKey: ["races"] });
      }
    }
  });

  const deleteRegistrationMutation = useMutation({
    mutationFn: ({ raceId, registrationId }: { raceId: number; registrationId: number }) =>
      apiFetch(`/api/races/${raceId}/registrations/${registrationId}`, {
        method: "DELETE"
      }),
    onMutate: (variables) => {
      setDeletingRegistrationId(variables.registrationId);
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ["races", variables.raceId] });
      queryClient.invalidateQueries({ queryKey: ["races"] });
    },
    onSettled: () => {
      setDeletingRegistrationId(null);
    }
  });

  const createRegistrationMutation = useMutation({
    mutationFn: ({ raceId, payload }: { raceId: number; payload: { client_id: number; race_mode?: string | null } }) =>
      apiFetch<{ item: RaceRegistration }>(`/api/races/${raceId}/registrations`, {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ["races", variables.raceId] });
      queryClient.invalidateQueries({ queryKey: ["races"] });
    }
  });

  function handleCreateSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const payload = readRacePayload(event.currentTarget, buildClustersPayload(clusterDrafts));
    createMutation.mutate(payload, {
      onSuccess: () => {
        event.currentTarget.reset();
        setClusterDrafts([]);
      }
    });
  }

  function handleUpdateSubmit(event: FormEvent<HTMLFormElement>, raceId: number) {
    event.preventDefault();
    const payload = readRacePayload(event.currentTarget, buildClustersPayload(clusterDrafts));
    updateMutation.mutate({ id: raceId, payload });
  }

  function handleCreateSlotsClick() {
    if (typeof selectedRaceId !== "number") return;
    createSlotsMutation.mutate(selectedRaceId);
  }

  function handleSeatParticipantsClick() {
    if (typeof selectedRaceId !== "number") return;
    seatParticipantsMutation.mutate(selectedRaceId);
  }

  function handleSelectRace(raceId: number | "new") {
    setSelectedRaceId(raceId);
  }

  function handleRegistrationSave(
    registration: RaceRegistration,
    payload: { status?: string; cluster_code?: string | null; notes?: string | null; race_mode?: string | null }
  ) {
    if (typeof selectedRaceId !== "number") return;
    registrationMutation.mutate({
      raceId: selectedRaceId,
      registrationId: registration.id,
      payload
    });
  }

  function handleRegistrationDelete(registration: RaceRegistration) {
    if (typeof selectedRaceId !== "number") return;
    if (!window.confirm("Удалить участника из списка?")) {
      return;
    }
    deleteRegistrationMutation.mutate({ raceId: selectedRaceId, registrationId: registration.id });
  }

  function handleAddParticipantSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (typeof selectedRaceId !== "number") return;
    const formData = new FormData(event.currentTarget);
    const mode = String(formData.get("race_mode") ?? "").trim();
    const clientId = selectedClientId || Number(formData.get("client_id"));
    if (!clientId || Number.isNaN(clientId)) {
      return;
    }
    createRegistrationMutation.mutate(
      {
        raceId: selectedRaceId,
        payload: {
          client_id: clientId,
          race_mode: mode || undefined
        }
      },
      {
        onSuccess: () => {
          (event.target as HTMLFormElement).reset();
          setSelectedClientId(null);
          setClientSearch("");
        }
      }
    );
  }

  const registrations = selectedRaceDetail?.registrations ?? [];
  const clusters = selectedRaceDetail?.clusters ?? [];

  return (
    <div className="races-page">
      <Panel
        title="Гонки"
        subtitle="Создайте новую гонку или отредактируйте текущую"
        headerExtra={
          <button
            type="button"
            className={`button ${selectedRaceId === "new" ? "active" : ""}`}
            onClick={() => handleSelectRace(selectedRaceId === "new" ? races[0]?.id ?? null : "new")}
          >
            {selectedRaceId === "new" ? "Отменить" : "Новая гонка"}
          </button>
        }
      >
        <div className="races-layout">
          <aside className="races-list">
            {racesQuery.isLoading ? (
              <div className="empty-state">Загружаем гонки…</div>
            ) : races.length === 0 ? (
              <div className="empty-state">Пока нет созданных гонок.</div>
            ) : (
              <div className="races-list-items">
        {races.map((race) => (
          <button
            type="button"
            key={race.id}
            className={`race-list-item ${race.id === selectedRaceId ? "active" : ""}`}
            onClick={() => handleSelectRace(race.id)}
          >
            <span className="race-list-title">{race.title}</span>
            <span className="race-list-meta">Дата: {formatDate(race.race_date)}</span>
            <span className="race-list-meta">Стоимость: {formatPrice(race.price_rub)} ₽</span>
            <span className="race-list-meta">
              {Number(race.pending_count ?? 0)} ожидание · {Number(race.approved_count ?? 0)} подтверждено
            </span>
            <span className="race-list-meta">Ссылка: /race/{race.slug}</span>
            <span className="race-list-meta">
              <a className="race-slug-link" href={`/app/race/summary/${race.id}`}>
                Сводка
              </a>
            </span>
            {!race.is_active && <span className="race-list-tag">архив</span>}
          </button>
        ))}
              </div>
            )}
          </aside>
          <section className="race-details">
            {selectedRaceId === "new" ? (
              <form className="race-form" onSubmit={handleCreateSubmit}>
                <h3>Новая гонка</h3>
                <div className="form-grid">
                  <label>
                    Название
                    <input type="text" name="title" placeholder="Весенний старт" required />
                  </label>
                  <label>
                    Дата
                    <input type="date" name="race_date" required />
                  </label>
                  <label>
                    Стоимость (₽)
                    <input type="number" name="price_rub" min={0} inputMode="numeric" required />
                  </label>
                  <label>
                    Номер для СБП
                    <input type="text" name="sbp_phone" placeholder="+7..." required />
                  </label>
                  <label>
                    Слаг (для публичной страницы)
                    <input type="text" name="slug" placeholder="spring-race" />
                  </label>
                  <label className="checkbox-field">
                    <input type="checkbox" name="is_active" defaultChecked />
                    Активная гонка
                  </label>
                </div>
                <label>
                  Инструкции по оплате
                  <textarea name="payment_instructions" rows={3} placeholder="Переведите по СБП и пришлите скрин." />
                </label>
                <label>
                  Описание
                  <textarea name="description" rows={3} placeholder="Коротко о формате, регламенте, трассе" />
                </label>
                <div className="clusters-section">
                  <div className="clusters-section-header">
                    <div>Кластеры</div>
                    <div className="clusters-section-subtitle">Название, код и время старта/финиша</div>
                  </div>
                  <ClustersEditor value={clusterDrafts} onChange={setClusterDrafts} />
                </div>
                <label>
                  Примечание
                  <textarea name="notes" rows={2} placeholder="Дополнительные детали" />
                </label>
                <button type="submit" className="button primary" disabled={createMutation.isPending}>
                  {createMutation.isPending ? "Сохраняем…" : "Создать гонку"}
                </button>
              </form>
            ) : selectedRace && selectedRaceDetail ? (
              <>
                <form className="race-form" key={selectedRaceDetail.id} onSubmit={(event) => handleUpdateSubmit(event, selectedRaceDetail.id)}>
                  <div className="race-form-header">
                    <h3>{selectedRaceDetail.title}</h3>
                    {selectedRaceDetail.slug && (
                      <a className="race-slug-link" href={`/race/${selectedRaceDetail.slug}`} target="_blank" rel="noreferrer">
                        Открыть публичную страницу
                      </a>
                    )}
                    <a className="race-slug-link" href={`/app/race/summary/${selectedRaceDetail.id}`}>
                      Сводка гонки
                    </a>
                  </div>
                  <div className="form-grid">
                    <label>
                      Название
                      <input type="text" name="title" defaultValue={selectedRaceDetail.title} required />
                    </label>
                    <label>
                      Дата
                      <input type="date" name="race_date" defaultValue={selectedRaceDetail.race_date} required />
                    </label>
                    <label>
                      Стоимость (₽)
                      <input type="number" name="price_rub" min={0} defaultValue={selectedRaceDetail.price_rub} required />
                    </label>
                    <label>
                      Номер для СБП
                      <input type="text" name="sbp_phone" defaultValue={selectedRaceDetail.sbp_phone} required />
                    </label>
                    <label>
                      Слаг (для публичной страницы)
                      <input type="text" name="slug" defaultValue={selectedRaceDetail.slug} required />
                    </label>
                    <label className="checkbox-field">
                      <input type="checkbox" name="is_active" defaultChecked={selectedRaceDetail.is_active} />
                      Активная гонка
                    </label>
                  </div>
                  <label>
                    Инструкции по оплате
                    <textarea name="payment_instructions" rows={3} defaultValue={selectedRaceDetail.payment_instructions ?? ""} />
                  </label>
                  <label>
                    Описание
                    <textarea name="description" rows={3} defaultValue={selectedRaceDetail.description ?? ""} />
                  </label>
                  <div className="clusters-section">
                    <div className="clusters-section-header">
                      <div>Кластеры</div>
                      <div className="clusters-section-subtitle">Название, код и время старта/финиша</div>
                    </div>
                    <ClustersEditor value={clusterDrafts} onChange={setClusterDrafts} />
                  </div>
                  <div className="race-slot-actions">
                    <div>
                      <div className="clusters-section-subtitle">Создаст слоты в расписании по времени кластеров этого старта.</div>
                      <div className="clusters-section-subtitle">Сначала сохраните изменения по кластерам, затем нажмите кнопку.</div>
                    </div>
                    <button
                      type="button"
                      className="button"
                      onClick={handleCreateSlotsClick}
                      disabled={createSlotsMutation.isPending}
                    >
                      {createSlotsMutation.isPending ? "Создаём слоты…" : "Создать слоты в расписании"}
                    </button>
                  </div>
                  {slotCreationResult && (
                    <div className="race-slot-result">
                      <div>
                        Создано слотов: {slotCreationResult.created}. Недоступно времени: {slotCreationResult.skipped_missing_time.length}. Дубликаты: {slotCreationResult.duplicates.length}.
                      </div>
                      {slotCreationResult.errors.length > 0 && <div>Ошибки: {slotCreationResult.errors.join(", ")}</div>}
                      {slotCreationResult.skipped_missing_time.length > 0 && (
                        <div>Без времени: {slotCreationResult.skipped_missing_time.join(", ")}</div>
                      )}
                      {slotCreationResult.duplicates.length > 0 && (
                        <div>Уже существовали: {slotCreationResult.duplicates.join(", ")}</div>
                      )}
                      {slotCreationResult.week_start_date && (
                        <div>Неделя расписания: {formatDate(slotCreationResult.week_start_date)}</div>
                      )}
                    </div>
                  )}
                  <div className="race-slot-actions">
                    <div>
                      <div className="clusters-section-subtitle">Рассадить подтверждённых участников по слотам их кластера.</div>
                      <div className="clusters-section-subtitle">Учитываем любимый велосипед и рост, если доступны.</div>
                    </div>
                    <button
                      type="button"
                      className="button"
                      onClick={handleSeatParticipantsClick}
                      disabled={seatParticipantsMutation.isPending}
                    >
                      {seatParticipantsMutation.isPending ? "Рассаживаем…" : "Рассадить участников"}
                    </button>
                  </div>
                  {seatResult && (
                    <div className="race-slot-result">
                      <div>
                        Рассажено {seatResult.placed} из {seatResult.total}. Онлайн пропущено: {seatResult.skipped_online}. Без кластера: {seatResult.skipped_missing_cluster}.
                      </div>
                      {seatResult.missing_slots.length > 0 && (
                        <div>Нет слотов для: {seatResult.missing_slots.join(", ")}</div>
                      )}
                      {seatResult.cluster_results.length > 0 && (
                        <div>
                          Кластеры:
                          <ul>
                            {seatResult.cluster_results.map((result) => (
                              <li key={result.code ?? result.cluster}>
                                {result.cluster}: {result.placed}/{result.requested}
                                {result.unplaced.length > 0 ? ` · не поместились: ${result.unplaced.join(", ")}` : ""}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                      {seatResult.unplaced_clients.length > 0 && (
                        <div>Не удалось рассадить: {seatResult.unplaced_clients.join(", ")}</div>
                      )}
                    </div>
                  )}
                  <label>
                    Примечание
                    <textarea name="notes" rows={2} defaultValue={selectedRaceDetail.notes ?? ""} />
                  </label>
                  <button type="submit" className="button primary" disabled={updateMutation.isPending}>
                    {updateMutation.isPending ? "Сохраняем…" : "Обновить гонку"}
                  </button>
                </form>
                {selectedRaceDetail.slug && (
                  <div className="race-share">
                    <div className="race-share-label">Публичная ссылка</div>
                    <code>{shareUrl}</code>
                    <a className="button" href={`/race/${selectedRaceDetail.slug}`} target="_blank" rel="noreferrer">
                      Открыть страницу
                    </a>
                  </div>
                )}
                <div className="race-registrations">
                  <h4>Заявки ({registrations.length})</h4>
                  <form className="inline-form" onSubmit={handleAddParticipantSubmit}>
                    <label>
                      Найти клиента
                      <input
                        type="search"
                        name="client_query"
                        placeholder="Имя или фамилия"
                        value={clientSearch}
                        onChange={(event) => setClientSearch(event.target.value)}
                      />
                    </label>
                    <label>
                      Клиент
                      <select
                        name="client_id"
                        value={selectedClientId ?? ""}
                        onChange={(event) => setSelectedClientId(event.target.value ? Number(event.target.value) : null)}
                        required
                      >
                        <option value="">Выберите клиента</option>
                        {clientOptions.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Формат
                      <select name="race_mode" defaultValue="offline">
                        <option value="offline">Оффлайн</option>
                        <option value="online">Онлайн</option>
                      </select>
                    </label>
                    <button type="submit" className="button" disabled={createRegistrationMutation.isPending}>
                      {createRegistrationMutation.isPending ? "Добавляем…" : "Добавить участника"}
                    </button>
                  </form>
                  {registrations.length === 0 ? (
                    <div className="empty-state">Заявок пока нет.</div>
                  ) : (
                    <div className="race-registrations-list">
                      {registrations.map((registration) => (
                        <RegistrationRow
                          key={registration.id}
                          registration={registration}
                          clusters={clusters}
                          isSaving={updatingRegistrationId === registration.id && registrationMutation.isPending}
                          onSave={handleRegistrationSave}
                          onDelete={handleRegistrationDelete}
                          isDeleting={deleteRegistrationMutation.isPending && deletingRegistrationId === registration.id}
                        />
                      ))}
                    </div>
                  )}
                </div>
              </>
            ) : (
              <div className="empty-state">Выберите гонку слева или создайте новую.</div>
            )}
          </section>
        </div>
      </Panel>
    </div>
  );
}

function ClustersEditor({ value, onChange }: { value: ClusterDraft[]; onChange: (drafts: ClusterDraft[]) => void }) {
  const drafts = value ?? [];

  function handleChange(index: number, field: keyof ClusterDraft, next: string) {
    const updated = [...drafts];
    const existing = updated[index] ?? { label: "", code: "", start_time: "", end_time: "" };
    updated[index] = { ...existing, [field]: next };
    onChange(updated);
  }

  function handleAdd() {
    onChange([...(drafts ?? []), { label: "", code: "", start_time: "", end_time: "" }]);
  }

  function handleRemove(index: number) {
    const updated = drafts.filter((_, idx) => idx !== index);
    onChange(updated);
  }

  return (
    <div className="clusters-editor">
      {drafts.length === 0 ? <div className="clusters-empty">Добавьте хотя бы один кластер.</div> : null}
      {drafts.map((cluster, index) => (
        <div className="cluster-row" key={index}>
          <input
            type="text"
            value={cluster.label}
            onChange={(event) => handleChange(index, "label", event.target.value)}
            placeholder="Например, A — быстрые"
            required
          />
          <input
            type="text"
            value={cluster.code ?? ""}
            onChange={(event) => handleChange(index, "code", event.target.value)}
            placeholder="Код (A/B/C)"
          />
          <input
            type="time"
            value={cluster.start_time ?? ""}
            onChange={(event) => handleChange(index, "start_time", event.target.value)}
            placeholder="Старт"
          />
          <input
            type="time"
            value={cluster.end_time ?? ""}
            onChange={(event) => handleChange(index, "end_time", event.target.value)}
            placeholder="Финиш"
          />
          <button type="button" className="button danger" onClick={() => handleRemove(index)}>
            Удалить
          </button>
        </div>
      ))}
      <button type="button" className="button" onClick={handleAdd}>
        Добавить кластер
      </button>
    </div>
  );
}

interface RegistrationRowProps {
  registration: RaceRegistration;
  clusters: RaceCluster[];
  isSaving: boolean;
  onSave: (
    registration: RaceRegistration,
    payload: { status: string; cluster_code: string | null; notes: string | null; race_mode: string | null }
  ) => void;
  onDelete: (registration: RaceRegistration) => void;
  isDeleting: boolean;
}

function RegistrationRow({ registration, clusters, isSaving, onSave, onDelete, isDeleting }: RegistrationRowProps) {
  const [status, setStatus] = useState(registration.status);
  const [clusterCode, setClusterCode] = useState(registration.cluster_code ?? "");
  const [notes, setNotes] = useState(registration.notes ?? "");
  const [mode, setMode] = useState(registration.race_mode ?? "");
  const hasTelegram = typeof registration.tg_user_id === "number" && !Number.isNaN(registration.tg_user_id);
  const usernameLabel = registration.tg_username ? `@${registration.tg_username}` : "—";
  const tgIdLabel = hasTelegram ? registration.tg_user_id : "—";

  useEffect(() => {
    setStatus(registration.status);
    setClusterCode(registration.cluster_code ?? "");
    setNotes(registration.notes ?? "");
    setMode(registration.race_mode ?? "");
  }, [registration.id, registration.status, registration.cluster_code, registration.notes, registration.race_mode]);

  function handleSave() {
    onSave(registration, {
      status,
      cluster_code: clusterCode || null,
      notes: notes.trim() || null,
      race_mode: mode || null
    });
  }

  const modeLabel =
    mode === "online"
      ? "Формат: Онлайн (у себя дома)"
      : mode === "offline"
        ? "Формат: Оффлайн (в Крутилке)"
        : "Формат: не выбран";

  const bikePreference =
    mode === "online"
      ? "Онлайн участие — велосипед не требуется"
      : registration.bring_own_bike === true
        ? "Со своим велосипедом"
        : registration.bring_own_bike === false
          ? "Нужен студийный велосипед (тип и передачи не требуются)"
          : "Предпочтение не указано";

  const axleLabel =
    mode === "online"
      ? "Онлайн — тип крепления не нужен"
      : registration.bring_own_bike === false
        ? "Студийный велосипед"
        : registration.axle_type || "Тип оси не указана";
  const gearsLabel =
    mode === "online"
      ? "Онлайн — передачи не требуются"
      : registration.bring_own_bike === false
        ? "Студийный велосипед"
        : registration.gears_label || "Передачи не указаны";

  return (
    <div className="race-registration-row">
      <div>
        <div className="race-registration-name">{registration.client_name ?? `Клиент #${registration.client_id}`}</div>
        <div className="race-registration-meta">
          Пользователь: {usernameLabel} · ID {tgIdLabel}
        </div>
        {!hasTelegram && (
          <div className="race-registration-meta">Telegram не привязан — уведомления не отправляются.</div>
        )}
        <div className="race-registration-meta">Отправлено: {formatDateTime(registration.payment_submitted_at)}</div>
        <div className="race-registration-meta">{modeLabel}</div>
        <div className="race-registration-meta">{bikePreference}</div>
        <div className="race-registration-meta">
          {axleLabel} · {gearsLabel}
        </div>
      </div>
      <div className="race-registration-controls">
        <label>
          Формат участия
          <select value={mode} onChange={(event) => setMode(event.target.value)}>
            <option value="">Не выбран</option>
            <option value="offline">Оффлайн (в Крутилке)</option>
            <option value="online">Онлайн (у себя дома)</option>
          </select>
        </label>
        <label>
          Статус
          <select value={status} onChange={(event) => setStatus(event.target.value)}>
            {STATUS_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          Кластер
          <select value={clusterCode} onChange={(event) => setClusterCode(event.target.value)}>
            <option value="">Не назначен</option>
            {clusters.map((cluster) => (
              <option key={cluster.code ?? cluster.label} value={cluster.code ?? cluster.label}>
                {cluster.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          Примечание
          <input type="text" value={notes} onChange={(event) => setNotes(event.target.value)} placeholder="Комментарий" />
        </label>
        <button type="button" className="button" onClick={handleSave} disabled={isSaving}>
          {isSaving ? "Сохраняем…" : "Сохранить"}
        </button>
        <button type="button" className="button danger" onClick={() => onDelete(registration)} disabled={isDeleting}>
          {isDeleting ? "Удаляем…" : "Удалить"}
        </button>
      </div>
    </div>
  );
}
