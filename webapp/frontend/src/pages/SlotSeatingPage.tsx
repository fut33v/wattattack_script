import classNames from "classnames";
import { useMemo, useState, useEffect } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { useAppContext } from "../lib/AppContext";
import { ApiError, apiFetch } from "../lib/api";
import type {
  ScheduleReservation,
  ScheduleSlot,
  ScheduleSlotDetailResponse,
  ScheduleStandSummary,
  ScheduleWeekDetailResponse,
  ClientRow,
  ClientListResponse,
  SlotCopyTargetsResponse,
  SlotCopyResponse
} from "../lib/types";

import "../styles/schedule.css";

const RESERVATION_STATUS_LABELS: Record<string, string> = {
  available: "Свободно",
  booked: "Занято",
  cancelled: "Отменено",
  pending: "Ожидание",
  waitlist: "Лист ожидания",
  blocked: "Заблокировано",
  legacy: "История",
  hold: "Держим"
};

type MovePayload = {
  reservationId: number;
  standId: number | null;
  swapReservationId?: number;
};

type MoveResponse = {
  reservation: ScheduleReservation;
  slot?: ScheduleSlot;
};

function SeatCard({
  reservation,
  label
}: {
  reservation: ScheduleReservation;
  label: string;
}) {
  const statusLabel = RESERVATION_STATUS_LABELS[reservation.status] ?? reservation.status;
  const heightLabel = reservation.client_height ? `${reservation.client_height} см` : null;

  return (
    <div
      className={classNames("slot-seat-card", reservation.status)}
      draggable
      onDragStart={(event) => {
        event.dataTransfer.setData("text/reservation-id", String(reservation.id));
      }}
    >
      <div className="slot-seat-name">{reservation.client_name || "—"}</div>
      {heightLabel ? <div className="slot-seat-meta">{heightLabel}</div> : null}
      <div className="slot-seat-status">{statusLabel}</div>
    </div>
  );
}

function StandTile({
  stand,
  reservation,
  onDrop,
  isActive,
  onHover,
  onAssignClick,
  showAssign,
  searchTerm,
  onSearchChange,
  searchResults,
  onSelectClient,
  isAssigning
}: {
  stand: ScheduleStandSummary | null;
  reservation: ScheduleReservation | null;
  onDrop: (reservationId: number, targetStandId: number | null) => void;
  isActive: boolean;
  onHover?: (standId: number | null) => void;
  onAssignClick?: () => void;
  showAssign?: boolean;
  searchTerm?: string;
  onSearchChange?: (value: string) => void;
  searchResults?: ClientRow[];
  onSelectClient?: (client: ClientRow) => void;
  isAssigning?: boolean;
}) {
  const standId = stand?.id ?? null;
  const primaryLabel = stand ? stand.code || `Станок ${stand.id}` : "Без станка";
  const secondaryLabel = stand?.display_name || stand?.title || null;
  const bikeLabel = stand?.bike_title || null;
  const heightMin = stand?.bike_height_min_cm;
  const heightMax = stand?.bike_height_max_cm;
  const heightRange =
    heightMin != null || heightMax != null
      ? `${heightMin != null ? Math.round(Number(heightMin)) : "?"}–${heightMax != null ? Math.round(Number(heightMax)) : "?"} см`
      : null;
  return (
    <div
      className={classNames("slot-stand-tile", { active: isActive })}
      onDragOver={(event) => {
        event.preventDefault();
      }}
      onDragEnter={(event) => {
        event.preventDefault();
        onHover?.(standId);
      }}
      onDragLeave={() => onHover?.(null)}
      onDrop={(event) => {
        event.preventDefault();
        onHover?.(null);
        const rawId = event.dataTransfer.getData("text/reservation-id");
        const parsed = Number(rawId);
        if (!Number.isNaN(parsed)) {
          onDrop(parsed, standId);
        }
      }}
    >
      <div className="slot-stand-header">
        <div className="slot-stand-title">{primaryLabel}</div>
        {secondaryLabel ? <div className="slot-stand-code subtle">{secondaryLabel}</div> : null}
      </div>
      {bikeLabel ? (
        <div className="slot-stand-bike">
          <span className="slot-stand-bike-title">{bikeLabel}</span>
          {heightRange ? <span className="slot-stand-bike-meta">Рост {heightRange}</span> : null}
        </div>
      ) : null}
      {reservation ? (
        <SeatCard reservation={reservation} label={primaryLabel} />
      ) : (
        <div className="slot-stand-empty">Перетащите клиента</div>
      )}
      {onAssignClick ? (
        <div className="slot-assign">
          <button type="button" className="btn primary" onClick={onAssignClick} disabled={isAssigning}>
            Добавить клиента
          </button>
          {showAssign ? (
            <div className="slot-assign-panel">
              <input
                type="search"
                placeholder="Поиск по имени…"
                value={searchTerm}
                onChange={(event) => onSearchChange?.(event.target.value)}
              />
              {searchTerm && (searchResults?.length ?? 0) === 0 ? (
                <div className="slot-stand-empty">Ничего не найдено</div>
              ) : null}
              <div className="slot-assign-results">
                {searchResults?.map((client) => {
                  const name =
                    client.full_name ||
                    [client.last_name, client.first_name].filter(Boolean).join(" ").trim() ||
                    `Клиент #${client.id}`;
                  const height = client.height ? `${client.height} см` : "";
                  return (
                    <button
                      key={client.id}
                      type="button"
                      className="slot-assign-option"
                      onClick={() => onSelectClient?.(client)}
                      disabled={isAssigning}
                    >
                      <span className="slot-assign-name">{name}</span>
                      {height ? <span className="slot-assign-meta">{height}</span> : null}
                    </button>
                  );
                })}
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

export default function SlotSeatingPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { session } = useAppContext();
  const slotId = id ? Number(id) : NaN;
  const [activeStandId, setActiveStandId] = useState<number | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [assignStandId, setAssignStandId] = useState<number | null>(null);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [isCopyOpen, setIsCopyOpen] = useState<boolean>(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState<boolean>(false);
  const [selectedTargets, setSelectedTargets] = useState<number[]>([]);
  const [copyMessage, setCopyMessage] = useState<string | null>(null);
  const [weekdayFilters, setWeekdayFilters] = useState<number[]>([]);
  const [timeFilterStart, setTimeFilterStart] = useState<string>("");
  const [timeFilterEnd, setTimeFilterEnd] = useState<string>("");
  const [slotEdit, setSlotEdit] = useState<{ label: string; sessionKind: string; instructorId: string }>({
    label: "",
    sessionKind: "self_service",
    instructorId: ""
  });

  const slotQuery = useQuery<ScheduleSlotDetailResponse>({
    queryKey: ["schedule-slot", slotId],
    enabled: Number.isFinite(slotId),
    queryFn: () => apiFetch<ScheduleSlotDetailResponse>(`/api/schedule/slots/${slotId}`)
  });

  const clientSearchQuery = useQuery<ClientListResponse>({
    queryKey: ["clients-search", searchTerm],
    enabled: assignStandId !== null && searchTerm.trim().length >= 2,
    queryFn: () =>
      apiFetch(`/api/clients?search=${encodeURIComponent(searchTerm.trim())}&page=1&sort=last_name&direction=asc`),
    staleTime: 30_000
  });

  const copyTargetsQuery = useQuery<SlotCopyTargetsResponse>({
    queryKey: ["schedule-slot-copy-targets", slotId],
    enabled: isCopyOpen && Number.isFinite(slotId),
    queryFn: () => apiFetch<SlotCopyTargetsResponse>(`/api/schedule/slots/${slotId}/copy-targets`),
    staleTime: 60_000
  });

  const slot = slotQuery.data?.slot;
  const isAdmin = session.isAdmin;

  useEffect(() => {
    if (!slot) return;
    setSlotEdit({
      label: slot.label ?? "",
      sessionKind: slot.session_kind,
      instructorId: slot.instructorId ? String(slot.instructorId) : ""
    });
  }, [slot?.id, slot?.label, slot?.session_kind, slot?.instructorId]);

  const slotUpdateMutation = useMutation<ScheduleSlotDetailResponse, ApiError, Partial<ScheduleSlot>>({
    mutationFn: (payload) =>
      apiFetch<ScheduleSlotDetailResponse>(`/api/schedule/slots/${slotId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: (data) => {
      queryClient.setQueryData<ScheduleSlotDetailResponse>(["schedule-slot", slotId], (prev) => {
        const mergedStands = data.stands ?? prev?.stands ?? [];
        return { ...data, stands: mergedStands };
      });
      setSlotEdit({
        label: data.slot.label ?? "",
        sessionKind: data.slot.session_kind,
        instructorId: data.slot.instructorId ? String(data.slot.instructorId) : ""
      });
      setErrorMessage(null);
    },
    onError: (error) => {
      setErrorMessage(error?.message ?? "Не удалось обновить слот");
    }
  });

  function applySlotUpdate(data: MoveResponse | { reservation: ScheduleReservation; slot?: ScheduleSlot }) {
    if (!data || !slotId) return;
    const weekId = (data as MoveResponse).slot?.week_id ?? slot?.week_id;
    queryClient.invalidateQueries({ queryKey: ["schedule-slot", slotId] });
    if (weekId) {
      queryClient.invalidateQueries({ queryKey: ["schedule-week", weekId] });
      queryClient.invalidateQueries({ queryKey: ["schedule-week"], exact: false });
      queryClient.invalidateQueries({ queryKey: ["schedule-weeks"] });
    }
    setErrorMessage(null);
    queryClient.setQueryData<ScheduleSlotDetailResponse>(["schedule-slot", slotId], (prev) => {
      if (!prev) return prev;
      if ((data as MoveResponse).slot) {
        return { ...prev, slot: (data as MoveResponse).slot! };
      }
      const updated = data.reservation;
      return {
        ...prev,
        slot: {
          ...prev.slot,
          reservations: prev.slot.reservations.map((item) => (item.id === updated.id ? updated : item))
        }
      };
    });
    if (weekId && (data as MoveResponse).slot) {
      queryClient.setQueryData<ScheduleWeekDetailResponse | undefined>(["schedule-week", weekId], (prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          slots: prev.slots.map((slotEntry: ScheduleSlot) =>
            slotEntry.id === (data as MoveResponse).slot!.id ? (data as MoveResponse).slot! : slotEntry
          )
        };
      });
    }
  }

  const moveMutation = useMutation<MoveResponse, unknown, MovePayload>({
    mutationFn: ({ reservationId, standId, swapReservationId }) =>
      apiFetch<MoveResponse>(`/api/schedule/reservations/${reservationId}`, {
        method: "PATCH",
        body: JSON.stringify({ standId, swapReservationId })
      }),
    onSuccess: (data) => {
      applySlotUpdate(data);
    },
    onError: (error) => {
      console.error("SlotSeatingPage: failed to move reservation", error);
      const apiError = error as ApiError;
      setErrorMessage(apiError?.message ?? "Не удалось изменить рассадку. Попробуйте ещё раз.");
    }
  });

  const assignMutation = useMutation<
    MoveResponse,
    unknown,
    { standId: number; clientId: number; reservationId: number }
  >({
    mutationFn: ({ reservationId, clientId }) =>
      apiFetch<MoveResponse>(`/api/schedule/reservations/${reservationId}`, {
        method: "PATCH",
        body: JSON.stringify({ clientId, status: "booked" })
      }),
    onSuccess: (data) => {
      applySlotUpdate(data);
      setAssignStandId(null);
      setSearchTerm("");
    },
    onError: (error) => {
      console.error("SlotSeatingPage: failed to assign client", error);
      const apiError = error as ApiError;
      setErrorMessage(apiError?.message ?? "Не удалось добавить клиента. Попробуйте ещё раз.");
    }
  });

  const copyMutation = useMutation<SlotCopyResponse, ApiError, { targetSlotIds: number[] }>({
    mutationFn: ({ targetSlotIds }) =>
      apiFetch<SlotCopyResponse>(`/api/schedule/slots/${slotId}/copy`, {
        method: "POST",
        body: JSON.stringify({ targetSlotIds })
      }),
    onSuccess: (data) => {
      data.updated_slots?.forEach((updatedSlot) => {
        if (updatedSlot.id === slotId) {
          queryClient.setQueryData<ScheduleSlotDetailResponse>(["schedule-slot", slotId], (prev) =>
            prev ? { ...prev, slot: updatedSlot } : prev
          );
        } else {
          queryClient.invalidateQueries({ queryKey: ["schedule-slot", updatedSlot.id] });
        }
        if (updatedSlot.week_id) {
          queryClient.invalidateQueries({ queryKey: ["schedule-week", updatedSlot.week_id] });
        }
      });
      queryClient.invalidateQueries({ queryKey: ["schedule-week"], exact: false });
      queryClient.invalidateQueries({ queryKey: ["schedule-weeks"] });
      setSelectedTargets([]);
      setIsCopyOpen(false);
      const updatedCount = data.results?.length ?? 0;
      setCopyMessage(updatedCount ? `Рассадка скопирована в ${updatedCount} слот(ов).` : "Рассадка скопирована.");
      setErrorMessage(null);
    },
    onError: (error) => {
      console.error("SlotSeatingPage: failed to copy seating", error);
      const apiError = error as ApiError;
      setCopyMessage(null);
      setErrorMessage(apiError?.message ?? "Не удалось скопировать рассадку. Попробуйте ещё раз.");
    }
  });

  const stands = useMemo<ScheduleStandSummary[]>(() => {
    const items = slotQuery.data?.stands ?? [];
    return items.slice().sort((a, b) => {
      const numA = Number(a.code);
      const numB = Number(b.code);
      const aVal = Number.isFinite(numA) ? numA : a.id;
      const bVal = Number.isFinite(numB) ? numB : b.id;
      return aVal - bVal;
    });
  }, [slotQuery.data?.stands]);
  const reservations = slot?.reservations ?? [];

  const standAssignments = useMemo(() => {
    const map = new Map<number, ScheduleReservation>();
    reservations.forEach((res) => {
      if (res.stand_id != null) {
        map.set(res.stand_id, res);
      }
    });
    return map;
  }, [reservations]);

  const unassigned = reservations.filter((res) => res.stand_id == null);

  const copyTargets = copyTargetsQuery.data?.items ?? [];

  function weekdayOf(dateStr: string) {
    const parsed = new Date(dateStr);
    return Number.isNaN(parsed.getTime()) ? null : parsed.getDay(); // 0=Sunday
  }

  function timeToMinutes(value: string) {
    if (!value) return null;
    const [hh, mm] = value.split(":");
    const h = Number(hh);
    const m = Number(mm);
    if (Number.isNaN(h) || Number.isNaN(m)) return null;
    return h * 60 + m;
  }

  const filteredCopyTargets = copyTargets.filter((target) => {
    const weekday = weekdayOf(target.slot_date);
    if (weekdayFilters.length > 0 && weekday !== null && !weekdayFilters.includes(weekday)) {
      return false;
    }

    const startMinutes = timeToMinutes(target.start_time);
    const endMinutes = timeToMinutes(target.end_time);
    const filterStart = timeToMinutes(timeFilterStart);
    const filterEnd = timeToMinutes(timeFilterEnd);

    if (filterStart !== null && startMinutes !== null && startMinutes < filterStart) {
      return false;
    }
    if (filterEnd !== null && endMinutes !== null && endMinutes > filterEnd) {
      return false;
    }
    return true;
  });

  function toggleTargetSelection(targetId: number) {
    setSelectedTargets((prev) => (prev.includes(targetId) ? prev.filter((id) => id !== targetId) : [...prev, targetId]));
  }

  function handleCopySubmit() {
    if (selectedTargets.length === 0 || copyMutation.isPending) return;
    copyMutation.mutate({ targetSlotIds: selectedTargets });
  }

  function toggleCopyPanel() {
    setIsCopyOpen((prev) => {
      const next = !prev;
      if (!next) {
        setSelectedTargets([]);
      }
      return next;
    });
  }

  function toggleWeekdayFilter(day: number) {
    setWeekdayFilters((prev) => {
      if (prev.includes(day)) {
        return prev.filter((item) => item !== day);
      }
      return [...prev, day].sort();
    });
  }

  function findReservationForStand(standId: number): ScheduleReservation | undefined {
    const direct = reservations.find((res) => res.stand_id === standId);
    if (direct) return direct;
    const available = reservations.find((res) => res.stand_id == null && res.status === "available");
    if (available) return available;
    return reservations.find((res) => res.stand_id == null);
  }

  function handleDrop(reservationId: number, targetStandId: number | null) {
    if (!session.isAdmin) {
      window.alert("Недостаточно прав для изменения рассадки.");
      return;
    }
    if (!slot) return;
    const moving = reservations.find((item) => item.id === reservationId);
    if (!moving) return;
    if ((moving.stand_id ?? null) === targetStandId) return;
    const targetReservation = targetStandId != null ? standAssignments.get(targetStandId) : undefined;
    moveMutation.mutate({
      reservationId,
      standId: targetStandId,
      swapReservationId: targetReservation?.id
    });
  }

  if (!Number.isFinite(slotId)) {
    return (
      <Panel title="Расстановка" subtitle="Неверный идентификатор слота">
        <div className="schedule-empty">Проверьте ссылку и попробуйте снова.</div>
      </Panel>
    );
  }

  if (slotQuery.isLoading) {
    return (
      <Panel title="Расстановка" subtitle="Загружаем данные слота">
        <div className="schedule-empty">Загружаем…</div>
      </Panel>
    );
  }

  if (slotQuery.isError || !slot) {
    return (
      <Panel title="Расстановка" subtitle="Ошибка">
        <div className="schedule-error">Не удалось загрузить слот.</div>
      </Panel>
    );
  }

  const weekLabel = slotQuery.data?.week?.week_start_date;
  const dateObj = new Date(slot.slot_date);
  const weekdayLabel = Number.isNaN(dateObj.getTime())
    ? ""
    : ["Вс", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"][dateObj.getDay()] ?? "";
  const slotLabel = `${slot.slot_date} (${weekdayLabel}) · ${slot.start_time}-${slot.end_time}`;
  const instructorName = slot.instructorName ?? (slot.instructorId != null ? `#${slot.instructorId}` : null);

  function handleSlotMetaSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isAdmin) return;
    const payload: Record<string, unknown> = {
      label: slotEdit.label.trim(),
      sessionKind: slotEdit.sessionKind,
      instructorId:
        slotEdit.sessionKind === "instructor" && slotEdit.instructorId ? Number(slotEdit.instructorId) : null
    };
    slotUpdateMutation.mutate(payload);
  }

  return (
    <Panel title="Рассадка слота" subtitle="Перетащите карточки клиентов между станками">
      <div className="slot-seating-header">
        <div className="slot-seating-meta">
          <div className="slot-seating-title">{slotLabel}</div>
          <div className="slot-seating-subtitle">Неделя: {weekLabel || "—"}</div>
          {!isAdmin ? (
            <div className="slot-seating-subtitle">
              {slot.label ? `Метка: ${slot.label}` : ""}
              {instructorName ? ` · Инструктор: ${instructorName}` : ""}
              {slot.session_kind ? ` · Режим: ${slot.session_kind}` : ""}
            </div>
          ) : null}
        </div>
      </div>

      {session.isAdmin ? (
        <div className="slot-meta-section">
          <button type="button" className="btn primary" onClick={() => setIsSettingsOpen((prev) => !prev)}>
            {isSettingsOpen ? "Скрыть настройки" : "Настройки слота"}
          </button>
          {isSettingsOpen ? (
            <div className="slot-meta-panel">
              <form className="slot-meta-form" onSubmit={handleSlotMetaSubmit}>
                <label>
                  Метка слота
                  <input
                    type="text"
                    value={slotEdit.label}
                    onChange={(event) => setSlotEdit((prev) => ({ ...prev, label: event.target.value }))}
                    placeholder="Например, Инструктор Петр"
                  />
                </label>
                <label>
                  Режим
                  <select
                    value={slotEdit.sessionKind}
                    onChange={(event) => setSlotEdit((prev) => ({ ...prev, sessionKind: event.target.value }))}
                  >
                    <option value="self_service">Самокрутка</option>
                    <option value="instructor">С инструктором</option>
                    <option value="race">Гонка</option>
                  </select>
                </label>
                <label>
                  Инструктор
                  <select
                    value={slotEdit.instructorId}
                    onChange={(event) => setSlotEdit((prev) => ({ ...prev, instructorId: event.target.value }))}
                    disabled={slotEdit.sessionKind !== "instructor"}
                  >
                    <option value="">— Без инструктора —</option>
                    {slotQuery.data?.instructors.map((inst) => (
                      <option key={inst.id} value={inst.id}>
                        {inst.full_name}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="slot-meta-actions">
                  <button type="submit" className="btn primary" disabled={slotUpdateMutation.isPending}>
                    Сохранить
                  </button>
                </div>
              </form>
            </div>
          ) : null}
        </div>
      ) : null}

      {session.isAdmin ? (
        <div className="slot-copy-section">
          <button type="button" className="btn" onClick={toggleCopyPanel} disabled={copyMutation.isPending}>
            {isCopyOpen ? "Скрыть копирование" : "Копировать слот"}
          </button>
          {isCopyOpen ? (
            <div className="slot-copy-panel">
              <div className="slot-copy-filters">
                <div className="slot-copy-weekdays">
                  <div className="slot-copy-weekdays-title">Дни недели</div>
                  {[
                    { value: 1, label: "Пн" },
                    { value: 2, label: "Вт" },
                    { value: 3, label: "Ср" },
                    { value: 4, label: "Чт" },
                    { value: 5, label: "Пт" },
                    { value: 6, label: "Сб" },
                    { value: 0, label: "Вс" },
                  ].map((item) => (
                    <label key={item.value} className="slot-copy-weekday">
                      <input
                        type="checkbox"
                        checked={weekdayFilters.includes(item.value)}
                        onChange={() => toggleWeekdayFilter(item.value)}
                      />
                      <span>{item.label}</span>
                    </label>
                  ))}
                  <button type="button" className="btn ghost" onClick={() => setWeekdayFilters([])} disabled={weekdayFilters.length === 0}>
                    Сбросить дни
                  </button>
                </div>
                <label>
                  Время c
                  <input
                    type="time"
                    value={timeFilterStart}
                    onChange={(event) => setTimeFilterStart(event.target.value)}
                  />
                </label>
                <label>
                  до
                  <input
                    type="time"
                    value={timeFilterEnd}
                    onChange={(event) => setTimeFilterEnd(event.target.value)}
                  />
                </label>
              </div>
              {copyTargetsQuery.isLoading ? <div className="slot-stand-empty">Загружаем слоты…</div> : null}
              {copyTargetsQuery.isError ? <div className="schedule-error">Не удалось загрузить список слотов.</div> : null}
              {!copyTargetsQuery.isLoading && !copyTargetsQuery.isError && filteredCopyTargets.length === 0 ? (
                <div className="slot-stand-empty">Нет подходящих будущих слотов.</div>
              ) : null}
              {filteredCopyTargets.length > 0 ? (
                <div className="slot-copy-list">
                  {filteredCopyTargets.map((target) => {
                    const selected = selectedTargets.includes(target.id);
                    const subtitleParts = [target.label, target.instructorName].filter(Boolean);
                    const weekday = weekdayOf(target.slot_date);
                    const weekdayLabel =
                      weekday === null
                        ? ""
                        : ["Вс", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"][weekday] || "";
                    return (
                      <label key={target.id} className="slot-copy-item">
                        <input
                          type="checkbox"
                          checked={selected}
                          onChange={() => toggleTargetSelection(target.id)}
                        />
                        <div className="slot-copy-item-body">
                          <div className="slot-copy-item-title">
                            {target.slot_date} · {target.start_time}-{target.end_time} {weekdayLabel ? `(${weekdayLabel})` : ""}
                          </div>
                          <div className="slot-copy-item-meta">
                            Неделя {target.week_start_date}
                            {subtitleParts.length > 0 ? ` · ${subtitleParts.join(" · ")}` : ""}
                          </div>
                        </div>
                      </label>
                    );
                  })}
                </div>
              ) : null}
              <div className="slot-copy-actions">
                <button
                  type="button"
                  className="btn primary"
                  onClick={handleCopySubmit}
                  disabled={copyMutation.isPending || selectedTargets.length === 0}
                >
                  {copyMutation.isPending ? "Копируем…" : "Скопировать слот"}
                </button>
                <button
                  type="button"
                  className="btn ghost"
                  onClick={() => setSelectedTargets([])}
                  disabled={selectedTargets.length === 0 || copyMutation.isPending}
                >
                  Очистить выбор
                </button>
              </div>
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="slot-seating-grid">
        <div className="slot-stands-grid">
          {stands.map((stand) => (
            <StandTile
              key={stand.id}
              stand={stand}
              reservation={standAssignments.get(stand.id) ?? null}
              onDrop={(reservationId) => handleDrop(reservationId, stand.id)}
              isActive={activeStandId === stand.id}
              onHover={setActiveStandId}
              onAssignClick={
                (standAssignments.get(stand.id)?.status ?? "") !== "booked"
                  ? () => setAssignStandId(stand.id)
                  : undefined
              }
              showAssign={assignStandId === stand.id}
              searchTerm={assignStandId === stand.id ? searchTerm : ""}
              onSearchChange={setSearchTerm}
              searchResults={assignStandId === stand.id ? clientSearchQuery.data?.items ?? [] : []}
              onSelectClient={(client) => {
                const targetReservation = findReservationForStand(stand.id);
                if (!targetReservation) {
                  setErrorMessage("Не найден слот для станка, попробуйте обновить страницу.");
                  return;
                }
                assignMutation.mutate({
                  standId: stand.id,
                  clientId: client.id,
                  reservationId: targetReservation.id
                });
              }}
              isAssigning={assignMutation.isPending}
            />
          ))}
        </div>
        <div
          className={classNames("slot-stand-tile", "slot-unassigned")}
          onDragOver={(event) => {
            event.preventDefault();
            setActiveStandId(null);
          }}
          onDrop={(event) => {
            event.preventDefault();
            const rawId = event.dataTransfer.getData("text/reservation-id");
            const parsed = Number(rawId);
            if (!Number.isNaN(parsed)) {
              handleDrop(parsed, null);
            }
          }}
        >
          <div className="slot-stand-header">
            <div className="slot-stand-title">Без станка</div>
            <div className="slot-stand-code">Очередь / ожидание</div>
          </div>
          {unassigned.length === 0 ? (
            <div className="slot-stand-empty">Перетащите сюда, чтобы убрать привязку</div>
          ) : (
            <div className="slot-unassigned-list">
              {unassigned.map((reservation) => (
                <SeatCard
                  key={reservation.id}
                  reservation={reservation}
                  label={reservation.client_name ?? "—"}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {moveMutation.isPending ? <div className="slot-seating-hint">Сохраняем изменения…</div> : null}
      {copyMessage ? <div className="schedule-success">{copyMessage}</div> : null}
      {errorMessage ? <div className="schedule-error">{errorMessage}</div> : null}
      <div className="slot-seating-hint">Поддерживается swap: перетащите на занятый станок для обмена.</div>
      {!session.isAdmin ? <div className="schedule-error">Требуются права администратора.</div> : null}
    </Panel>
  );
}
