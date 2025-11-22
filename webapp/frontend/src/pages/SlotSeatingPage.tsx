import classNames from "classnames";
import { useMemo, useState } from "react";
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
  ScheduleWeekDetailResponse
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
  const secondary = reservation.client_name && reservation.client_name.trim() !== label ? label : "";

  return (
    <div
      className={classNames("slot-seat-card", reservation.status)}
      draggable
      onDragStart={(event) => {
        event.dataTransfer.setData("text/reservation-id", String(reservation.id));
      }}
    >
      <div className="slot-seat-name">{reservation.client_name || "—"}</div>
      {secondary ? <div className="slot-seat-meta">{secondary}</div> : null}
      <div className="slot-seat-status">{statusLabel}</div>
    </div>
  );
}

function StandTile({
  stand,
  reservation,
  onDrop,
  isActive,
  onHover
}: {
  stand: ScheduleStandSummary | null;
  reservation: ScheduleReservation | null;
  onDrop: (reservationId: number, targetStandId: number | null) => void;
  isActive: boolean;
  onHover?: (standId: number | null) => void;
}) {
  const title = stand ? stand.display_name || stand.code || stand.title || `Станок ${stand.id}` : "Без станка";
  const standId = stand?.id ?? null;
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
        <div className="slot-stand-title">{title}</div>
        {stand?.code ? <div className="slot-stand-code">{stand.code}</div> : null}
      </div>
      {reservation ? (
        <SeatCard reservation={reservation} label={title} />
      ) : (
        <div className="slot-stand-empty">Перетащите клиента</div>
      )}
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

  const slotQuery = useQuery<ScheduleSlotDetailResponse>({
    queryKey: ["schedule-slot", slotId],
    enabled: Number.isFinite(slotId),
    queryFn: () => apiFetch<ScheduleSlotDetailResponse>(`/api/schedule/slots/${slotId}`)
  });

  const moveMutation = useMutation<MoveResponse, unknown, MovePayload>({
    mutationFn: ({ reservationId, standId, swapReservationId }) =>
      apiFetch<MoveResponse>(`/api/schedule/reservations/${reservationId}`, {
        method: "PATCH",
        body: JSON.stringify({ standId, swapReservationId })
      }),
    onSuccess: (data) => {
      if (!data || !slotId) return;
      const weekId = data.slot?.week_id ?? slot?.week_id;
      queryClient.invalidateQueries({ queryKey: ["schedule-slot", slotId] });
      if (weekId) {
        queryClient.invalidateQueries({ queryKey: ["schedule-week", weekId] });
        queryClient.invalidateQueries({ queryKey: ["schedule-week"], exact: false });
        queryClient.invalidateQueries({ queryKey: ["schedule-weeks"] });
      }
      setErrorMessage(null);
      queryClient.setQueryData<ScheduleSlotDetailResponse>(["schedule-slot", slotId], (prev) => {
        if (!prev) return prev;
        if (data.slot) {
          return { ...prev, slot: data.slot };
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
      if (weekId && data.slot) {
        queryClient.setQueryData<ScheduleWeekDetailResponse | undefined>(["schedule-week", weekId], (prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            slots: prev.slots.map((slotEntry: ScheduleSlot) =>
              slotEntry.id === data.slot!.id ? data.slot! : slotEntry
            )
          };
        });
      }
    },
    onError: (error) => {
      console.error("SlotSeatingPage: failed to move reservation", error);
      const apiError = error as ApiError;
      setErrorMessage(apiError?.message ?? "Не удалось изменить рассадку. Попробуйте ещё раз.");
    }
  });

  const slot = slotQuery.data?.slot;
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
  const slotLabel = `${slot.slot_date} · ${slot.start_time}-${slot.end_time}`;
  const instructorName = slot.instructorName ?? (slot.instructorId != null ? `#${slot.instructorId}` : null);

  return (
    <Panel title="Рассадка слота" subtitle="Перетащите карточки клиентов между станками">
      <div className="slot-seating-header">
        <div className="slot-seating-meta">
          <div className="slot-seating-title">{slotLabel}</div>
          <div className="slot-seating-subtitle">
            Неделя: {weekLabel || "—"}
            {slot.label ? ` · ${slot.label}` : ""}
            {instructorName ? ` · Инструктор: ${instructorName}` : ""}
          </div>
        </div>
        <div className="slot-seating-actions">
          <button type="button" className="btn ghost" onClick={() => navigate(-1)}>
            Назад
          </button>
          <Link to="/schedule/manage" className="btn ghost">
            К расписанию
          </Link>
        </div>
      </div>

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
      {errorMessage ? <div className="schedule-error">{errorMessage}</div> : null}
      <div className="slot-seating-hint">Поддерживается swap: перетащите на занятый станок для обмена.</div>
      {!session.isAdmin ? <div className="schedule-error">Требуются права администратора.</div> : null}
    </Panel>
  );
}
