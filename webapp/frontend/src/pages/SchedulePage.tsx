import classNames from "classnames";
import { FormEvent, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type {
  ScheduleReservation,
  ScheduleSlot,
  ScheduleStandSummary,
  ScheduleWeekDetailResponse,
  ScheduleWeekListResponse,
  ScheduleWeekRow,
  InstructorRow
} from "../lib/types";
import { useAppContext } from "../lib/AppContext";

import "../styles/schedule.css";

const SESSION_KIND_OPTIONS = [
  { value: "self_service", label: "Самокрутка" },
  { value: "instructor", label: "С инструктором" }
];

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

function summarizeReservations(reservations: ScheduleReservation[]): { occupied: number; free: number } {
  const occupied = reservations.filter((reservation) => reservation.status !== "available").length;
  return {
    occupied,
    free: Math.max(reservations.length - occupied, 0)
  };
}

type CreateWeekPayload = {
  weekStartDate: string;
  copyFromWeekId?: number;
  replace?: boolean;
};

type CopyWeekPayload = {
  weekId: number;
  sourceWeekId: number;
  replace: boolean;
};

type SlotPayload = Record<string, unknown>;

type ReservationPayload = Record<string, unknown>;

type CreateWeekResponse = {
  week: ScheduleWeekRow;
  copied: { slots: number; placeholders: number };
  defaults?: number;
  slots: ScheduleSlot[];
  stands: ScheduleStandSummary[];
  instructors: InstructorRow[];
};

type CopyWeekResponse = {
  copied: { slots: number; placeholders: number };
  slots: ScheduleSlot[];
  stands: ScheduleStandSummary[];
  instructors: InstructorRow[];
};

type SlotsEnvelope = {
  slots: ScheduleSlot[];
  instructors?: InstructorRow[];
};

type SlotEnvelope = {
  slot: ScheduleSlot;
  slots: ScheduleSlot[];
  instructors?: InstructorRow[];
};

type SyncWeekResponse = {
  inserted: number;
  slots: ScheduleSlot[];
  instructors: InstructorRow[];
};

type FillTemplateParams = {
  weekId: number;
  force: boolean;
};

type FillTemplateResponse = {
  created: number;
  slots: ScheduleSlot[];
  instructors: InstructorRow[];
};

function formatDateLabel(value: string): string {
  const date = new Date(`${value}T00:00:00`);
  return date.toLocaleDateString("ru-RU", {
    weekday: "short",
    day: "2-digit",
    month: "short"
  });
}

function formatWeekLabel(week: ScheduleWeekRow): string {
  const date = new Date(`${week.week_start_date}T00:00:00`);
  const base = date.toLocaleDateString("ru-RU", {
    day: "2-digit",
    month: "short"
  });
  if (week.title) {
    return `${base} · ${week.title}`;
  }
  return base;
}

function buildStandLookup(stands: ScheduleStandSummary[]): Map<number, string> {
  const map = new Map<number, string>();
  stands.forEach((stand) => {
    const label = stand.display_name || stand.code || `Станок ${stand.id}`;
    map.set(stand.id, label);
  });
  return map;
}

export default function SchedulePage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();
  const isAdmin = session.isAdmin;

  const [selectedWeekId, setSelectedWeekId] = useState<number | null>(null);
  const [newWeekDate, setNewWeekDate] = useState<string>("");
  const [newWeekCopySource, setNewWeekCopySource] = useState<number | "">("");
  const [newWeekReplace, setNewWeekReplace] = useState<boolean>(false);

  const [copySourceWeekId, setCopySourceWeekId] = useState<number | "">("");
  const [copyReplace, setCopyReplace] = useState<boolean>(true);

  const [newSlotDate, setNewSlotDate] = useState<string>("");
  const [newSlotStart, setNewSlotStart] = useState<string>("");
  const [newSlotEnd, setNewSlotEnd] = useState<string>("");
  const [newSlotLabel, setNewSlotLabel] = useState<string>("");
  const [newSlotSessionKind, setNewSlotSessionKind] = useState<string>("self_service");
  const [newSlotInstructorId, setNewSlotInstructorId] = useState<string>("");

  const [collapsedSlots, setCollapsedSlots] = useState<Record<number, boolean>>({});
  const weeksQuery = useQuery<ScheduleWeekListResponse>({
    queryKey: ["schedule-weeks"],
    queryFn: () => apiFetch<ScheduleWeekListResponse>("/api/schedule/weeks?page=1&page_size=50")
  });

  useEffect(() => {
    if (!weeksQuery.data) return;
    if (selectedWeekId === null && weeksQuery.data.items.length > 0) {
      setSelectedWeekId(weeksQuery.data.items[0].id);
    }
  }, [weeksQuery.data, selectedWeekId]);

  const weekDetailQuery = useQuery<ScheduleWeekDetailResponse>({
    queryKey: ["schedule-week", selectedWeekId],
    queryFn: () => apiFetch<ScheduleWeekDetailResponse>(`/api/schedule/weeks/${selectedWeekId}`),
    enabled: selectedWeekId !== null
  });

  const currentWeek = weekDetailQuery.data?.week;
  const slots = weekDetailQuery.data?.slots ?? [];
  const stands = weekDetailQuery.data?.stands ?? [];
  const instructors = weekDetailQuery.data?.instructors ?? [];

  useEffect(() => {
    if (currentWeek) {
      setNewSlotDate(currentWeek.week_start_date);
    }
  }, [currentWeek?.id]);

  const standLookup = useMemo(() => buildStandLookup(stands), [stands]);

  const [collapsedDays, setCollapsedDays] = useState<Record<string, boolean>>({});

  useEffect(() => {
    if (newSlotSessionKind !== "instructor") {
      setNewSlotInstructorId("");
      return;
    }
    if (instructors.length === 0) {
      setNewSlotInstructorId("");
      return;
    }
    if (newSlotInstructorId && !instructors.some((item) => String(item.id) === newSlotInstructorId)) {
      setNewSlotInstructorId("");
      return;
    }
    if (!newSlotInstructorId) {
      setNewSlotInstructorId(String(instructors[0].id));
    }
  }, [newSlotSessionKind, instructors, newSlotInstructorId]);

  const slotsGrouped = useMemo(() => {
    const groups = new Map<string, ScheduleSlot[]>();
    slots.forEach((slot) => {
      const bucket = groups.get(slot.slot_date);
      if (bucket) {
        bucket.push(slot);
      } else {
        groups.set(slot.slot_date, [slot]);
      }
    });
    return Array.from(groups.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([slotDate, list]) => ({
        slotDate,
        slots: list.slice().sort((a, b) => a.start_time.localeCompare(b.start_time))
      }));
  }, [slots]);

  useEffect(() => {
    setCollapsedDays((prev) => {
      const next: Record<string, boolean> = {};
      let changed = false;
      slotsGrouped.forEach((group) => {
        const current = prev[group.slotDate];
        if (current === undefined) {
          changed = true;
          next[group.slotDate] = true;
        } else {
          next[group.slotDate] = current;
        }
      });
      if (Object.keys(prev).length !== Object.keys(next).length) {
        changed = true;
      }
      if (!changed) {
        return prev;
      }
      return next;
    });
  }, [slotsGrouped]);

  useEffect(() => {
    setCollapsedSlots((prev) => {
      const next: Record<number, boolean> = {};
      let changed = false;
      slots.forEach((slot) => {
        const existing = prev[slot.id];
        if (existing === undefined) {
          next[slot.id] = true;
          changed = true;
        } else {
          next[slot.id] = existing;
        }
      });
      if (Object.keys(prev).length !== Object.keys(next).length) {
        changed = true;
      }
      if (!changed) {
        return prev;
      }
      return next;
    });
  }, [slots]);

  function handleToggleDay(dateValue: string) {
    setCollapsedDays((prev) => ({
      ...prev,
      [dateValue]: !prev[dateValue]
    }));
  }

  function handleToggleSlot(slotId: number) {
    setCollapsedSlots((prev) => ({
      ...prev,
      [slotId]: !prev[slotId]
    }));
  }

  const createWeekMutation = useMutation<CreateWeekResponse, unknown, CreateWeekPayload>({
    mutationFn: (payload: CreateWeekPayload) =>
      apiFetch<CreateWeekResponse>("/api/schedule/weeks", {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ["schedule-weeks"] });
      if (data?.week?.id) {
        queryClient.invalidateQueries({ queryKey: ["schedule-week", data.week.id] });
        queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", data.week.id], {
          week: data.week,
          slots: data.slots ?? [],
          stands: data.stands ?? [],
          instructors: data.instructors ?? []
        });
        setSelectedWeekId(data.week.id);
      }
    },
    onError: (error) => {
      console.error("SchedulePage: failed to create week", error);
    }
  });

  const copyWeekMutation = useMutation<CopyWeekResponse, unknown, CopyWeekPayload>({
    mutationFn: (payload: CopyWeekPayload) =>
      apiFetch<CopyWeekResponse>(`/api/schedule/weeks/${payload.weekId}/copy`, {
        method: "POST",
        body: JSON.stringify({
          sourceWeekId: payload.sourceWeekId,
          replace: payload.replace
        })
      }),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["schedule-week", variables.weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", variables.weekId], (prev) => {
        if (!prev) {
          if (!currentWeek) {
            return prev;
          }
          return {
            week: currentWeek,
            slots: data.slots ?? [],
            stands: data.stands ?? [],
            instructors: data.instructors ?? []
          };
        }
        return {
          ...prev,
          slots: data.slots ?? prev.slots,
          stands: data.stands ?? prev.stands,
          instructors: data.instructors ?? prev.instructors ?? []
        };
      });
    },
    onError: (error) => {
      console.error("SchedulePage: failed to copy week", error);
    }
  });

  const deleteWeekMutation = useMutation({
    mutationFn: (weekId: number) =>
      apiFetch(`/api/schedule/weeks/${weekId}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedule-weeks"] });
      setSelectedWeekId(null);
      setCopySourceWeekId("");
    },
    onError: (error) => {
      console.error("SchedulePage: failed to delete week", error);
    }
  });

  const syncWeekMutation = useMutation<SyncWeekResponse, unknown, number>({
    mutationFn: (weekId: number) =>
      apiFetch<SyncWeekResponse>(`/api/schedule/weeks/${weekId}/sync`, {
        method: "POST"
      }),
    onSuccess: (data, weekId) => {
      queryClient.invalidateQueries({ queryKey: ["schedule-week", weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", weekId], (prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          slots: data.slots ?? [],
          instructors: data.instructors ?? prev?.instructors ?? []
        };
      });
    },
    onError: (error) => {
      console.error("SchedulePage: failed to sync week capacity", error);
    }
  });

  const fillTemplateMutation = useMutation<FillTemplateResponse, unknown, FillTemplateParams>({
    mutationFn: ({ weekId, force }) =>
      apiFetch<FillTemplateResponse>(`/api/schedule/weeks/${weekId}/fill-template?force=${force ? "true" : "false"}`, {
        method: "POST"
      }),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["schedule-week", variables.weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", variables.weekId], (prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          slots: data.slots ?? [],
          instructors: data.instructors ?? prev?.instructors ?? []
        };
      });
    },
    onError: (error) => {
      console.error("SchedulePage: failed to fill template", error);
    }
  });

  const createSlotMutation = useMutation<SlotEnvelope, unknown, { weekId: number; payload: SlotPayload }>({
    mutationFn: ({ weekId, payload }) =>
      apiFetch<SlotEnvelope>(`/api/schedule/slots`, {
        method: "POST",
        body: JSON.stringify({ weekId, ...payload })
      }),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["schedule-week", variables.weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", variables.weekId], (prev) => {
        if (!prev) {
          return prev;
        }
        return {
          ...prev,
          slots: data.slots ?? prev.slots,
          instructors: data.instructors ?? prev.instructors ?? []
        };
      });
      setNewSlotStart("");
      setNewSlotEnd("");
      setNewSlotLabel("");
      setNewSlotInstructorId("");
    },
    onError: (error) => {
      console.error("SchedulePage: failed to create slot", error);
    }
  });

  const updateSlotMutation = useMutation<SlotEnvelope, unknown, { slotId: number; weekId: number; payload: SlotPayload }>({
    mutationFn: ({ slotId, payload }) =>
      apiFetch<SlotEnvelope>(`/api/schedule/slots/${slotId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["schedule-week", variables.weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", variables.weekId], (prev) => {
        if (!prev) {
          return prev;
        }
        return {
          ...prev,
          slots: data.slots ?? prev.slots,
          instructors: data.instructors ?? prev.instructors ?? []
        };
      });
    },
    onError: (error) => {
      console.error("SchedulePage: failed to update slot", error);
    }
  });

  const deleteSlotMutation = useMutation<SlotsEnvelope, unknown, { slotId: number; weekId: number }>({
    mutationFn: ({ slotId }) =>
      apiFetch<SlotsEnvelope>(`/api/schedule/slots/${slotId}`, {
        method: "DELETE"
      }),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["schedule-week", variables.weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", variables.weekId], (prev) => {
        if (!prev) {
          return prev;
        }
        return {
          ...prev,
          slots: data.slots ?? prev.slots,
          instructors: data.instructors ?? prev.instructors ?? []
        };
      });
    },
    onError: (error) => {
      console.error("SchedulePage: failed to delete slot", error);
    }
  });

  const updateReservationMutation = useMutation<
    { reservation: ScheduleReservation; slot?: ScheduleSlot },
    unknown,
    { reservationId: number; payload: ReservationPayload }
  >({
    mutationFn: ({ reservationId, payload }) =>
      apiFetch<{ reservation: ScheduleReservation; slot?: ScheduleSlot }>(`/api/schedule/reservations/${reservationId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: (data) => {
      if (!data) return;
      const weekId = data.slot?.week_id ?? currentWeek?.id ?? null;
      if (!weekId) return;
      queryClient.invalidateQueries({ queryKey: ["schedule-week", weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", weekId], (prev) => {
        if (!prev) return prev;
        const updatedSlots = prev.slots.map((slot) => {
          if (slot.id !== (data.slot?.id ?? slot.id)) {
            return slot;
          }
          if (data.slot) {
            return data.slot;
          }
          return {
            ...slot,
            reservations: slot.reservations.map((reservationEntry) =>
              reservationEntry.id === data.reservation.id ? data.reservation : reservationEntry
            )
          };
        });
        return { ...prev, slots: updatedSlots };
      });
    },
    onError: (error) => {
      console.error("SchedulePage: failed to update reservation", error);
    }
  });

  const clearReservationMutation = useMutation<
    { reservation: ScheduleReservation; slot?: ScheduleSlot },
    unknown,
    number
  >({
    mutationFn: (reservationId: number) =>
      apiFetch<{ reservation: ScheduleReservation; slot?: ScheduleSlot }>(`/api/schedule/reservations/${reservationId}/clear`, {
        method: "POST"
      }),
    onSuccess: (data) => {
      if (!data) return;
      const weekId = data.slot?.week_id ?? currentWeek?.id ?? null;
      if (!weekId) return;
      queryClient.invalidateQueries({ queryKey: ["schedule-week", weekId] });
      queryClient.setQueryData<ScheduleWeekDetailResponse>(["schedule-week", weekId], (prev) => {
        if (!prev) return prev;
        const updatedSlots = prev.slots.map((slot) => {
          if (slot.id !== (data.slot?.id ?? slot.id)) {
            return slot;
          }
          if (data.slot) {
            return data.slot;
          }
          return {
            ...slot,
            reservations: slot.reservations.map((reservationEntry) =>
              reservationEntry.id === data.reservation.id ? data.reservation : reservationEntry
            )
          };
        });
        return { ...prev, slots: updatedSlots };
      });
    },
    onError: (error) => {
      console.error("SchedulePage: failed to clear reservation", error);
    }
  });

  function handleCreateWeek(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isAdmin || !newWeekDate) return;
    const payload: CreateWeekPayload = {
      weekStartDate: newWeekDate
    };
    if (newWeekCopySource !== "" && typeof newWeekCopySource === "number") {
      payload.copyFromWeekId = newWeekCopySource;
      payload.replace = newWeekReplace;
    } else if (newWeekReplace) {
      payload.replace = true;
    }
    createWeekMutation.mutate(payload);
  }

  function handleCopyWeek(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isAdmin || !selectedWeekId) return;
    if (copySourceWeekId === "" || typeof copySourceWeekId !== "number") {
      return;
    }
    copyWeekMutation.mutate({
      weekId: selectedWeekId,
      sourceWeekId: copySourceWeekId,
      replace: copyReplace
    });
  }

  function handleDeleteWeek() {
    if (!isAdmin || !selectedWeekId) return;
    const label = currentWeek ? formatWeekLabel(currentWeek) : `#${selectedWeekId}`;
    if (!window.confirm(`Удалить неделю ${label}? Все слоты и записи будут удалены.`)) {
      return;
    }
    deleteWeekMutation.mutate(selectedWeekId);
  }

  function handleCreateSlot(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isAdmin || !selectedWeekId) return;
    if (!newSlotDate || !newSlotStart || !newSlotEnd) return;
    if (newSlotSessionKind === "instructor" && !newSlotInstructorId) {
      window.alert("Выберите инструктора для слота.");
      return;
    }
    createSlotMutation.mutate({
      weekId: selectedWeekId,
      payload: {
        slotDate: newSlotDate,
        startTime: newSlotStart,
        endTime: newSlotEnd,
        label: newSlotLabel.trim() || undefined,
        sessionKind: newSlotSessionKind,
        instructorId:
          newSlotSessionKind === "instructor" && newSlotInstructorId ? Number(newSlotInstructorId) : null
      }
    });
  }

  function handleSlotSubmit(event: FormEvent<HTMLFormElement>, slot: ScheduleSlot) {
    event.preventDefault();
    if (!isAdmin) return;
    const formData = new FormData(event.currentTarget);
    const payload: SlotPayload = {};

    const slotDate = formData.get("slotDate");
    if (typeof slotDate === "string" && slotDate && slotDate !== slot.slot_date) {
      payload.slotDate = slotDate;
    }
    const startTime = formData.get("startTime");
    if (typeof startTime === "string" && startTime && startTime !== slot.start_time) {
      payload.startTime = startTime;
    }
    const endTime = formData.get("endTime");
    if (typeof endTime === "string" && endTime && endTime !== slot.end_time) {
      payload.endTime = endTime;
    }
    const label = formData.get("label");
    if (typeof label === "string") {
      const trimmed = label.trim();
      if ((trimmed || null) !== (slot.label ?? null)) {
        payload.label = trimmed === "" ? null : trimmed;
      }
    }
    const sessionKind = formData.get("sessionKind");
    if (typeof sessionKind === "string" && sessionKind !== slot.session_kind) {
      payload.sessionKind = sessionKind;
    }
    const instructorId = formData.get("instructorId");
    if (typeof instructorId === "string") {
      const trimmed = instructorId.trim();
      const currentValue = slot.instructorId != null ? String(slot.instructorId) : "";
      if (trimmed !== currentValue) {
        if (trimmed === "") {
          payload.instructorId = null;
        } else {
          const parsed = Number(trimmed);
          if (!Number.isNaN(parsed)) {
            payload.instructorId = parsed;
          }
        }
      }
    }

    if (
      ("sessionKind" in payload && payload.sessionKind !== "instructor") ||
      (payload.sessionKind === undefined && slot.session_kind !== "instructor" && slot.instructorId != null)
    ) {
      payload.instructorId = null;
    }

    const nextSessionKind =
      typeof payload.sessionKind === "string" ? (payload.sessionKind as string) : slot.session_kind;
    const nextInstructorId =
      payload.instructorId !== undefined ? payload.instructorId : (slot.instructorId ?? null);
    if (nextSessionKind === "instructor" && (nextInstructorId === null || Number.isNaN(nextInstructorId))) {
      window.alert("Выберите инструктора для этого слота.");
      return;
    }

    if (Object.keys(payload).length === 0) {
      return;
    }

    updateSlotMutation.mutate({ slotId: slot.id, weekId: slot.week_id, payload });
  }

  function handleReservationSubmit(event: FormEvent<HTMLFormElement>, reservation: ScheduleReservation) {
    event.preventDefault();
    if (!isAdmin) return;
    const formData = new FormData(event.currentTarget);
    const payload: ReservationPayload = {};

    const clientName = formData.get("clientName");
    if (typeof clientName === "string") {
      const trimmed = clientName.trim();
      const current = reservation.client_name ?? "";
      if (trimmed !== current) {
        payload.clientName = trimmed === "" ? null : trimmed;
      }
    }

    const status = formData.get("status");
    if (typeof status === "string" && status && status !== reservation.status) {
      payload.status = status;
    }

    if (Object.keys(payload).length === 0) {
      return;
    }

    updateReservationMutation.mutate({ reservationId: reservation.id, payload });
  }

  function handleReservationClear(reservation: ScheduleReservation) {
    if (!isAdmin) return;
    clearReservationMutation.mutate(reservation.id);
  }

  function handleSlotDelete(slot: ScheduleSlot) {
    if (!isAdmin) return;
    if (!window.confirm("Удалить слот и все записи?")) return;
    deleteSlotMutation.mutate({ slotId: slot.id, weekId: slot.week_id });
  }

  function renderReservation(reservation: ScheduleReservation) {
    const standLabel =
      (reservation.stand_id != null ? standLookup.get(reservation.stand_id) : null) ||
      reservation.stand_code ||
      "Станок";
    const clientName = reservation.client_name ? reservation.client_name.trim() : "";
    const primaryLabel = clientName || standLabel;
    const statusLabel = RESERVATION_STATUS_LABELS[reservation.status] ?? reservation.status;

    if (!isAdmin) {
      return (
        <div key={reservation.id} className="schedule-reservation">
          <span className="schedule-reservation-stand">{primaryLabel}</span>
          <span className="schedule-reservation-name">{clientName ? standLabel : ""}</span>
          <span className={classNames("schedule-reservation-status", reservation.status)}>{statusLabel}</span>
        </div>
      );
    }

    const isSaving = updateReservationMutation.isPending || clearReservationMutation.isPending;

    return (
      <form
        key={reservation.id}
        className="schedule-reservation schedule-reservation-editable"
        onSubmit={(event) => handleReservationSubmit(event, reservation)}
      >
        <span className="schedule-reservation-stand">{primaryLabel}</span>
        <input
          type="text"
          name="clientName"
          defaultValue={reservation.client_name ?? ""}
          placeholder={standLabel}
          disabled={isSaving}
        />
        <select name="status" defaultValue={reservation.status} disabled={isSaving}>
          {Object.entries(RESERVATION_STATUS_LABELS).map(([value, text]) => (
            <option key={value} value={value}>
              {text}
            </option>
          ))}
        </select>
        <div className="schedule-reservation-actions">
          <button type="submit" className="btn primary" disabled={isSaving}>
            Сохранить
          </button>
          <button
            type="button"
            className="btn ghost"
            onClick={() => handleReservationClear(reservation)}
            disabled={isSaving}
          >
            Очистить
          </button>
        </div>
      </form>
    );
  }

  function renderSlot(slot: ScheduleSlot) {
    const sessionKindLabel =
      SESSION_KIND_OPTIONS.find((option) => option.value === slot.session_kind)?.label ?? slot.session_kind;
    const isProcessing = updateSlotMutation.isPending || deleteSlotMutation.isPending;
    const collapsed = collapsedSlots[slot.id] ?? true;
    const { occupied, free } = summarizeReservations(slot.reservations);
    const slotLabel = (slot.label ?? "").trim() || sessionKindLabel;
    const instructorName =
      slot.instructorName ??
      (slot.instructorId != null ? instructors.find((item) => item.id === slot.instructorId)?.full_name : null);

    return (
      <div key={slot.id} className={classNames("schedule-slot-card", { collapsed })}>
        <button
          type="button"
          className={classNames("schedule-slot-summary", { collapsed })}
          onClick={() => handleToggleSlot(slot.id)}
        >
          <span className="schedule-slot-arrow">{collapsed ? "▶" : "▼"}</span>
          <div className="schedule-slot-summary-info">
            <div className="schedule-slot-time">
              {slot.start_time}
              {"-"}
              {slot.end_time}
            </div>
            <div className="schedule-slot-meta">{slotLabel}</div>
            {instructorName ? (
              <div className="schedule-slot-instructor">Инструктор: {instructorName}</div>
            ) : null}
            <div className="schedule-slot-stats">
              <span className="occupied">Занято {occupied}</span>
              <span className="free">Свободно {free}</span>
            </div>
          </div>
        </button>
        {!collapsed ? (
          <div className="schedule-slot-body">
            <div className="schedule-slot-details">
              <span className="schedule-slot-detail-label">Инструктор:</span>
              <span className="schedule-slot-detail-value">{instructorName ?? "—"}</span>
            </div>
            {isAdmin ? (
              <div className="schedule-slot-header-actions">
                <form className="schedule-slot-form" onSubmit={(event) => handleSlotSubmit(event, slot)}>
                  <input type="date" name="slotDate" defaultValue={slot.slot_date} />
                  <input type="time" name="startTime" defaultValue={slot.start_time} />
                  <input type="time" name="endTime" defaultValue={slot.end_time} />
                  <input type="text" name="label" defaultValue={slot.label ?? ""} placeholder="Метка" />
                  <select name="sessionKind" defaultValue={slot.session_kind}>
                    {SESSION_KIND_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                  <select name="instructorId" defaultValue={slot.instructorId ?? ""}>
                    <option value="">— Без инструктора —</option>
                    {instructors.map((instructor) => (
                      <option key={instructor.id} value={instructor.id}>
                        {instructor.full_name}
                      </option>
                    ))}
                  </select>
                  <button type="submit" className="btn primary" disabled={isProcessing}>
                    Обновить
                  </button>
                </form>
                <button
                  type="button"
                  className="btn danger"
                  onClick={() => handleSlotDelete(slot)}
                  disabled={isProcessing}
                >
                  Удалить
                </button>
              </div>
            ) : null}
            <div className="schedule-reservations">
              {slot.reservations.map((reservation) => renderReservation(reservation))}
            </div>
          </div>
        ) : null}
      </div>
    );
  }

  return (
    <Panel title="Расписание" subtitle="Управляйте недельными слотами и станками">
      <div className="schedule-public-link-banner">
        <a href="/schedule" target="_blank" rel="noopener noreferrer" className="btn ghost">
          Публичное расписание
        </a>
      </div>
      <div className="schedule-layout">
        <aside className="schedule-sidebar">
          <div className="schedule-sidebar-title">Недели</div>
          {weeksQuery.isLoading ? (
            <div className="schedule-empty">Загружаем недели…</div>
          ) : weeksQuery.isError ? (
            <div className="schedule-error">Не удалось загрузить список недель.</div>
          ) : weeksQuery.data && weeksQuery.data.items.length > 0 ? (
            <div className="schedule-week-list">
              {weeksQuery.data.items.map((week) => (
                <button
                  key={week.id}
                  type="button"
                  onClick={() => setSelectedWeekId(week.id)}
                  className={classNames("schedule-week-item", { active: week.id === selectedWeekId })}
                >
                  <span className="schedule-week-label">{formatWeekLabel(week)}</span>
                  <span className="schedule-week-meta">
                    {week.slots_count ?? 0} слотов · {week.week_start_date}
                  </span>
                </button>
              ))}
            </div>
          ) : (
            <div className="schedule-empty">Недели не созданы.</div>
          )}

          {isAdmin ? (
            <div className="schedule-sidebar-section">
              <div className="schedule-sidebar-subheader">Создать неделю</div>
              <form className="schedule-form" onSubmit={handleCreateWeek}>
                <label>
                  Дата понедельника
                  <input
                    type="date"
                    value={newWeekDate}
                    onChange={(event) => setNewWeekDate(event.target.value)}
                    required
                  />
                </label>
                <label>
                  Скопировать слоты
                  <select
                    value={newWeekCopySource === "" ? "" : String(newWeekCopySource)}
                    onChange={(event) =>
                      setNewWeekCopySource(event.target.value === "" ? "" : Number(event.target.value))
                    }
                  >
                    <option value="">— Без копирования —</option>
                    {(weeksQuery.data?.items ?? []).map((week) => (
                      <option key={week.id} value={week.id}>
                        {formatWeekLabel(week)}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="schedule-checkbox">
                  <input
                    type="checkbox"
                    checked={newWeekReplace}
                    onChange={(event) => setNewWeekReplace(event.target.checked)}
                  />
                  Очистить существующую неделю
                </label>
                <button type="submit" className="btn primary" disabled={createWeekMutation.isPending}>
                  Создать
                </button>
              </form>
            </div>
          ) : null}
        </aside>

        <section className="schedule-content">
          {selectedWeekId === null ? (
            <div className="schedule-empty">Выберите неделю слева.</div>
          ) : weekDetailQuery.isLoading ? (
            <div className="schedule-empty">Загружаем слоты недели…</div>
          ) : weekDetailQuery.isError ? (
            <div className="schedule-error">Не удалось загрузить детали недели.</div>
          ) : (
            <>
              <div className="schedule-toolbar">
                <div className="schedule-week-info">
                  <div className="schedule-week-title">{formatWeekLabel(currentWeek!)}</div>
                  <div className="schedule-week-subtitle">
                    {currentWeek?.week_start_date}
                    {currentWeek?.notes ? ` · ${currentWeek.notes}` : ""}
                  </div>
                </div>
                {isAdmin ? (
                  <div className="schedule-toolbar-actions">
                    <form className="schedule-form inline" onSubmit={handleCopyWeek}>
                      <select
                        value={copySourceWeekId === "" ? "" : String(copySourceWeekId)}
                        onChange={(event) =>
                          setCopySourceWeekId(event.target.value === "" ? "" : Number(event.target.value))
                        }
                      >
                        <option value="">— Источник слотов —</option>
                        {(weeksQuery.data?.items ?? [])
                          .filter((week) => week.id !== selectedWeekId)
                          .map((week) => (
                            <option key={week.id} value={week.id}>
                              {formatWeekLabel(week)}
                            </option>
                          ))}
                      </select>
                      <label className="schedule-checkbox">
                        <input
                          type="checkbox"
                          checked={copyReplace}
                          onChange={(event) => setCopyReplace(event.target.checked)}
                        />
                        Перезаписать слоты
                      </label>
                      <button type="submit" className="btn primary" disabled={copyWeekMutation.isPending}>
                        Копировать слоты
                      </button>
                    </form>
                    <button
                      type="button"
                      className="btn ghost"
                      onClick={() => selectedWeekId && syncWeekMutation.mutate(selectedWeekId)}
                      disabled={syncWeekMutation.isPending}
                    >
                      Синхронизировать станки
                    </button>
                    <button
                      type="button"
                      className="btn ghost"
                      onClick={() =>
                        selectedWeekId &&
                        fillTemplateMutation.mutate({
                          weekId: selectedWeekId,
                          force: fillTemplateMutation.variables?.force ?? false
                        })
                      }
                      disabled={fillTemplateMutation.isPending || !selectedWeekId}
                    >
                      Заполнить по шаблону
                    </button>
                    <button
                      type="button"
                      className="btn danger"
                      onClick={handleDeleteWeek}
                      disabled={deleteWeekMutation.isPending || !selectedWeekId}
                    >
                      Удалить неделю
                    </button>
                  </div>
                ) : null}
              </div>

              {isAdmin ? (
                <form className="schedule-form slot-create" onSubmit={handleCreateSlot}>
                  <h3>Новый слот</h3>
                  <div className="slot-create-grid">
                    <label>
                      Дата
                      <input
                        type="date"
                        value={newSlotDate}
                        onChange={(event) => setNewSlotDate(event.target.value)}
                        required
                      />
                    </label>
                    <label>
                      Начало
                      <input
                        type="time"
                        value={newSlotStart}
                        onChange={(event) => setNewSlotStart(event.target.value)}
                        required
                      />
                    </label>
                    <label>
                      Конец
                      <input
                        type="time"
                        value={newSlotEnd}
                        onChange={(event) => setNewSlotEnd(event.target.value)}
                        required
                      />
                    </label>
                    <label>
                      Метка
                      <input
                        type="text"
                        value={newSlotLabel}
                        onChange={(event) => setNewSlotLabel(event.target.value)}
                        placeholder="Например, тренер"
                      />
                    </label>
                    <label>
                      Режим
                      <select
                        value={newSlotSessionKind}
                        onChange={(event) => setNewSlotSessionKind(event.target.value)}
                      >
                        {SESSION_KIND_OPTIONS.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Инструктор
                      <select
                        value={newSlotInstructorId}
                        onChange={(event) => setNewSlotInstructorId(event.target.value)}
                        disabled={newSlotSessionKind !== "instructor" || instructors.length === 0}
                      >
                        <option value="">— Выберите инструктора —</option>
                        {instructors.map((instructor) => (
                          <option key={instructor.id} value={instructor.id}>
                            {instructor.full_name}
                          </option>
                        ))}
                      </select>
                      {newSlotSessionKind === "instructor" && instructors.length === 0 ? (
                        <div className="schedule-hint">Сначала добавьте инструктора в разделе «Инструкторы».</div>
                      ) : null}
                    </label>
                  </div>
                  <button
                    type="submit"
                    className="btn primary"
                    disabled={
                      createSlotMutation.isPending ||
                      (newSlotSessionKind === "instructor" && (!newSlotInstructorId || instructors.length === 0))
                    }
                  >
                    Добавить слот
                  </button>
                </form>
              ) : null}

              {slots.length === 0 ? (
                <div className="schedule-empty">В выбранной неделе пока нет слотов.</div>
              ) : (
                <div className="schedule-slots">
                  {slotsGrouped.map((group) => (
                    <div key={group.slotDate} className="schedule-day">
                      <button
                        type="button"
                        className={classNames("schedule-day-header", {
                          collapsed: collapsedDays[group.slotDate]
                        })}
                        onClick={() => handleToggleDay(group.slotDate)}
                      >
                        <span className="schedule-day-icon">
                          {collapsedDays[group.slotDate] ? "▶" : "▼"}
                        </span>
                        <span>{formatDateLabel(group.slotDate)}</span>
                        <span className="schedule-day-count">{group.slots.length}</span>
                      </button>
                      {!collapsedDays[group.slotDate] ? (
                        <div className="schedule-day-slots">{group.slots.map((slot) => renderSlot(slot))}</div>
                      ) : null}
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </section>
      </div>
    </Panel>
  );
}
