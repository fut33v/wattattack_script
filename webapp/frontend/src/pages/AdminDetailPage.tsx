import { FormEvent, useMemo } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { AdminRow, InstructorListResponse } from "../lib/types";

export default function AdminDetailPage() {
  const { id } = useParams<{ id: string }>();
  const adminId = id ? Number(id) : NaN;
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const adminQuery = useQuery<{ item: AdminRow }>({
    queryKey: ["admin", adminId],
    enabled: Number.isFinite(adminId),
    queryFn: () => apiFetch<{ item: AdminRow }>(`/api/admins/${adminId}`)
  });

  const instructorsQuery = useQuery<InstructorListResponse>({
    queryKey: ["instructors"],
    queryFn: () => apiFetch<InstructorListResponse>("/api/instructors")
  });

  const updateMutation = useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      apiFetch(`/api/admins/${adminId}`, {
        method: "PATCH",
        body: JSON.stringify(body)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", adminId] });
      queryClient.invalidateQueries({ queryKey: ["admins"] });
    }
  });

  const deleteMutation = useMutation({
    mutationFn: () =>
      apiFetch(`/api/admins/${adminId}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admins"] });
      navigate("/admins");
    }
  });

  const admin = adminQuery.data?.item;
  const isLoading = adminQuery.isLoading;

  const instructorOptions = useMemo(() => instructorsQuery.data?.items ?? [], [instructorsQuery.data]);

  function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (!admin) return;
    const fd = new FormData(e.currentTarget);
    const body: Record<string, unknown> = {};
    const tgId = fd.get("tg_id");
    const username = fd.get("username");
    const displayName = fd.get("display_name");
    const instructorId = fd.get("instructor_id");
    const notifyBookingEvents = fd.get("notify_booking_events");
    const notifyInstructorOnly = fd.get("notify_instructor_only");

    if (tgId) {
      const parsed = Number(tgId);
      if (!Number.isNaN(parsed)) body.tg_id = parsed;
    } else {
      body.tg_id = null;
    }
    if (username) body.username = (username as string).trim();
    else body.username = null;
    if (displayName) body.display_name = (displayName as string).trim();
    else body.display_name = null;
    if (instructorId !== null) {
      const raw = (instructorId as string) ?? "";
      if (raw === "") {
        body.instructor_id = null;
      } else {
        const parsed = Number(raw);
        if (!Number.isNaN(parsed)) {
          body.instructor_id = parsed;
        }
      }
    }
    body.notify_booking_events = notifyBookingEvents === "on";
    body.notify_instructor_only = notifyInstructorOnly === "on";
    updateMutation.mutate(body);
  }

  return (
    <Panel title="Администратор" subtitle="Детали и редактирование">
      {isLoading || !admin ? (
        <div className="empty-state">{isLoading ? "Загружаем…" : "Админ не найден."}</div>
      ) : (
        <form className="admin-detail-form" onSubmit={handleSubmit}>
          <label>
            Telegram ID
            <input name="tg_id" type="number" defaultValue={admin.tg_id ?? ""} />
          </label>
          <label>
            Username
            <input name="username" type="text" defaultValue={admin.username ?? ""} placeholder="@username" />
          </label>
          <label>
            Имя
            <input name="display_name" type="text" defaultValue={admin.display_name ?? ""} />
          </label>
          <label>
            Инструктор
            <select name="instructor_id" defaultValue={admin.instructor_id ?? ""}>
              <option value="">Не выбран</option>
              {instructorOptions.map((ins) => (
                <option key={ins.id} value={ins.id}>
                  {ins.full_name}
                </option>
              ))}
            </select>
          </label>
          <label className="checkbox">
            <input type="checkbox" name="notify_booking_events" defaultChecked={admin.notify_booking_events ?? true} />
            <span>Получать уведомления о брони/отменах</span>
          </label>
          <label className="checkbox">
            <input type="checkbox" name="notify_instructor_only" defaultChecked={admin.notify_instructor_only ?? false} />
            <span>Только по моему инструктору</span>
          </label>
          <div className="admin-detail-actions">
            <button type="submit" className="button" disabled={updateMutation.isPending}>
              {updateMutation.isPending ? "Сохраняем…" : "Сохранить"}
            </button>
            <button
              type="button"
              className="button danger"
              onClick={() => deleteMutation.mutate()}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending ? "Удаляем…" : "Удалить"}
            </button>
          </div>
        </form>
      )}
    </Panel>
  );
}
