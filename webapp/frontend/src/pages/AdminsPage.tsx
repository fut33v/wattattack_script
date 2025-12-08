import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { AdminListResponse, AdminRow, InstructorListResponse } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

export default function AdminsPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  if (!session.isAdmin) {
    return (
      <Panel title="Администраторы" subtitle="Доступно только администраторам">
        <div className="empty-state">Недостаточно прав.</div>
      </Panel>
    );
  }

  const listQuery = useQuery<AdminListResponse>({
    queryKey: ["admins"],
    queryFn: () => apiFetch<AdminListResponse>("/api/admins")
  });
  const instructorsQuery = useQuery<InstructorListResponse>({
    queryKey: ["instructors"],
    queryFn: () => apiFetch<InstructorListResponse>("/api/instructors")
  });

  const addMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch("/api/admins", {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admins"] });
    }
  });

  const deleteMutation = useMutation({
    mutationFn: ({ adminId, tg_id, username }: { adminId: number; tg_id?: number | null; username?: string | null }) => {
      const params = new URLSearchParams();
      if (tg_id != null) params.set("tg_id", String(tg_id));
      if (username) params.set("username", username);
      const suffix = params.toString() ? `?${params.toString()}` : "";
      return apiFetch(`/api/admins/${adminId}${suffix}`, {
        method: "DELETE"
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admins"] });
    }
  });

  const updateMutation = useMutation({
    mutationFn: ({ adminId, body }: { adminId: number; body: Record<string, unknown> }) =>
      apiFetch(`/api/admins/${adminId}`, {
        method: "PATCH",
        body: JSON.stringify(body)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admins"] });
    }
  });

  function handleAdd(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const payload: Record<string, unknown> = {};
    const tgId = formData.get("tg_id");
    const username = formData.get("username");
    const displayName = formData.get("display_name");
    const instructorId = formData.get("instructor_id");
    if (!tgId && !username) return;
    if (tgId) {
      const tgNumber = Number(tgId);
      if (!Number.isNaN(tgNumber)) {
        payload.tg_id = tgNumber;
      }
    }
    if (username) payload.username = (username as string).trim();
    if (displayName) payload.display_name = (displayName as string).trim();
    if (instructorId) {
      const instrNum = Number(instructorId);
      if (!Number.isNaN(instrNum)) {
        payload.instructor_id = instrNum;
      }
    }
    addMutation.mutate(payload);
    form.reset();
  }

  return (
    <Panel
      title="Администраторы"
      subtitle="Управление списком Telegram-админов"
      headerExtra={
        <form className="admin-form" onSubmit={handleAdd}>
          <input type="number" name="tg_id" placeholder="Telegram ID" />
          <input type="text" name="username" placeholder="username" />
          <input type="text" name="display_name" placeholder="Имя" />
          <select name="instructor_id" defaultValue="">
            <option value="">Инструктор (опц.)</option>
            {(instructorsQuery.data?.items ?? []).map((ins) => (
              <option key={ins.id} value={ins.id}>
                {ins.full_name}
              </option>
            ))}
          </select>
          <label className="checkbox inline">
            <input type="checkbox" name="notify_booking_events" defaultChecked />
            <span>Уведомления</span>
          </label>
          <label className="checkbox inline">
            <input type="checkbox" name="notify_instructor_only" />
            <span>Только мой инструктор</span>
          </label>
          <button type="submit" className="button">
            {addMutation.isPending ? "Добавляем…" : "Добавить"}
          </button>
        </form>
      }
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем админов…</div>
      ) : (
        <DataGrid<AdminRow>
          items={listQuery.data?.items ?? []}
          getRowKey={(item) => item.id}
          emptyMessage={<div className="empty-state">Администраторы не найдены.</div>}
          actions={(item) => (
            <div className="admin-actions admin-actions--inline">
              <button type="button" className="button small" onClick={() => navigate(`/admins/${item.id}`)}>
                ✏️
              </button>
              <button
                type="button"
                className="button danger small"
                title="Удалить"
                onClick={() =>
                  deleteMutation.mutate({
                    adminId: item.id,
                    tg_id: item.tg_id ?? undefined,
                    username: item.username ?? undefined
                  })
                }
                disabled={deleteMutation.isPending}
              >
                🗑
              </button>
            </div>
          )}
          columns={[
            {
              key: "id",
              title: "ID",
              className: "cell-id",
              render: (item) => <div className="id-chip">#{item.id}</div>
            },
            {
              key: "tg_id",
              title: "Telegram ID",
              render: (item) => item.tg_id ?? "—"
            },
            {
              key: "username",
              title: "Username",
              render: (item) => (item.username ? `@${item.username}` : "—")
            },
            {
              key: "display_name",
              title: "Имя",
              render: (item) => item.display_name ?? "—"
            },
            {
              key: "instructor",
              title: "Инструктор",
              render: (item) => {
                if (item.instructor_id == null) return "—";
                const instructors = instructorsQuery.data?.items ?? [];
                const match = instructors.find((ins) => ins.id === item.instructor_id);
                return match ? match.full_name : `#${item.instructor_id}`;
              }
            },
            {
              key: "prefs",
              title: "Увед.",
              render: (item) => {
                const prefs: string[] = [];
                if (item.notify_booking_events !== false) {
                  prefs.push(item.notify_instructor_only ? "только мой" : "все");
                } else {
                  prefs.push("выкл");
                }
                return prefs.join(", ");
              }
            },
            {
              key: "created_at",
              title: "Добавлен",
              render: (item) => item.created_at ?? "—"
            }
          ]}
        />
      )}
    </Panel>
  );
}
