import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { FormEvent } from "react";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { AdminListResponse, AdminRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

export default function AdminsPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();

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
    mutationFn: ({ adminId, tg_id, username }: { adminId: number; tg_id?: number | null; username?: string | null }) =>
      apiFetch(`/api/admins/${adminId}?tg_id=${tg_id ?? ""}&username=${username ?? ""}`, {
        method: "DELETE"
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
    if (!tgId && !username) return;
    if (tgId) {
      const tgNumber = Number(tgId);
      if (!Number.isNaN(tgNumber)) {
        payload.tg_id = tgNumber;
      }
    }
    if (username) payload.username = (username as string).trim();
    if (displayName) payload.display_name = (displayName as string).trim();
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
            <button
              type="button"
              className="button danger"
              onClick={() => deleteMutation.mutate({ adminId: item.id, tg_id: item.tg_id ?? undefined, username: item.username ?? undefined })}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending ? "Удаляем…" : "Удалить"}
            </button>
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
