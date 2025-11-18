import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { FormEvent } from "react";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { ClientLinkListResponse, ClientLinkRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

export default function ClientLinksPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();

  const listQuery = useQuery<ClientLinkListResponse>({
    queryKey: ["client-links"],
    queryFn: () => apiFetch<ClientLinkListResponse>("/api/client-links")
  });

  const createMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch("/api/client-links", {
        method: "POST",
        body: JSON.stringify(payload)
      })
  });

  const updateMutation = useMutation({
    mutationFn: ({ clientId, payload }: { clientId: number; payload: Record<string, unknown> }) =>
      apiFetch(`/api/client-links/${clientId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["client-links"] });
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (clientId: number) =>
      apiFetch(`/api/client-links/${clientId}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["client-links"] });
    }
  });

  const createErrorMessage =
    createMutation.isError && createMutation.error instanceof Error
      ? createMutation.error.message
      : null;

  if (!session.isAdmin) {
    return (
      <Panel title="Связки" subtitle="Только администраторы могут управлять связками">
        <div className="empty-state">Недостаточно прав.</div>
      </Panel>
    );
  }

  function handleCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    createMutation.reset();

    const form = event.currentTarget;
    const formData = new FormData(form);
    const clientIdValue = formData.get("client_id");
    const tgUserIdValue = formData.get("tg_user_id");
    if (!clientIdValue || !tgUserIdValue) return;

    const clientId = Number(clientIdValue);
    const tgUserId = Number(tgUserIdValue);
    if (Number.isNaN(clientId) || Number.isNaN(tgUserId)) return;

    const payload: Record<string, unknown> = {
      client_id: clientId,
      tg_user_id: tgUserId
    };

    const tgUsername = (formData.get("tg_username") as string | null)?.trim();
    const tgFullName = (formData.get("tg_full_name") as string | null)?.trim();
    if (tgUsername) payload.tg_username = tgUsername;
    if (tgFullName) payload.tg_full_name = tgFullName;

    createMutation.mutate(payload, {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ["client-links"] });
        form.reset();
      }
    });
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>, row: ClientLinkRow) {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const payload: Record<string, unknown> = {};
    const tgUserId = formData.get("tg_user_id");
    if (!tgUserId) return;
    const tgNumber = Number(tgUserId);
    if (Number.isNaN(tgNumber)) return;
    payload.tg_user_id = tgNumber;
    payload.tg_username = (formData.get("tg_username") as string | null)?.trim() || null;
    payload.tg_full_name = (formData.get("tg_full_name") as string | null)?.trim() || null;
    updateMutation.mutate({ clientId: row.client_id, payload });
  }

  function handleStravaConnect(row: ClientLinkRow) {
    // Get the Strava authorization URL from the backend
    const stravaAuthUrl = `/strava/authorize?state=${row.tg_user_id}`;
    window.open(stravaAuthUrl, '_blank');
  }

  return (
    <Panel
      title="Связки"
      subtitle="Привязка клиентов к Telegram-аккаунтам"
      headerExtra={
        <form className="admin-form" onSubmit={handleCreate}>
          <input type="number" name="client_id" placeholder="ID клиента" required />
          <input type="number" name="tg_user_id" placeholder="Telegram ID" required />
          <input type="text" name="tg_username" placeholder="username (опционально)" />
          <input type="text" name="tg_full_name" placeholder="Имя (опционально)" />
          <button type="submit" className="button">
            {createMutation.isPending ? "Создаем…" : "Создать"}
          </button>
        </form>
      }
    >
      {createErrorMessage && <div className="form-error">Не удалось создать связку: {createErrorMessage}</div>}
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем связки…</div>
      ) : (
        <DataGrid<ClientLinkRow>
          items={listQuery.data?.items ?? []}
          getRowKey={(item) => item.client_id}
          emptyMessage={<div className="empty-state">Связок нет.</div>}
          actions={(item) => renderActions(item)}
          columns={[
            {
              key: "client_id",
              title: "Client ID",
              className: "cell-id",
              render: (item) => <div className="id-chip">#{item.client_id}</div>
            },
            {
              key: "tg_user_id",
              title: "Telegram ID",
              render: (item) => (
                <input type="number" name="tg_user_id" defaultValue={item.tg_user_id} form={`link-${item.client_id}`} />
              )
            },
            {
              key: "tg_username",
              title: "Username",
              render: (item) => (
                <input type="text" name="tg_username" defaultValue={item.tg_username ?? ""} form={`link-${item.client_id}`} />
              )
            },
            {
              key: "tg_full_name",
              title: "Имя",
              render: (item) => (
                <input type="text" name="tg_full_name" defaultValue={item.tg_full_name ?? ""} form={`link-${item.client_id}`} />
              )
            },
            {
              key: "strava",
              title: "Strava",
              render: (item) => (
                <div>
                  {item.strava_connected || item.strava_access_token ? (
                    <span className="status-badge status-badge--success">
                      Подключена{item.strava_athlete_name ? ` (${item.strava_athlete_name})` : ""}
                    </span>
                  ) : (
                    <span className="status-badge status-badge--warning">Не подключена</span>
                  )}
                </div>
              )
            },
            {
              key: "created_at",
              title: "Создано",
              render: (item) => item.created_at ?? "—"
            },
            {
              key: "updated_at",
              title: "Обновлено",
              render: (item) => item.updated_at ?? "—"
            }
          ]}
        />
      )}
    </Panel>
  );

  function renderActions(item: ClientLinkRow) {
    return (
      <div className="row-actions">
        <form id={`link-${item.client_id}`} className="row-form" onSubmit={(event) => handleSubmit(event, item)}>
          <button type="submit" className="button">
            {updateMutation.isPending ? "Сохраняю…" : "Сохранить"}
          </button>
        </form>
        <button
          type="button"
          className="button"
          onClick={() => handleStravaConnect(item)}
        >
          Strava
        </button>
        <button
          type="button"
          className="button danger"
          onClick={() => deleteMutation.mutate(item.client_id)}
          disabled={deleteMutation.isPending}
        >
          {deleteMutation.isPending ? "Удаляю…" : "Удалить"}
        </button>
      </div>
    );
  }
}
