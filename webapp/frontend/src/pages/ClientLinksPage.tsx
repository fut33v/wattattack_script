import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { ClientLinkListResponse, ClientLinkRow, VkClientLinkListResponse, VkClientLinkRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

export default function ClientLinksPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<"tg" | "vk" | "strava">("tg");

  const tgQuery = useQuery<ClientLinkListResponse>({
    queryKey: ["client-links"],
    queryFn: () => apiFetch<ClientLinkListResponse>("/api/client-links")
  });

  const vkQuery = useQuery<VkClientLinkListResponse>({
    queryKey: ["vk-client-links"],
    queryFn: () => apiFetch<VkClientLinkListResponse>("/api/vk-client-links")
  });

  const tgCreate = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch("/api/client-links", {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["client-links"] })
  });

  const vkCreate = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch("/api/vk-client-links", {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["vk-client-links"] })
  });

  const tgUpdate = useMutation({
    mutationFn: ({ clientId, payload }: { clientId: number; payload: Record<string, unknown> }) =>
      apiFetch(`/api/client-links/${clientId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["client-links"] })
  });

  const vkUpdate = useMutation({
    mutationFn: ({ clientId, payload }: { clientId: number; payload: Record<string, unknown> }) =>
      apiFetch(`/api/vk-client-links/${clientId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["vk-client-links"] })
  });

  const tgDelete = useMutation({
    mutationFn: (clientId: number) =>
      apiFetch(`/api/client-links/${clientId}`, {
        method: "DELETE"
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["client-links"] })
  });

  const vkDelete = useMutation({
    mutationFn: (clientId: number) =>
      apiFetch(`/api/vk-client-links/${clientId}`, {
        method: "DELETE"
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["vk-client-links"] })
  });

  if (!session.isAdmin) {
    return (
      <Panel title="Связки" subtitle="Только администраторы могут управлять связками">
        <div className="empty-state">Недостаточно прав.</div>
      </Panel>
    );
  }

  const tgError =
    tgCreate.isError && tgCreate.error instanceof Error ? `Не удалось создать связку: ${tgCreate.error.message}` : null;
  const vkError =
    vkCreate.isError && vkCreate.error instanceof Error ? `Не удалось создать связку: ${vkCreate.error.message}` : null;

  return (
    <Panel
      title="Связки"
      subtitle="Привязка клиентов к мессенджерам"
      headerExtra={
        <div className="tabs">
          <button className={`tab ${activeTab === "tg" ? "tab--active" : ""}`} onClick={() => setActiveTab("tg")}>
            Telegram
          </button>
          <button className={`tab ${activeTab === "vk" ? "tab--active" : ""}`} onClick={() => setActiveTab("vk")}>
            ВКонтакте
          </button>
          <button
            className={`tab ${activeTab === "strava" ? "tab--active" : ""}`}
            onClick={() => setActiveTab("strava")}
          >
            Strava
          </button>
        </div>
      }
    >
      {activeTab === "tg" ? (
        <TelegramSection
          listQuery={tgQuery}
          createError={tgError}
          onCreate={handleTgCreate}
          onUpdate={handleTgUpdate}
          onDelete={handleTgDelete}
          onStravaConnect={handleStravaConnect}
          updatePending={tgUpdate.isPending}
          deletePending={tgDelete.isPending}
        />
      ) : (
        activeTab === "vk" ? (
          <VkSection
            listQuery={vkQuery}
            createError={vkError}
            onCreate={handleVkCreate}
            onUpdate={handleVkUpdate}
            onDelete={handleVkDelete}
            updatePending={vkUpdate.isPending}
            deletePending={vkDelete.isPending}
          />
        ) : (
          <StravaSection listQuery={tgQuery} />
        )
      )}
    </Panel>
  );

  function handleTgCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    tgCreate.reset();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const clientId = Number(formData.get("client_id"));
    const tgUserId = Number(formData.get("tg_user_id"));
    if (!clientId || !tgUserId || Number.isNaN(clientId) || Number.isNaN(tgUserId)) return;

    const payload: Record<string, unknown> = { client_id: clientId, tg_user_id: tgUserId };
    const tgUsername = (formData.get("tg_username") as string | null)?.trim();
    const tgFullName = (formData.get("tg_full_name") as string | null)?.trim();
    if (tgUsername) payload.tg_username = tgUsername;
    if (tgFullName) payload.tg_full_name = tgFullName;

    tgCreate.mutate(payload, {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ["client-links"] });
        form.reset();
      }
    });
  }

  function handleVkCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    vkCreate.reset();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const clientId = Number(formData.get("client_id"));
    const vkUserId = Number(formData.get("vk_user_id"));
    if (!clientId || !vkUserId || Number.isNaN(clientId) || Number.isNaN(vkUserId)) return;

    const payload: Record<string, unknown> = { client_id: clientId, vk_user_id: vkUserId };
    const vkUsername = (formData.get("vk_username") as string | null)?.trim();
    const vkFullName = (formData.get("vk_full_name") as string | null)?.trim();
    if (vkUsername) payload.vk_username = vkUsername;
    if (vkFullName) payload.vk_full_name = vkFullName;

    vkCreate.mutate(payload, {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ["vk-client-links"] });
        form.reset();
      }
    });
  }

  function handleTgUpdate(event: FormEvent<HTMLFormElement>, row: ClientLinkRow) {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {
      tg_user_id: Number(formData.get("tg_user_id")),
      tg_username: (formData.get("tg_username") as string | null)?.trim() || null,
      tg_full_name: (formData.get("tg_full_name") as string | null)?.trim() || null
    };
    tgUpdate.mutate({ clientId: row.client_id, payload });
  }

  function handleVkUpdate(event: FormEvent<HTMLFormElement>, row: VkClientLinkRow) {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {
      vk_user_id: Number(formData.get("vk_user_id")),
      vk_username: (formData.get("vk_username") as string | null)?.trim() || null,
      vk_full_name: (formData.get("vk_full_name") as string | null)?.trim() || null
    };
    vkUpdate.mutate({ clientId: row.client_id, payload });
  }

  function handleTgDelete(clientId: number) {
    tgDelete.mutate(clientId);
  }

  function handleVkDelete(clientId: number) {
    vkDelete.mutate(clientId);
  }

  function handleStravaConnect(row: ClientLinkRow) {
    const stravaAuthUrl = `/strava/authorize?state=${row.tg_user_id}`;
    window.open(stravaAuthUrl, "_blank");
  }
}

function TelegramSection({
  listQuery,
  createError,
  onCreate,
  onUpdate,
  onDelete,
  onStravaConnect,
  updatePending,
  deletePending
}: {
  listQuery: ReturnType<typeof useQuery<ClientLinkListResponse>>;
  createError: string | null;
  onCreate: (e: FormEvent<HTMLFormElement>) => void;
  onUpdate: (e: FormEvent<HTMLFormElement>, row: ClientLinkRow) => void;
  onDelete: (clientId: number) => void;
  onStravaConnect: (row: ClientLinkRow) => void;
  updatePending: boolean;
  deletePending: boolean;
}) {
  return (
    <>
      {createError && <div className="form-error">{createError}</div>}
      <form className="admin-form" onSubmit={onCreate}>
        <input type="number" name="client_id" placeholder="ID клиента" required />
        <input type="number" name="tg_user_id" placeholder="Telegram ID" required />
        <input type="text" name="tg_username" placeholder="username (опционально)" />
        <input type="text" name="tg_full_name" placeholder="Имя (опционально)" />
        <button type="submit" className="button">
          Создать
        </button>
      </form>
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем связки…</div>
      ) : (
        <DataGrid<ClientLinkRow>
          items={listQuery.data?.items ?? []}
          getRowKey={(item) => item.client_id}
          emptyMessage={<div className="empty-state">Связок нет.</div>}
          actions={(item) => (
            <div className="row-actions">
              <form id={`link-${item.client_id}`} className="row-form" onSubmit={(event) => onUpdate(event, item)}>
                <button type="submit" className="button">
                  {updatePending ? "Сохраняю…" : "Сохранить"}
                </button>
              </form>
              <button
                type="button"
                className="button danger"
                onClick={() => onDelete(item.client_id)}
                disabled={deletePending}
              >
                {deletePending ? "Удаляю…" : "Удалить"}
              </button>
            </div>
          )}
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
            { key: "created_at", title: "Создано", render: (item) => formatDate(item.created_at) },
            { key: "updated_at", title: "Обновлено", render: (item) => formatDate(item.updated_at) }
          ]}
        />
      )}
    </>
  );
}

function VkSection({
  listQuery,
  createError,
  onCreate,
  onUpdate,
  onDelete,
  updatePending,
  deletePending
}: {
  listQuery: ReturnType<typeof useQuery<VkClientLinkListResponse>>;
  createError: string | null;
  onCreate: (e: FormEvent<HTMLFormElement>) => void;
  onUpdate: (e: FormEvent<HTMLFormElement>, row: VkClientLinkRow) => void;
  onDelete: (clientId: number) => void;
  updatePending: boolean;
  deletePending: boolean;
}) {
  return (
    <>
      {createError && <div className="form-error">{createError}</div>}
      <form className="admin-form" onSubmit={onCreate}>
        <input type="number" name="client_id" placeholder="ID клиента" required />
        <input type="number" name="vk_user_id" placeholder="VK user ID" required />
        <input type="text" name="vk_username" placeholder="username (опционально)" />
        <input type="text" name="vk_full_name" placeholder="Имя (опционально)" />
        <button type="submit" className="button">
          Создать
        </button>
      </form>
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем VK-связки…</div>
      ) : (
        <DataGrid<VkClientLinkRow>
          items={listQuery.data?.items ?? []}
          getRowKey={(item) => item.client_id}
          emptyMessage={<div className="empty-state">VK-связок нет.</div>}
          actions={(item) => (
            <div className="row-actions">
              <form id={`vk-link-${item.client_id}`} className="row-form" onSubmit={(event) => onUpdate(event, item)}>
                <button type="submit" className="button">
                  {updatePending ? "Сохраняю…" : "Сохранить"}
                </button>
              </form>
              <button
                type="button"
                className="button danger"
                onClick={() => onDelete(item.client_id)}
                disabled={deletePending}
              >
                {deletePending ? "Удаляю…" : "Удалить"}
              </button>
            </div>
          )}
          columns={[
            {
              key: "client_id",
              title: "Client ID",
              className: "cell-id",
              render: (item) => <div className="id-chip">#{item.client_id}</div>
            },
            {
              key: "vk_user_id",
              title: "VK ID",
              render: (item) => (
                <input type="number" name="vk_user_id" defaultValue={item.vk_user_id} form={`vk-link-${item.client_id}`} />
              )
            },
            {
              key: "vk_username",
              title: "Username",
              render: (item) => (
                <input type="text" name="vk_username" defaultValue={item.vk_username ?? ""} form={`vk-link-${item.client_id}`} />
              )
            },
            {
              key: "vk_full_name",
              title: "Имя",
              render: (item) => (
                <input type="text" name="vk_full_name" defaultValue={item.vk_full_name ?? ""} form={`vk-link-${item.client_id}`} />
              )
            },
            { key: "created_at", title: "Создано", render: (item) => formatDate(item.created_at) },
            { key: "updated_at", title: "Обновлено", render: (item) => formatDate(item.updated_at) }
          ]}
        />
      )}
    </>
  );
}

function formatDate(value?: string | null) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function StravaSection({ listQuery }: { listQuery: ReturnType<typeof useQuery<ClientLinkListResponse>> }) {
  const items = (listQuery.data?.items ?? []).filter(
    (item) => item.strava_connected || item.strava_access_token || item.strava_athlete_name
  );

  return listQuery.isLoading ? (
    <div className="empty-state">Загружаем Strava свзяки…</div>
  ) : (
    <DataGrid<ClientLinkRow>
      items={items}
      getRowKey={(item) => item.client_id}
      emptyMessage={<div className="empty-state">Strava-связок нет.</div>}
      columns={[
        {
          key: "client_id",
          title: "Client ID",
          className: "cell-id",
          render: (item) => <div className="id-chip">#{item.client_id}</div>
        },
        { key: "tg_user_id", title: "Telegram ID", render: (item) => item.tg_user_id ?? "—" },
        {
          key: "status",
          title: "Статус",
          render: (item) => (
            <span className="status-badge status-badge--success">
              Подключена{item.strava_athlete_name ? ` (${item.strava_athlete_name})` : ""}
            </span>
          )
        },
        { key: "created_at", title: "Создано", render: (item) => formatDate(item.created_at) },
        { key: "updated_at", title: "Обновлено", render: (item) => formatDate(item.updated_at) }
      ]}
    />
  );
}
