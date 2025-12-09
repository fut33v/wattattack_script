import { useState } from "react";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import { usePaginatedQuery } from "../lib/hooks";
import type {
  AccountAssignmentListResponse,
  AccountAssignmentRow,
  AssignmentNotificationListResponse,
  AssignmentNotificationRow,
  PulseNotification,
  PulseNotificationListResponse
} from "../lib/types";

function formatDateTime(value: string | null | undefined) {
  if (!value) return "—";
  const parsed = dayjs(value);
  return parsed.isValid() ? parsed.format("DD.MM.YYYY HH:mm") : value;
}

function formatSlotDate(slotDate: string | null | undefined, startTime: string | null | undefined) {
  if (!slotDate) return "—";
  const ts = startTime ? `${slotDate}T${startTime}` : slotDate;
  return formatDateTime(ts);
}

function formatSlot(slotDate: string | null | undefined, startTime: string | null | undefined) {
  if (!slotDate) return "—";
  const ts = startTime ? `${slotDate}T${startTime}` : slotDate;
  return formatDateTime(ts);
}

function renderPagination(
  page: number,
  totalPages: number,
  setPage: (value: number) => void,
  isFetching: boolean,
  pageSize?: number
) {
  return (
    <div className="pagination-controls">
      <button className="button" disabled={page <= 1 || isFetching} onClick={() => setPage(Math.max(page - 1, 1))}>
        ⟵ Назад
      </button>
      <div className="page-indicator">
        Страница {page} из {totalPages}
        {pageSize ? ` (по ${pageSize} на странице)` : ""}
      </div>
      <button className="button" disabled={page >= totalPages || isFetching} onClick={() => setPage(page + 1)}>
        Вперед ⟶
      </button>
    </div>
  );
}

export default function PulsePage() {
  const [pulsePage, setPulsePage] = useState(1);
  const [assignmentsPage, setAssignmentsPage] = useState(1);
  const [notificationsPage, setNotificationsPage] = useState(1);

  const [pulseCollapsed, setPulseCollapsed] = useState(true);
  const [assignmentsCollapsed, setAssignmentsCollapsed] = useState(true);
  const [notificationsCollapsed, setNotificationsCollapsed] = useState(true);

  const pulseQuery = usePaginatedQuery<PulseNotificationListResponse>(
    ["pulse-notifications"],
    pulsePage,
    () => {
      const params = new URLSearchParams({ page: String(pulsePage) });
      return apiFetch<PulseNotificationListResponse>(`/api/pulse/notifications?${params.toString()}`);
    },
    setPulsePage
  );

  const assignmentsQuery = usePaginatedQuery<AccountAssignmentListResponse>(
    ["account-assignments"],
    assignmentsPage,
    () => {
      const params = new URLSearchParams({ page: String(assignmentsPage) });
      return apiFetch<AccountAssignmentListResponse>(`/api/schedule/account-assignments?${params.toString()}`);
    },
    setAssignmentsPage
  );

  const assignmentNotificationsQuery = usePaginatedQuery<AssignmentNotificationListResponse>(
    ["assignment-notifications"],
    notificationsPage,
    () => {
      const params = new URLSearchParams({ page: String(notificationsPage) });
      return apiFetch<AssignmentNotificationListResponse>(`/api/schedule/assignment-notifications?${params.toString()}`);
    },
    setNotificationsPage
  );

  const pulsePagination = pulseQuery.data?.pagination;
  const assignmentsPagination = assignmentsQuery.data?.pagination;
  const assignmentNotificationsPagination = assignmentNotificationsQuery.data?.pagination;

  const pulseItems = pulseQuery.data?.items ?? [];
  const assignments = assignmentsQuery.data?.items ?? [];
  const assignmentNotifications = assignmentNotificationsQuery.data?.items ?? [];

  const buildHeaderExtra = (
    collapsed: boolean,
    toggle: () => void,
    pagination: { totalPages: number; pageSize?: number } | undefined,
    page: number,
    setPage: (value: number) => void,
    isFetching: boolean
  ) => (
    <div className="panel-actions">
      <button type="button" className="button ghost collapse-toggle" onClick={toggle}>
        <span className="chevron">{collapsed ? "▶" : "▼"}</span>
        {collapsed ? "Развернуть" : "Свернуть"}
      </button>
      {pagination && renderPagination(page, pagination.totalPages, setPage, isFetching, pagination.pageSize)}
    </div>
  );

  return (
    <div className="stack gap-lg">
      <Panel
        title="Pulse"
        subtitle="Журнал уведомлений о бронированиях и клиентах"
        headerExtra={buildHeaderExtra(
          pulseCollapsed,
          () => setPulseCollapsed((prev) => !prev),
          pulsePagination,
          pulsePage,
          setPulsePage,
          pulseQuery.isFetching
        )}
      >
        {pulseCollapsed ? (
          <div className="empty-state">Секция свернута.</div>
        ) : pulseQuery.isLoading ? (
          <div className="empty-state">Загружаем Pulse…</div>
        ) : (
          <DataGrid<PulseNotification>
            items={pulseItems}
            getRowKey={(item) => item.id}
            emptyMessage={<div className="empty-state">Пока нет уведомлений.</div>}
            columns={[
              { key: "id", title: "ID", render: (item) => item.id },
              { key: "event_type", title: "Тип", render: (item) => item.event_type },
              { key: "client", title: "Клиент", render: (item) => item.client_name || "—" },
              {
                key: "slot",
                title: "Слот",
                render: (item) => formatSlotDate(item.slot_date, item.start_time)
              },
              { key: "stand_label", title: "Станок", render: (item) => item.stand_label || "—" },
              { key: "source", title: "Источник", render: (item) => item.source || "—" },
              {
                key: "message_text",
                title: "Текст",
                render: (item) => item.message_text || "—"
              },
              {
                key: "created_at",
                title: "Создано",
                render: (item) => formatDateTime(item.created_at)
              }
            ]}
          />
        )}
      </Panel>

      <Panel
        title="Автоназначения аккаунтов"
        subtitle="Журнал фактических применений клиентских профилей"
        headerExtra={buildHeaderExtra(
          assignmentsCollapsed,
          () => setAssignmentsCollapsed((prev) => !prev),
          assignmentsPagination,
          assignmentsPage,
          setAssignmentsPage,
          assignmentsQuery.isFetching
        )}
      >
        {assignmentsCollapsed ? (
          <div className="empty-state">Секция свернута.</div>
        ) : assignmentsQuery.isLoading ? (
          <div className="empty-state">Загружаем историю назначений…</div>
        ) : (
          <DataGrid<AccountAssignmentRow>
            items={assignments}
            getRowKey={(item) => item.id}
            emptyMessage={<div className="empty-state">Назначений ещё нет.</div>}
            columns={[
              { key: "id", title: "ID", render: (item) => item.id },
              {
                key: "account",
                title: "Аккаунт",
                render: (item) => item.account_name || item.account_id
              },
              {
                key: "client",
                title: "Клиент",
                render: (item) => item.client_name || item.client_full_name || `ID ${item.client_id ?? "?"}`
              },
              {
                key: "slot",
                title: "Слот",
                render: (item) => formatSlot(item.slot_date, item.start_time)
              },
              {
                key: "stand",
                title: "Станок",
                render: (item) => item.stand_code || item.stand_title || "—"
              },
              {
                key: "applied_at",
                title: "Применено",
                render: (item) => formatDateTime(item.applied_at)
              },
              {
                key: "reservation",
                title: "Бронь",
                render: (item) => `#${item.reservation_id} (${item.reservation_status || "?"})`
              }
            ]}
          />
        )}
      </Panel>

      <Panel
        title="Уведомления автоназначений"
        subtitle="Когда и кому отправляли уведомления об автоназначении аккаунтов"
        headerExtra={buildHeaderExtra(
          notificationsCollapsed,
          () => setNotificationsCollapsed((prev) => !prev),
          assignmentNotificationsPagination,
          notificationsPage,
          setNotificationsPage,
          assignmentNotificationsQuery.isFetching
        )}
      >
        {notificationsCollapsed ? (
          <div className="empty-state">Секция свернута.</div>
        ) : assignmentNotificationsQuery.isLoading ? (
          <div className="empty-state">Загружаем уведомления…</div>
        ) : (
          <DataGrid<AssignmentNotificationRow>
            items={assignmentNotifications}
            getRowKey={(item) => item.id}
            emptyMessage={<div className="empty-state">Уведомлений ещё нет.</div>}
            columns={[
              { key: "id", title: "ID", render: (item) => item.id },
              {
                key: "account",
                title: "Аккаунт",
                render: (item) => item.account_name || item.account_id
              },
              {
                key: "status",
                title: "Статус",
                render: (item) => item.status
              },
              {
                key: "client",
                title: "Клиент",
                render: (item) => item.client_name || item.client_full_name || `ID ${item.client_id ?? "?"}`
              },
              {
                key: "slot",
                title: "Слот",
                render: (item) => formatSlot(item.slot_date, item.start_time)
              },
              {
                key: "stand",
                title: "Станок",
                render: (item) => item.stand_code || item.stand_title || "—"
              },
              {
                key: "notified_at",
                title: "Отправлено",
                render: (item) => formatDateTime(item.notified_at)
              },
              {
                key: "reservation",
                title: "Бронь",
                render: (item) => `#${item.reservation_id} (${item.reservation_status || "?"})`
              }
            ]}
          />
        )}
      </Panel>
    </div>
  );
}
