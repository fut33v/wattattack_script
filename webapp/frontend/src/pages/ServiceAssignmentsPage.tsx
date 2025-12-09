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
  AssignmentNotificationRow
} from "../lib/types";

function formatDateTime(value: string | null | undefined) {
  if (!value) return "—";
  const parsed = dayjs(value);
  return parsed.isValid() ? parsed.format("DD.MM.YYYY HH:mm") : value;
}

function formatSlot(slotDate: string | null | undefined, startTime: string | null | undefined) {
  if (!slotDate) return "—";
  const ts = startTime ? `${slotDate}T${startTime}` : slotDate;
  return formatDateTime(ts);
}

export default function ServiceAssignmentsPage() {
  const [notificationsPage, setNotificationsPage] = useState(1);
  const [assignmentsPage, setAssignmentsPage] = useState(1);

  const notificationsQuery = usePaginatedQuery<AssignmentNotificationListResponse>(
    ["assignment-notifications"],
    notificationsPage,
    () => {
      const params = new URLSearchParams({ page: String(notificationsPage) });
      return apiFetch<AssignmentNotificationListResponse>(`/api/schedule/assignment-notifications?${params.toString()}`);
    },
    setNotificationsPage
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

  function renderPagination(
    page: number,
    totalPages: number,
    setPage: (value: number) => void,
    isFetching: boolean
  ) {
    return (
      <div className="pagination-controls">
        <button className="button" disabled={page <= 1 || isFetching} onClick={() => setPage(Math.max(page - 1, 1))}>
          ⟵ Назад
        </button>
        <div className="page-indicator">Страница {page} из {totalPages}</div>
        <button className="button" disabled={page >= totalPages || isFetching} onClick={() => setPage(page + 1)}>
          Вперед ⟶
        </button>
      </div>
    );
  }

  const notificationsPagination = notificationsQuery.data?.pagination;
  const assignmentsPagination = assignmentsQuery.data?.pagination;

  return (
    <div className="stack gap-lg">
      <Panel
        title="Автоназначения аккаунтов"
        subtitle="Журнал фактических применений клиентских профилей"
        headerExtra={
          assignmentsPagination &&
          renderPagination(assignmentsPage, assignmentsPagination.totalPages, setAssignmentsPage, assignmentsQuery.isFetching)
        }
      >
        {assignmentsQuery.isLoading ? (
          <div className="empty-state">Загружаем историю назначений…</div>
        ) : (
          <DataGrid<AccountAssignmentRow>
            items={assignmentsQuery.data?.items ?? []}
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
        headerExtra={
          notificationsPagination &&
          renderPagination(
            notificationsPage,
            notificationsPagination.totalPages,
            setNotificationsPage,
            notificationsQuery.isFetching
          )
        }
      >
        {notificationsQuery.isLoading ? (
          <div className="empty-state">Загружаем уведомления…</div>
        ) : (
          <DataGrid<AssignmentNotificationRow>
            items={notificationsQuery.data?.items ?? []}
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
