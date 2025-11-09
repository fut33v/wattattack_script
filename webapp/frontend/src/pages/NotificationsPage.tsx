import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { WorkoutNotification, WorkoutNotificationListResponse } from "../lib/types";

export default function NotificationsPage() {
  const [page, setPage] = useState(1);
  const queryClient = useQueryClient();

  const listQuery = useQuery<WorkoutNotificationListResponse>({
    queryKey: ["notifications", page],
    queryFn: () => {
      const params = new URLSearchParams({ page: String(page) });
      return apiFetch<WorkoutNotificationListResponse>(`/api/schedule/notifications?${params.toString()}`);
    },
    placeholderData: (previousData) => previousData
  });

  const data = listQuery.data;
  const pagination = data?.pagination;
  const items = data?.items ?? [];

  useEffect(() => {
    if (pagination && page > 1 && items.length === 0 && !listQuery.isFetching) {
      setPage((prev) => Math.max(prev - 1, 1));
    }
  }, [items.length, pagination, page, listQuery.isFetching]);

  const headerControls = (
    <div className="notifications-controls">
      <div className="notifications-header">
        <h3>Уведомления о тренировках</h3>
        <p>Список отправленных уведомлений клиентам о предстоящих тренировках</p>
      </div>
      {pagination && (
        <div className="pagination-controls">
          <button
            className="button"
            disabled={page <= 1 || listQuery.isFetching}
            onClick={() => setPage((prev) => Math.max(prev - 1, 1))}
            type="button"
          >
            ⟵ Назад
          </button>
          <div className="page-indicator">
            Страница {pagination.page} из {pagination.totalPages} (по {pagination.pageSize} на странице)
          </div>
          <button
            className="button"
            disabled={page >= pagination.totalPages || listQuery.isFetching}
            onClick={() => setPage((prev) => prev + 1)}
            type="button"
          >
            Вперед ⟶
          </button>
        </div>
      )}
    </div>
  );

  function formatDateTime(dateString: string | null | undefined): string {
    if (!dateString) return "—";
    try {
      return dayjs(dateString).format("DD.MM.YYYY HH:mm");
    } catch {
      return dateString;
    }
  }

  function formatNotificationType(type: string): string {
    if (type.startsWith("reminder_")) {
      const hours = type.replace("reminder_", "");
      return `Напоминание за ${hours}`;
    }
    return type;
  }

  return (
    <Panel
      title="Уведомления"
      subtitle="Список отправленных уведомлений о тренировках"
      headerExtra={headerControls}
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем уведомления…</div>
      ) : (
        <DataGrid<WorkoutNotification>
          items={items}
          getRowKey={(item) => item.id}
          emptyMessage={<div className="empty-state">Уведомления не найдены.</div>}
          columns={[
            {
              key: "id",
              title: "ID",
              render: (item) => item.id
            },
            {
              key: "client_name",
              title: "Клиент",
              render: (item) => item.client_name || `${item.client_first_name || ""} ${item.client_last_name || ""}`.trim() || "—"
            },
            {
              key: "slot_date",
              title: "Дата тренировки",
              render: (item) => formatDateTime(`${item.slot_date}T${item.start_time}`)
            },
            {
              key: "stand",
              title: "Станок",
              render: (item) => item.stand_code || item.stand_title || "—"
            },
            {
              key: "notification_type",
              title: "Тип уведомления",
              render: (item) => formatNotificationType(item.notification_type)
            },
            {
              key: "sent_at",
              title: "Отправлено",
              render: (item) => formatDateTime(item.sent_at)
            }
          ]}
        />
      )}
    </Panel>
  );
}
