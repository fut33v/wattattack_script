import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { PulseNotification, PulseNotificationListResponse } from "../lib/types";

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

export default function PulsePage() {
  const [page, setPage] = useState(1);

  const listQuery = useQuery<PulseNotificationListResponse>({
    queryKey: ["pulse-notifications", page],
    queryFn: () => {
      const params = new URLSearchParams({ page: String(page) });
      return apiFetch<PulseNotificationListResponse>(`/api/pulse/notifications?${params.toString()}`);
    },
    placeholderData: (prev) => prev
  });

  const data = listQuery.data;
  const pagination = data?.pagination;
  const items = data?.items ?? [];

  useEffect(() => {
    if (pagination && page > 1 && items.length === 0 && !listQuery.isFetching) {
      setPage((prev) => Math.max(prev - 1, 1));
    }
  }, [items.length, pagination, page, listQuery.isFetching]);

  const header = (
    <div className="notifications-controls">
      <div className="notifications-header">
        <h3>Pulse</h3>
        <p>Лента системных уведомлений: бронирования и новые анкеты</p>
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

  return (
    <Panel title="Pulse" subtitle="Журнал уведомлений о бронированиях и клиентах" headerExtra={header}>
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем Pulse…</div>
      ) : (
        <DataGrid<PulseNotification>
          items={items}
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
  );
}
