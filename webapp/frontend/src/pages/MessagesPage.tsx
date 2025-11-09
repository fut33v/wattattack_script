import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid, { Column } from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { Pagination } from "../lib/types";

interface UserMessage {
  id: number;
  tg_user_id: number;
  tg_username: string | null;
  tg_full_name: string | null;
  message_text: string;
  created_at: string;
}

interface UserMessageListResponse {
  items: UserMessage[];
  pagination: Pagination;
}

export default function MessagesPage() {
  const [page, setPage] = useState(1);
  const queryClient = useQueryClient();

  const listQuery = useQuery<UserMessageListResponse>({
    queryKey: ["messages", page],
    queryFn: () => apiFetch<UserMessageListResponse>(`/api/messages?page=${page}&page_size=50`),
    placeholderData: (previousData) => previousData
  });

  const pagination = listQuery.data?.pagination;

  useEffect(() => {
    if (pagination && page > pagination.totalPages && pagination.totalPages > 0) {
      setPage(pagination.totalPages);
    }
  }, [pagination, page]);

  function formatDateTime(value: string | null | undefined): string {
    if (!value) return "—";
    try {
      return dayjs(value).format("DD.MM.YYYY HH:mm");
    } catch {
      return value;
    }
  }

  function formatUser(message: UserMessage): string {
    const nameParts = [];
    if (message.tg_full_name) {
      nameParts.push(message.tg_full_name);
    }
    if (message.tg_username) {
      nameParts.push(`@${message.tg_username}`);
    }
    if (nameParts.length === 0) {
      nameParts.push(`ID: ${message.tg_user_id}`);
    }
    return nameParts.join(" ");
  }

  const headerControls = (
    <div className="messages-controls">
      <div className="messages-header">
        <h3>Сообщения пользователей</h3>
        <p>Список всех сообщений, отправленных пользователями в krutilkavnbot</p>
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

  const items = listQuery.data?.items ?? [];

  return (
    <Panel
      title="Сообщения"
      subtitle="Список сообщений от пользователей krutilkavnbot"
      headerExtra={headerControls}
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем сообщения…</div>
      ) : (
        <DataGrid<UserMessage>
          items={items}
          getRowKey={(item) => item.id}
          emptyMessage={<div className="empty-state">Сообщения не найдены.</div>}
          columns={[
            {
              key: "id",
              title: "ID",
              render: (item) => item.id
            },
            {
              key: "user",
              title: "Пользователь",
              render: (item) => formatUser(item)
            },
            {
              key: "message_text",
              title: "Сообщение",
              render: (item) => item.message_text
            },
            {
              key: "created_at",
              title: "Отправлено",
              render: (item) => formatDateTime(item.created_at)
            }
          ]}
        />
      )}
    </Panel>
  );
}