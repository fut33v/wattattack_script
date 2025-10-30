import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { ClientListResponse, ClientRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

export default function ClientsPage() {
  const { session } = useAppContext();
  const [page, setPage] = useState(1);

  const listQuery = useQuery<ClientListResponse>({
    queryKey: ["clients", page],
    queryFn: () => apiFetch<ClientListResponse>(`/api/clients?page=${page}`),
    placeholderData: (previousData) => previousData
  });

  const data = listQuery.data;
  const pagination = data?.pagination;
  const items = data?.items ?? [];

  return (
    <Panel
      title="Клиенты"
      subtitle="Краткий список клиентов. Для изменения данных откройте карточку клиента."
      headerExtra={
        pagination && (
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
        )
      }
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем клиентов…</div>
      ) : (
        <DataGrid<ClientRow>
          items={items}
          getRowKey={(item) => item.id}
          emptyMessage={<div className="empty-state">Клиенты не найдены.</div>}
          actions={session.isAdmin ? (item) => <Link className="button" to={`/clients/${item.id}`}>Редактировать</Link> : undefined}
          tableClassName="compact-table"
          columns={[
            {
              key: "id",
              title: "ID",
              className: "cell-id",
              render: (item) => <div className="id-chip">#{item.id}</div>
            },
            {
              key: "full_name",
              title: "Имя",
              render: (item) =>
                item.full_name || [item.first_name, item.last_name].filter(Boolean).join(" ") || "—"
            },
            {
              key: "gender",
              title: "Пол",
              render: (item) => item.gender ?? "—"
            },
            {
              key: "height",
              title: "Рост",
              render: (item) => (item.height ? `${item.height} см` : "—")
            },
            {
              key: "weight",
              title: "Вес",
              render: (item) => (item.weight ? `${item.weight} кг` : "—")
            },
            {
              key: "ftp",
              title: "FTP",
              render: (item) => item.ftp ?? "—"
            },
            {
              key: "pedals",
              title: "Педали",
              render: (item) => item.pedals ?? "—"
            },
            {
              key: "submitted_at",
              title: "Анкета",
              render: (item) =>
                item.submitted_at ? dayjs(item.submitted_at).format("DD.MM.YY HH:mm") : "—"
            }
          ]}
        />
      )}
    </Panel>
  );
}
