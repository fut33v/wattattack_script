import { FormEvent, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch, ApiError } from "../lib/api";
import type { ClientListResponse, ClientRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

const GENDER_LABELS: Record<string, string> = {
  male: "Мужской",
  female: "Женский"
};

function formatGender(value: string | null | undefined): string {
  if (!value) return "—";
  const key = value.toLowerCase();
  return GENDER_LABELS[key] ?? value;
}

export default function ClientsPage() {
  const { session } = useAppContext();
  const [page, setPage] = useState(1);
  const [searchInput, setSearchInput] = useState("");
  const [searchTerm, setSearchTerm] = useState("");
  const [sortField, setSortField] = useState<string>("submitted_at");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const queryClient = useQueryClient();

  const listQuery = useQuery<ClientListResponse>({
    queryKey: ["clients", page, searchTerm, sortField, sortDirection],
    queryFn: () => {
      const params = new URLSearchParams({ page: String(page) });
      if (searchTerm.trim() !== "") {
        params.set("search", searchTerm.trim());
      }
      if (sortField) {
        params.set("sort", sortField);
      }
      if (sortDirection) {
        params.set("direction", sortDirection);
      }
      return apiFetch<ClientListResponse>(`/api/clients?${params.toString()}`);
    },
    placeholderData: (previousData) => previousData
  });

  const data = listQuery.data;
  const pagination = data?.pagination;
  const items = data?.items ?? [];
  const isSearching = searchTerm.trim() !== "";

  useEffect(() => {
    if (pagination && page > 1 && items.length === 0 && !listQuery.isFetching) {
      setPage((prev) => Math.max(prev - 1, 1));
    }
  }, [items.length, pagination, page, listQuery.isFetching]);

  function handleSearchSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalized = searchInput.trim();
    setSearchTerm(normalized);
    setPage(1);
  }

  function handleClearSearch() {
    setSearchInput("");
    if (searchTerm !== "") {
      setSearchTerm("");
      setPage(1);
    }
  }

  function handleSortChange(event: React.ChangeEvent<HTMLSelectElement>) {
    setSortField(event.target.value);
    setPage(1);
  }

  function handleDirectionChange(event: React.ChangeEvent<HTMLSelectElement>) {
    const value = event.target.value === "desc" ? "desc" : "asc";
    setSortDirection(value);
    setPage(1);
  }

  const deleteMutation = useMutation({
    mutationFn: (clientId: number) =>
      apiFetch(`/api/clients/${clientId}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["clients"], exact: false });
    }
  });

  const headerControls = (
    <div className="clients-controls">
      {session.isAdmin && (
        <div className="clients-actions">
          <Link className="button inventory-create-button" to="/clients/new">
            Добавить клиента
          </Link>
        </div>
      )}
      <form className="clients-search" onSubmit={handleSearchSubmit}>
        <input
          type="search"
          placeholder="Поиск по имени или фамилии…"
          value={searchInput}
          onChange={(event) => setSearchInput(event.target.value)}
          aria-label="Поиск клиентов"
        />
        <button type="submit" className="button" disabled={listQuery.isFetching}>
          Найти
        </button>
        <button
          type="button"
          className="button secondary"
          onClick={handleClearSearch}
          disabled={!isSearching && searchInput.trim() === ""}
        >
          Сбросить
        </button>
      </form>
      <div className="clients-sort">
        <label>
          Сортировка
          <select value={sortField} onChange={handleSortChange}>
            <option value="last_name">Фамилия</option>
            <option value="first_name">Имя</option>
            <option value="submitted_at">Анкета</option>
            <option value="height">Рост</option>
            <option value="weight">Вес</option>
            <option value="ftp">FTP</option>
            <option value="id">ID</option>
          </select>
        </label>
        <label>
          Порядок
          <select value={sortDirection} onChange={handleDirectionChange}>
            <option value="asc">По возрастанию</option>
            <option value="desc">По убыванию</option>
          </select>
        </label>
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
    <Panel
      title="Клиенты"
      subtitle="Краткий список клиентов. Для изменения данных откройте карточку клиента."
      headerExtra={headerControls}
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем клиентов…</div>
      ) : (
        <DataGrid<ClientRow>
          items={items}
          getRowKey={(item) => item.id}
          emptyMessage={<div className="empty-state">Клиенты не найдены.</div>}
          actions={
            session.isAdmin
              ? (item) => (
                  <div className="row-actions">
                    <Link className="button" to={`/clients/${item.id}`}>
                      Редактировать
                    </Link>
                    <button
                      type="button"
                      className="button danger"
                      disabled={deleteMutation.isPending}
                      onClick={() => {
                        if (!window.confirm(`Удалить клиента #${item.id}?`)) return;
                        deleteMutation.mutate(item.id, {
                          onError: (error) => {
                            const message = error instanceof ApiError ? error.message : "Не удалось удалить клиента.";
                            window.alert(message);
                          }
                        });
                      }}
                    >
                      Удалить
                    </button>
                  </div>
                )
              : undefined
          }
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
              render: (item) => formatGender(item.gender)
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
