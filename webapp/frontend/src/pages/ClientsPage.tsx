import { FormEvent, useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import classNames from "classnames";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import ClientActionsMenu from "../components/ClientActionsMenu";
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
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const [searchInput, setSearchInput] = useState("");
  const [searchTerm, setSearchTerm] = useState("");
  const [sortField, setSortField] = useState<string>("submitted_at");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [genderFilter, setGenderFilter] = useState<"all" | "male" | "female" | "unknown">("all");
  const [pedalsFilter, setPedalsFilter] = useState<"all" | "топталки" | "контакты" | "принесу свои">("all");
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
  const filteredItems = useMemo(() => {
    return items.filter((item) => {
      const normalizedGender = (item.gender ?? "").toLowerCase();
      const normalizedPedals = (item.pedals ?? "").toLowerCase();
      const genderMatches =
        genderFilter === "all" ||
        (genderFilter === "male" && normalizedGender === "male") ||
        (genderFilter === "female" && normalizedGender === "female") ||
        (genderFilter === "unknown" && !normalizedGender);
      const pedalsMatches =
        pedalsFilter === "all" ||
        normalizedPedals === pedalsFilter ||
        normalizedPedals.includes(pedalsFilter);
      return genderMatches && pedalsMatches;
    });
  }, [items, genderFilter, pedalsFilter]);

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

  function handleGenderFilterChange(next: typeof genderFilter) {
    setGenderFilter(next);
  }

  function handlePedalsFilterChange(next: typeof pedalsFilter) {
    setPedalsFilter(next);
  }

  function handleEdit(client: ClientRow) {
    navigate(`/clients/${client.id}`);
  }

  function handleDelete(client: ClientRow) {
    if (!window.confirm(`Удалить клиента #${client.id}?`)) return;
    deleteMutation.mutate(client.id, {
      onError: (error) => {
        const message = error instanceof ApiError ? error.message : "Не удалось удалить клиента.";
        window.alert(message);
      }
    });
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

  const headerControls = session.isAdmin ? (
    <Link className="button primary add-client-button" to="/clients/new">
      Добавить клиента
    </Link>
  ) : null;

  return (
    <Panel
      title="Клиенты"
      subtitle="Краткий список клиентов. Для изменения данных откройте карточку клиента."
      headerExtra={headerControls}
    >
      {/* Компактный фильтр-бар над таблицей */}
      <form className="clients-toolbar" onSubmit={handleSearchSubmit}>
        <div className="clients-filter-row">
          <input
            type="search"
            className="clients-search-input"
            placeholder="Поиск по имени или фамилии…"
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            aria-label="Поиск клиентов"
          />
          <div className="select-group">
            <label>
              Сортировка
              <select value={sortField} onChange={handleSortChange}>
                <option value="submitted_at">Анкета</option>
                <option value="last_name">Фамилия</option>
                <option value="first_name">Имя</option>
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
          <div className="clients-toolbar-actions">
            <button type="submit" className="button primary" disabled={listQuery.isFetching}>
              Найти
            </button>
            <button
              type="button"
              className="button ghost"
              onClick={handleClearSearch}
              disabled={!isSearching && searchInput.trim() === ""}
            >
              Сбросить
            </button>
          </div>
        </div>
        <div className="clients-filter-row clients-filter-row--chips">
          <div className="chip-group">
            <span className="chip-label">Пол</span>
            {[
              { value: "all", label: "Все" },
              { value: "male", label: "Мужской" },
              { value: "female", label: "Женский" },
              { value: "unknown", label: "Не указан" }
            ].map((option) => (
              <button
                key={option.value}
                type="button"
                className={classNames("filter-chip", genderFilter === option.value && "active")}
                onClick={() => handleGenderFilterChange(option.value as typeof genderFilter)}
              >
                {option.label}
              </button>
            ))}
          </div>
          <div className="chip-group">
            <span className="chip-label">Педали</span>
            {[
              { value: "all", label: "Все" },
              { value: "топталки", label: "Топталки" },
              { value: "контакты", label: "Контакты" },
              { value: "принесу свои", label: "Принесу свои" }
            ].map((option) => (
              <button
                key={option.value}
                type="button"
                className={classNames("filter-chip", pedalsFilter === option.value && "active")}
                onClick={() => handlePedalsFilterChange(option.value as typeof pedalsFilter)}
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>
      </form>

      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем клиентов…</div>
      ) : (
        <>
          {/* Таблица с липким заголовком и кликабельными строками */}
          <DataGrid<ClientRow>
            items={filteredItems}
            getRowKey={(item) => item.id}
            emptyMessage={<div className="empty-state">Клиенты не найдены.</div>}
            actions={
              session.isAdmin
                ? (item) => (
                    <ClientActionsMenu
                      onEdit={() => handleEdit(item)}
                      onDelete={() => handleDelete(item)}
                      disabled={deleteMutation.isPending}
                    />
                  )
                : undefined
            }
            stickyHeader
            onRowClick={handleEdit}
            tableClassName="compact-table clients-table"
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
                className: "cell-name",
                render: (item) => {
                  const value = item.full_name || [item.first_name, item.last_name].filter(Boolean).join(" ");
                  return (
                    <div className="text-ellipsis" title={value || undefined}>
                      {value || "—"}
                    </div>
                  );
                }
              },
              {
                key: "gender",
                title: "Пол",
                render: (item) => <span className="pill pill-muted">{formatGender(item.gender)}</span>
              },
              {
                key: "height",
                title: "Рост",
                render: (item) =>
                  item.height ? <span className="pill">{item.height} см</span> : <span className="muted">—</span>
              },
              {
                key: "weight",
                title: "Вес",
                render: (item) =>
                  item.weight ? <span className="pill">{item.weight} кг</span> : <span className="muted">—</span>
              },
              {
                key: "ftp",
                title: "FTP",
                render: (item) =>
                  item.ftp !== null && item.ftp !== undefined ? (
                    <span className="pill pill-accent">{item.ftp}</span>
                  ) : (
                    <span className="muted">—</span>
                  )
              },
              {
                key: "pedals",
                title: "Педали",
                className: "cell-pedals",
                render: (item) => (
                  <div className="text-ellipsis" title={item.pedals ?? undefined}>
                    {item.pedals ?? <span className="muted">—</span>}
                  </div>
                )
              },
              {
                key: "submitted_at",
                title: "Анкета",
                className: "cell-date",
                render: (item) =>
                  item.submitted_at ? (
                    <span className="muted">{dayjs(item.submitted_at).format("DD.MM.YY HH:mm")}</span>
                  ) : (
                    <span className="muted">—</span>
                  )
              }
            ]}
          />
          {pagination && (
            <div className="pagination-controls pagination-controls--bottom">
              <button
                className="button ghost icon-only"
                disabled={page <= 1 || listQuery.isFetching}
                onClick={() => setPage((prev) => Math.max(prev - 1, 1))}
                type="button"
                aria-label="Предыдущая страница"
              >
                ⟵
              </button>
              <div className="page-indicator">Стр. {pagination.page} / {pagination.totalPages}</div>
              <button
                className="button ghost icon-only"
                disabled={page >= pagination.totalPages || listQuery.isFetching}
                onClick={() => setPage((prev) => prev + 1)}
                type="button"
                aria-label="Следующая страница"
              >
                ⟶
              </button>
            </div>
          )}
        </>
      )}
    </Panel>
  );
}
