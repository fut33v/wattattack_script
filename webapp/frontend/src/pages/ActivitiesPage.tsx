import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { ActivityIdRecord, ActivityIdListResponse, AccountListResponse } from "../lib/types";

export default function ActivitiesPage() {
  const [page, setPage] = useState(1);
  const [accountId, setAccountId] = useState<string>("");
  const [sortKey, setSortKey] = useState<"start_time" | "created_at" | "">("");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  const queryClient = useQueryClient();

  // Fetch accounts for the filter dropdown
  const accountsQuery = useQuery<AccountListResponse>({
    queryKey: ["accounts"],
    queryFn: () => apiFetch<AccountListResponse>("/api/activities/accounts"),
  });

  const listQuery = useQuery<ActivityIdListResponse>({
    queryKey: ["activities", page, accountId, sortKey, sortDir],
    queryFn: () => {
      const params = new URLSearchParams({ 
        page: String(page),
        ...(accountId && { account_id: accountId }),
        ...(sortKey && { sort: sortKey, dir: sortDir }),
      });
      return apiFetch<ActivityIdListResponse>(`/api/activities?${params.toString()}`);
    },
    placeholderData: (previousData) => previousData
  });

  const data = listQuery.data;
  const pagination = data?.pagination;
  const items = data?.items ?? [];
  const rowKey = (item: ActivityIdRecord) => `${item.account_id}-${item.activity_id}`;

  useEffect(() => {
    if (pagination && page > 1 && items.length === 0 && !listQuery.isFetching) {
      setPage((prev) => Math.max(prev - 1, 1));
    }
  }, [items.length, pagination, page, listQuery.isFetching]);

  useEffect(() => {
    // Сбрасываем выделение при смене страницы или списка
    setSelectedKeys(new Set());
  }, [page, accountId, items]);

  const handleDelete = async (account_id: string, activity_id: string) => {
    if (window.confirm(`Are you sure you want to delete activity ${activity_id} for account ${account_id}?`)) {
      try {
        await apiFetch(`/api/activities/${account_id}/${activity_id}`, { method: "DELETE" });
        // Refresh the list
        queryClient.invalidateQueries({ queryKey: ["activities"] });
      } catch (error) {
        console.error("Failed to delete activity:", error);
        alert("Failed to delete activity");
      }
    }
  };

  const handleAccountFilterChange = (value: string) => {
    setAccountId(value);
    setPage(1);
  };

  const toggleSort = (key: "start_time" | "created_at") => {
    setSortKey((prev) => {
      if (prev !== key) {
        setSortDir("desc");
        return key;
      }
      setSortDir((prevDir) => (prevDir === "desc" ? "asc" : "desc"));
      return key;
    });
  };

  const selectedItems = items.filter((item) => selectedKeys.has(rowKey(item)));
  const isAllSelected = items.length > 0 && items.every((item) => selectedKeys.has(rowKey(item)));

  const handleToggleRow = (key: string | number) => {
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(String(key))) {
        next.delete(String(key));
      } else {
        next.add(String(key));
      }
      return next;
    });
  };

  const handleToggleAll = (checked: boolean) => {
    if (checked) {
      setSelectedKeys(new Set(items.map((item) => rowKey(item))));
    } else {
      setSelectedKeys(new Set());
    }
  };

  const handleBulkDelete = async () => {
    if (!selectedItems.length) return;
    const message =
      selectedItems.length === 1
        ? `Удалить активность ${selectedItems[0].activity_id} для ${selectedItems[0].account_id}?`
        : `Удалить ${selectedItems.length} выбранных активностей?`;
    if (!window.confirm(message)) return;

    try {
      for (const item of selectedItems) {
        // eslint-disable-next-line no-await-in-loop
        await apiFetch(`/api/activities/${item.account_id}/${item.activity_id}`, { method: "DELETE" });
      }
      setSelectedKeys(new Set());
      queryClient.invalidateQueries({ queryKey: ["activities"] });
    } catch (error) {
      console.error("Bulk delete failed", error);
      alert("Не удалось удалить выбранные активности");
    }
  };

  const headerControls = (
    <div className="activities-controls">
      <div className="activities-header">
        <h3>Активности WattAttack</h3>
        <p>Список отслеживаемых активностей из WattAttack</p>
      </div>
      <div className="activities-filters">
        <select
          value={accountId}
          onChange={(e) => handleAccountFilterChange(e.target.value)}
          className="filter-select"
        >
          <option value="">Все аккаунты</option>
          {accountsQuery.data?.accounts.map((account) => (
            <option key={account} value={account}>
              {account}
            </option>
          ))}
        </select>
      </div>
      <div className="activities-actions">
        <button
          className="button button--danger"
          type="button"
          disabled={!selectedItems.length || listQuery.isFetching}
          onClick={handleBulkDelete}
        >
          Удалить выбранные ({selectedItems.length})
        </button>
      </div>
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

  const formatFlag = (value: boolean | null | undefined) => (value ? "✅" : "✖️");

  return (
    <Panel
      title="Активности"
      subtitle="Список отслеживаемых активностей из WattAttack"
      headerExtra={headerControls}
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем активности…</div>
      ) : (
        <>
          <DataGrid<ActivityIdRecord>
            items={items}
            getRowKey={rowKey}
            emptyMessage={<div className="empty-state">Активности не найдены.</div>}
            selection={{
              selectedKeys,
              onToggle: (key) => handleToggleRow(String(key)),
              onToggleAll: handleToggleAll,
              isAllSelected,
            }}
            sortState={{
              sortKey: sortKey || null,
              direction: sortDir,
              onSort: (key) => toggleSort(key as "start_time" | "created_at"),
            }}
            columns={[
              {
                key: "account_id",
                title: "Account ID",
                render: (item) => item.account_id
              },
              {
                key: "activity_id",
                title: "Activity ID",
                render: (item) => item.activity_id
              },
              {
                key: "start_time",
                title: "Дата активности",
                sortable: true,
                render: (item) => formatDateTime(item.start_time),
              },
              {
                key: "client_id",
                title: "Клиент ID",
                render: (item) => item.client_id ?? "—",
              },
              {
                key: "scheduled_name",
                title: "Имя по расписанию",
                render: (item) => item.scheduled_name || "—",
              },
              {
                key: "profile_name",
                title: "Имя в WattAttack",
                render: (item) => item.profile_name || "—",
              },
            {
              key: "sent_clientbot",
              title: "БОТ",
              render: (item) => formatFlag(item.sent_clientbot),
            },
              {
                key: "sent_strava",
                title: "Strava",
                render: (item) => formatFlag(item.sent_strava),
              },
              {
                key: "sent_intervals",
                title: "Intervals",
                render: (item) => formatFlag(item.sent_intervals),
              },
              {
                key: "created_at",
                title: "Дата добавления",
                sortable: true,
                render: (item) => formatDateTime(item.created_at)
              },
            {
              key: "actions",
              title: "Действия",
              render: (item) => (
                <button
                  className="icon-button"
                  title="Удалить"
                  onClick={() => handleDelete(item.account_id, item.activity_id)}
                >
                  🗑️
                </button>
              )
            }
          ]}
          />
          {pagination && (
            <div className="pagination-controls pagination-controls--bottom">
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
        </>
      )}
    </Panel>
  );
}
