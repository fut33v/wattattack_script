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
  const queryClient = useQueryClient();

  // Fetch accounts for the filter dropdown
  const accountsQuery = useQuery<AccountListResponse>({
    queryKey: ["accounts"],
    queryFn: () => apiFetch<AccountListResponse>("/api/activities/accounts"),
  });

  const listQuery = useQuery<ActivityIdListResponse>({
    queryKey: ["activities", page, accountId],
    queryFn: () => {
      const params = new URLSearchParams({ 
        page: String(page),
        ...(accountId && { account_id: accountId })
      });
      return apiFetch<ActivityIdListResponse>(`/api/activities?${params.toString()}`);
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

  return (
    <Panel
      title="Активности"
      subtitle="Список отслеживаемых активностей из WattAttack"
      headerExtra={headerControls}
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем активности…</div>
      ) : (
        <DataGrid<ActivityIdRecord>
          items={items}
          getRowKey={(item) => `${item.account_id}-${item.activity_id}`}
          emptyMessage={<div className="empty-state">Активности не найдены.</div>}
          columns={[
            {
              key: "id",
              title: "ID",
              render: (item) => item.id
            },
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
              key: "created_at",
              title: "Дата добавления",
              render: (item) => formatDateTime(item.created_at)
            },
            {
              key: "actions",
              title: "Действия",
              render: (item) => (
                <button
                  className="button button--danger"
                  onClick={() => handleDelete(item.account_id, item.activity_id)}
                >
                  Удалить
                </button>
              )
            }
          ]}
        />
      )}
    </Panel>
  );
}