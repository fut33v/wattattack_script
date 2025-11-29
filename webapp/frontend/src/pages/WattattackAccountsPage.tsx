import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { ApiError, apiFetch } from "../lib/api";
import type { WattAttackAccount, WattAttackAccountListResponse, WattAttackAccountResponse } from "../lib/types";

export default function WattattackAccountsPage() {
  const queryClient = useQueryClient();
  const listQuery = useQuery<WattAttackAccountListResponse>({
    queryKey: ["wattattack-accounts"],
    queryFn: () => apiFetch("/api/wattattack/accounts"),
  });

  const [formState, setFormState] = useState<Partial<WattAttackAccount>>({});
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const upsertMutation = useMutation({
    mutationFn: () =>
      apiFetch<WattAttackAccountResponse>("/api/wattattack/accounts", {
        method: "POST",
        body: formState,
      }),
    onSuccess: () => {
      setSuccess("Сохранено");
      setError(null);
      setFormState({});
      queryClient.invalidateQueries({ queryKey: ["wattattack-accounts"] });
    },
    onError: (err: unknown) => {
      const message = err instanceof ApiError ? err.message : "Не удалось сохранить аккаунт";
      setError(message);
      setSuccess(null);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (accountId: string) =>
      apiFetch(`/api/wattattack/accounts/${encodeURIComponent(accountId)}`, { method: "DELETE" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["wattattack-accounts"] });
    },
  });

  const items = listQuery.data?.items ?? [];

  const formValid = useMemo(() => Boolean(formState.id && formState.email && formState.password), [formState]);

  const handleEdit = (acc: WattAttackAccount) => {
    setFormState({
      id: acc.id,
      name: acc.name || "",
      email: acc.email,
      password: acc.password,
      base_url: acc.base_url || "",
      stand_ids: acc.stand_ids || [],
    });
    setError(null);
    setSuccess(null);
  };

  const handleDelete = (acc: WattAttackAccount) => {
    if (window.confirm(`Удалить аккаунт ${acc.id}?`)) {
      deleteMutation.mutate(acc.id);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!formValid) {
      setError("Заполните id, email и пароль");
      return;
    }
    upsertMutation.mutate();
  };

  return (
    <Panel title="Аккаунты WattAttack" subtitle="Управление учетками для импорта активностей">
      {listQuery.isLoading && <div className="empty-state">Загружаем аккаунты…</div>}
      {listQuery.isError && <div className="form-error">Не удалось загрузить список.</div>}

      <div className="card">
        <h3>Добавить / обновить</h3>
        <form className="form-grid" onSubmit={handleSubmit}>
          <label>
            ID
            <input
              type="text"
              value={formState.id ?? ""}
              onChange={(e) => setFormState((prev) => ({ ...prev, id: e.target.value }))}
              placeholder="krutilka_001"
              required
            />
          </label>
          <label>
            Имя
            <input
              type="text"
              value={formState.name ?? ""}
              onChange={(e) => setFormState((prev) => ({ ...prev, name: e.target.value }))}
              placeholder="Крутилка 001"
            />
          </label>
          <label>
            Email
            <input
              type="email"
              value={formState.email ?? ""}
              onChange={(e) => setFormState((prev) => ({ ...prev, email: e.target.value }))}
              required
            />
          </label>
          <label>
            Пароль
            <input
              type="text"
              value={formState.password ?? ""}
              onChange={(e) => setFormState((prev) => ({ ...prev, password: e.target.value }))}
              required
            />
          </label>
          <label>
            Base URL
            <input
              type="text"
              value={formState.base_url ?? ""}
              onChange={(e) => setFormState((prev) => ({ ...prev, base_url: e.target.value }))}
              placeholder="https://wattattack.com"
            />
          </label>
          <label>
            Stand IDs (через запятую)
            <input
              type="text"
              value={(formState.stand_ids || []).join(",")}
              onChange={(e) => {
                const raw = e.target.value;
                const parts = raw.split(",").map((v) => v.trim()).filter(Boolean);
                const nums = parts
                  .map((p) => {
                    const n = Number(p);
                    return Number.isFinite(n) ? n : null;
                  })
                  .filter((n) => n !== null) as number[];
                setFormState((prev) => ({ ...prev, stand_ids: nums }));
              }}
              placeholder="1,2,3"
            />
          </label>
          <div className="form-actions">
            <button type="submit" className="button primary" disabled={upsertMutation.isPending || !formValid}>
              {upsertMutation.isPending ? "Сохраняем…" : "Сохранить"}
            </button>
            {error && <span className="form-error">{error}</span>}
            {success && <span className="form-success">{success}</span>}
          </div>
        </form>
      </div>

      <DataGrid<WattAttackAccount>
        items={items}
        getRowKey={(item) => item.id}
        emptyMessage={<div className="empty-state">Аккаунты не найдены.</div>}
        columns={[
          { key: "id", title: "ID", render: (item) => item.id },
          { key: "name", title: "Имя", render: (item) => item.name || "—" },
          { key: "email", title: "Email", render: (item) => item.email },
          { key: "base_url", title: "Base URL", render: (item) => item.base_url || "—" },
          { key: "stand_ids", title: "Станки", render: (item) => (item.stand_ids || []).join(", ") || "—" },
          {
            key: "actions",
            title: "Действия",
            render: (item) => (
              <div className="actions">
                <button type="button" className="button" onClick={() => handleEdit(item)}>
                  Редактировать
                </button>
                <button type="button" className="button button--danger" onClick={() => handleDelete(item)}>
                  Удалить
                </button>
              </div>
            ),
          },
        ]}
      />
    </Panel>
  );
}
