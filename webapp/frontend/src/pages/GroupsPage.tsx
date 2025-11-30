import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { GroupListResponse } from "../lib/types";

export default function GroupsPage() {
  const queryClient = useQueryClient();
  const [newGroupName, setNewGroupName] = useState("САМОКРУТЧИКИ");

  const groupsQuery = useQuery<GroupListResponse>({
    queryKey: ["groups"],
    queryFn: () => apiFetch<GroupListResponse>("/api/groups")
  });

  const addMutation = useMutation({
    mutationFn: (group_name: string) =>
      apiFetch("/api/groups", {
        method: "POST",
        body: JSON.stringify({ group_name })
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["groups"] });
      setNewGroupName("САМОКРУТЧИКИ");
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (group_name: string) =>
      apiFetch(`/api/groups?group_name=${encodeURIComponent(group_name)}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["groups"] });
    }
  });

  const groups = groupsQuery.data?.items ?? [];

  return (
    <Panel title="Группы" subtitle="Предопределенные группы клиентов">
      <form
        className="form-grid"
        onSubmit={(event) => {
          event.preventDefault();
          const value = newGroupName.trim();
          if (!value) return;
          addMutation.mutate(value);
        }}
      >
        <label>
          Название группы
          <input
            type="text"
            value={newGroupName}
            onChange={(event) => setNewGroupName(event.target.value)}
            placeholder="Например, САМОКРУТЧИКИ"
            disabled={addMutation.isPending}
          />
        </label>
        <div className="form-actions">
          <button type="submit" className="button" disabled={addMutation.isPending}>
            {addMutation.isPending ? "Сохраняем…" : "Создать группу"}
          </button>
          {addMutation.isError && <span className="form-error">Не удалось создать группу.</span>}
        </div>
      </form>

      {groupsQuery.isLoading ? (
        <div className="empty-state">Загружаем группы…</div>
      ) : groupsQuery.isError ? (
        <div className="form-error">Не удалось загрузить группы.</div>
      ) : groups.length === 0 ? (
        <div className="empty-state">Групп пока нет.</div>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Группа</th>
              <th>Создана</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {groups.map((group) => (
              <tr key={group.group_name}>
                <td>{group.group_name}</td>
                <td>{group.created_at ? dayjs(group.created_at).format("DD.MM.YYYY HH:mm") : "—"}</td>
                <td className="text-right">
                  <button
                    type="button"
                    className="button danger"
                    onClick={() => deleteMutation.mutate(group.group_name)}
                    disabled={deleteMutation.isPending}
                  >
                    Удалить
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}
