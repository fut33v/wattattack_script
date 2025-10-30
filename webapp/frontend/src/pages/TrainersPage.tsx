import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { FormEvent } from "react";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { TrainerListResponse, TrainerRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

export default function TrainersPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();

  const listQuery = useQuery<TrainerListResponse>({
    queryKey: ["trainers"],
    queryFn: () => apiFetch<TrainerListResponse>("/api/trainers")
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: Record<string, unknown> }) =>
      apiFetch(`/api/trainers/${id}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["trainers"] });
    }
  });

  const isAdmin = session.isAdmin;

  function handleSubmit(event: FormEvent<HTMLFormElement>, trainer: TrainerRow) {
    event.preventDefault();
    if (!isAdmin) return;
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};
    ["axle_types", "cassette"].forEach((key) => {
      const value = formData.get(key);
      if (typeof value === "string") {
        payload[key] = value.trim() || null;
      }
    });
    updateMutation.mutate({ id: trainer.id, payload });
  }

  return (
    <Panel title="Тренажеры" subtitle="Список тренажеров и совместимых комплектующих">
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем тренажеры…</div>
      ) : (
        <DataGrid<TrainerRow>
          items={listQuery.data?.items ?? []}
          getRowKey={(item) => item.id}
          emptyMessage={<div className="empty-state">Тренажеры не найдены.</div>}
          actions={isAdmin ? (item) => renderActions(item) : undefined}
          columns={[
            {
              key: "id",
              title: "ID",
              className: "cell-id",
              render: (item) => <div className="id-chip">#{item.id}</div>
            },
            {
              key: "code",
              title: "Код",
              render: (item) => item.code ?? "—"
            },
            {
              key: "title",
              title: "Название",
              render: (item) => (
                <div>
                  <div className="primary">{item.title ?? "—"}</div>
                  <div className="muted">{item.display_name ?? ""}</div>
                </div>
              )
            },
            {
              key: "owner",
              title: "Владелец",
              render: (item) => item.owner ?? "—"
            },
            {
              key: "equipment",
              title: "Оборудование",
              render: (item) => (
                <div>
                  <div className="metric">
                    <span className="label">Оси</span>
                    {isAdmin ? (
                      <input type="text" name="axle_types" defaultValue={item.axle_types ?? ""} form={`trainer-${item.id}`} />
                    ) : (
                      <span>{item.axle_types ?? "—"}</span>
                    )}
                  </div>
                  <div className="metric">
                    <span className="label">Кассета</span>
                    {isAdmin ? (
                      <input type="text" name="cassette" defaultValue={item.cassette ?? ""} form={`trainer-${item.id}`} />
                    ) : (
                      <span>{item.cassette ?? "—"}</span>
                    )}
                  </div>
                  <div className="muted subtle">{item.notes ?? ""}</div>
                </div>
              )
            }
          ]}
        />
      )}
    </Panel>
  );

  function renderActions(item: TrainerRow) {
    return (
      <form id={`trainer-${item.id}`} className="row-form" onSubmit={(event) => handleSubmit(event, item)}>
        <button type="submit" className="button">
          {updateMutation.isPending ? "Сохраняю…" : "Сохранить"}
        </button>
      </form>
    );
  }
}
