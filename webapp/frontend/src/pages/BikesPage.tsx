import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { FormEvent } from "react";

import Panel from "../components/Panel";
import DataGrid from "../components/DataGrid";
import { apiFetch } from "../lib/api";
import type { BikeListResponse, BikeRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

export default function BikesPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();

  const listQuery = useQuery<BikeListResponse>({
    queryKey: ["bikes"],
    queryFn: () => apiFetch<BikeListResponse>("/api/bikes")
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: Record<string, unknown> }) =>
      apiFetch(`/api/bikes/${id}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["bikes"] });
    }
  });

  const isAdmin = session.isAdmin;

  function handleSubmit(event: FormEvent<HTMLFormElement>, bike: BikeRow) {
    event.preventDefault();
    if (!isAdmin) return;
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};
    ["height_min_cm", "height_max_cm"].forEach((key) => {
      const value = formData.get(key);
      if (value === null || value === "") {
        payload[key] = null;
      } else {
        const numberValue = Number(value);
        payload[key] = Number.isNaN(numberValue) ? null : numberValue;
      }
    });
    updateMutation.mutate({ id: bike.id, payload });
  }

  return (
    <Panel title="Велосипеды" subtitle="Инвентарь для подбора в студии Крутилка">
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем велосипеды…</div>
      ) : (
        <DataGrid<BikeRow>
          items={listQuery.data?.items ?? []}
          getRowKey={(item) => item.id}
          emptyMessage={<div className="empty-state">Велосипеды не найдены.</div>}
          actions={isAdmin ? (item) => renderActions(item) : undefined}
          columns={[
            {
              key: "id",
              title: "ID",
              className: "cell-id",
              render: (item) => <div className="id-chip">#{item.id}</div>
            },
            {
              key: "title",
              title: "Название",
              render: (item) => (
                <div>
                  <div className="primary">{item.title}</div>
                  <div className="muted">Владелец: {item.owner ?? "—"}</div>
                </div>
              )
            },
            {
              key: "size",
              title: "Размер",
              render: (item) => (
                <div className="metric">
                  <span className="label">Маркировка</span>
                  <span>{item.size_label ?? "—"}</span>
                  <span className="label">Рама</span>
                  <span>{item.frame_size_cm ?? "—"}</span>
                </div>
              )
            },
            {
              key: "height",
              title: "Рост",
              render: (item) => (
                <>
                  <div className="metric">
                    <span className="label">Мин</span>
                    {isAdmin ? (
                      <input type="number" name="height_min_cm" defaultValue={item.height_min_cm ?? ""} form={`bike-${item.id}`} />
                    ) : (
                      <span>{item.height_min_cm ?? "—"}</span>
                    )}
                  </div>
                  <div className="metric">
                    <span className="label">Макс</span>
                    {isAdmin ? (
                      <input type="number" name="height_max_cm" defaultValue={item.height_max_cm ?? ""} form={`bike-${item.id}`} />
                    ) : (
                      <span>{item.height_max_cm ?? "—"}</span>
                    )}
                  </div>
                </>
              )
            },
            {
              key: "setup",
              title: "Комплектация",
              render: (item) => (
                <div>
                  <div className="metric">
                    <span className="label">Передачи</span>
                    <span>{item.gears ?? "—"}</span>
                  </div>
                  <div className="metric">
                    <span className="label">Ось</span>
                    <span>{item.axle_type ?? "—"}</span>
                  </div>
                  <div className="metric">
                    <span className="label">Кассета</span>
                    <span>{item.cassette ?? "—"}</span>
                  </div>
                </div>
              )
            }
          ]}
        />
      )}
    </Panel>
  );

  function renderActions(item: BikeRow) {
    return (
      <form id={`bike-${item.id}`} className="row-form" onSubmit={(event) => handleSubmit(event, item)}>
        <button type="submit" className="button">
          {updateMutation.isPending ? "Сохраняю…" : "Сохранить"}
        </button>
      </form>
    );
  }
}
