import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import classNames from "classnames";
import { useEffect, useMemo, useState, type FormEvent } from "react";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { BikeListResponse, BikeRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

type BikeResponse = { item: BikeRow };

export default function BikesPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();
  const [selectedBikeId, setSelectedBikeId] = useState<number | "new" | null>(null);

  const listQuery = useQuery<BikeListResponse>({
    queryKey: ["bikes"],
    queryFn: () => apiFetch<BikeListResponse>("/api/bikes")
  });

  const bikes = listQuery.data?.items ?? [];
  const isAdmin = session.isAdmin;

  useEffect(() => {
    if (selectedBikeId === "new") return;
    if (bikes.length === 0) {
      if (selectedBikeId !== null) {
        setSelectedBikeId(null);
      }
      return;
    }
    const exists = typeof selectedBikeId === "number" && bikes.some((bike) => bike.id === selectedBikeId);
    if (!exists) {
      setSelectedBikeId(bikes[0]?.id ?? null);
    }
  }, [bikes, selectedBikeId]);

  const selectedBike = useMemo<BikeRow | null>(
    () => (typeof selectedBikeId === "number" ? bikes.find((bike) => bike.id === selectedBikeId) ?? null : null),
    [selectedBikeId, bikes]
  );

  const updateMutation = useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: Record<string, unknown> }) =>
      apiFetch<BikeResponse>(`/api/bikes/${id}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["bikes"] });
    }
  });

  const createMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch<BikeResponse>("/api/bikes", {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ["bikes"] });
      if (data?.item?.id) {
        setSelectedBikeId(data.item.id);
      } else {
        setSelectedBikeId(null);
      }
    }
  });

  const isCreating = selectedBikeId === "new";

  function handleUpdateSubmit(event: FormEvent<HTMLFormElement>, bike: BikeRow) {
    event.preventDefault();
    if (!isAdmin) return;
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};
    ["height_min_cm", "height_max_cm"].forEach((key) => {
      const value = formData.get(key);
      if (typeof value === "string") {
        const trimmed = value.trim();
        if (trimmed === "") {
          payload[key] = null;
        } else {
          const normalized = trimmed.replace(",", ".");
          const numberValue = Number(normalized);
          if (Number.isNaN(numberValue)) {
            return;
          }
          payload[key] = numberValue;
        }
      }
    });
    if (Object.keys(payload).length === 0) {
      return;
    }
    updateMutation.mutate({ id: bike.id, payload });
  }

  function handleCreateSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isAdmin) return;
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};

    const title = formData.get("title");
    if (typeof title !== "string" || title.trim() === "") {
      return;
    }
    payload.title = title.trim();

    ["owner", "size_label", "frame_size_cm", "gears", "axle_type", "cassette"].forEach((key) => {
      const value = formData.get(key);
      if (typeof value === "string") {
        const trimmed = value.trim();
        if (trimmed !== "") {
          payload[key] = trimmed;
        }
      }
    });

    ["height_min_cm", "height_max_cm"].forEach((key) => {
      const value = formData.get(key);
      if (typeof value === "string") {
        const trimmed = value.trim();
        if (trimmed !== "") {
          const normalized = trimmed.replace(",", ".");
          const numberValue = Number(normalized);
          if (!Number.isNaN(numberValue)) {
            payload[key] = numberValue;
          }
        }
      }
    });

    createMutation.mutate(payload, {
      onSuccess: () => {
        (event.target as HTMLFormElement).reset();
      }
    });
  }

  const listContent =
    bikes.length === 0 ? (
      <div className="empty-state">Велосипеды не найдены.</div>
    ) : (
      <div className="bikes-list-items">
        {bikes.map((bike) => (
          <button
            key={bike.id}
            type="button"
            className={classNames("bike-list-item", { active: bike.id === selectedBikeId })}
            onClick={() => setSelectedBikeId(bike.id)}
          >
            <span className="bike-list-title">{bike.title}</span>
            <span className="bike-list-meta">{bike.owner ? `Владелец: ${bike.owner}` : "Владелец не указан"}</span>
            <span className="bike-list-meta">{formatBikeSize(bike)}</span>
            <span className="bike-list-height">{formatHeightRange(bike)}</span>
          </button>
        ))}
      </div>
    );

  return (
    <Panel
      title="Велосипеды"
      subtitle="Выберите велосипед слева и настройте ростовые ограничения справа"
      headerExtra={
        isAdmin ? (
          <button
            type="button"
            className={classNames("button", "inventory-create-button", { active: isCreating })}
            onClick={() => setSelectedBikeId(isCreating ? bikes[0]?.id ?? null : "new")}
            disabled={createMutation.isPending}
          >
            {isCreating ? "Отменить" : "Добавить велосипед"}
          </button>
        ) : undefined
      }
    >
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем велосипеды…</div>
      ) : listQuery.isError ? (
        <div className="empty-state">Не удалось загрузить список велосипедов.</div>
      ) : (
        <div className="bikes-layout">
          <aside className="bikes-list">
            {isAdmin && bikes.length > 0 && (
              <button
                type="button"
                className={classNames("bike-list-item", "create-entry", { active: isCreating })}
                onClick={() => setSelectedBikeId("new")}
                disabled={createMutation.isPending}
              >
                <span className="bike-list-title">{isCreating ? "Заполнение новой карточки" : "➕ Новый велосипед"}</span>
                <span className="bike-list-meta">Создать и добавить в инвентарь</span>
              </button>
            )}
            {listContent}
          </aside>
          <section className="bike-details">
            {isCreating ? (
              isAdmin ? (
                <form className="bike-form" onSubmit={handleCreateSubmit}>
                  <div className="form-grid">
                    <label>
                      Название *
                      <input type="text" name="title" placeholder="Canyon Ultimate CF SL" required />
                    </label>
                    <label>
                      Владелец
                      <input type="text" name="owner" placeholder="Имя владельца или студия" />
                    </label>
                    <label>
                      Маркировка размера
                      <input type="text" name="size_label" placeholder="M / 54" />
                    </label>
                    <label>
                      Рама (см)
                      <input type="text" name="frame_size_cm" placeholder="54" />
                    </label>
                    <label>
                      Передачи
                      <input type="text" name="gears" placeholder="2x11" />
                    </label>
                    <label>
                      Тип оси
                      <input type="text" name="axle_type" placeholder="12x142 TA" />
                    </label>
                    <label>
                      Кассета
                      <input type="text" name="cassette" placeholder="11-30" />
                    </label>
                    <label>
                      Рост минимальный (см)
                      <input type="number" name="height_min_cm" min={0} step="1" />
                    </label>
                    <label>
                      Рост максимальный (см)
                      <input type="number" name="height_max_cm" min={0} step="1" />
                    </label>
                  </div>
                  {createMutation.isError ? (
                    <div className="form-error">Не удалось создать велосипед. Проверьте данные и попробуйте снова.</div>
                  ) : null}
                  <div className="form-actions">
                    <button type="submit" disabled={createMutation.isPending}>
                      {createMutation.isPending ? "Создаем…" : "Создать велосипед"}
                    </button>
                  </div>
                </form>
              ) : (
                <div className="empty-state">Добавление доступно только администраторам.</div>
              )
            ) : selectedBike ? (
              <form className="bike-form" onSubmit={(event) => handleUpdateSubmit(event, selectedBike)}>
                <div className="form-grid">
                  <label>
                    Название
                    <span className="read-value">{selectedBike.title}</span>
                  </label>
                  <label>
                    Владелец
                    <span className="read-value">{selectedBike.owner ?? "—"}</span>
                  </label>
                  <label>
                    Маркировка размера
                    <span className="read-value">{selectedBike.size_label ?? "—"}</span>
                  </label>
                  <label>
                    Рама (см)
                    <span className="read-value">{selectedBike.frame_size_cm ?? "—"}</span>
                  </label>
                  <label>
                    Тип оси
                    <span className="read-value">{selectedBike.axle_type ?? "—"}</span>
                  </label>
                  <label>
                    Кассета
                    <span className="read-value">{selectedBike.cassette ?? "—"}</span>
                  </label>
                  <label>
                    Передачи
                    <span className="read-value">{selectedBike.gears ?? "—"}</span>
                  </label>
                  <label>
                    Рост минимальный (см)
                    {isAdmin ? (
                      <input
                        type="number"
                        name="height_min_cm"
                        defaultValue={selectedBike.height_min_cm ?? ""}
                        min={0}
                        step="1"
                      />
                    ) : (
                      <span className="read-value">{selectedBike.height_min_cm ?? "—"}</span>
                    )}
                  </label>
                  <label>
                    Рост максимальный (см)
                    {isAdmin ? (
                      <input
                        type="number"
                        name="height_max_cm"
                        defaultValue={selectedBike.height_max_cm ?? ""}
                        min={0}
                        step="1"
                      />
                    ) : (
                      <span className="read-value">{selectedBike.height_max_cm ?? "—"}</span>
                    )}
                  </label>
                </div>
                {isAdmin ? (
                  <div className="form-actions">
                    <button type="submit" disabled={updateMutation.isPending}>
                      {updateMutation.isPending ? "Сохраняем…" : "Сохранить изменения"}
                    </button>
                  </div>
                ) : (
                  <div className="trainer-hint">Изменения доступны только администраторам.</div>
                )}
                {updateMutation.isError ? (
                  <div className="form-error">Не удалось обновить параметры велосипеда, попробуйте еще раз.</div>
                ) : null}
              </form>
            ) : (
              <div className="empty-state">Выберите велосипед слева, чтобы увидеть детали.</div>
            )}
          </section>
        </div>
      )}
    </Panel>
  );
}

function formatHeightRange(bike: BikeRow): string {
  const min = bike.height_min_cm;
  const max = bike.height_max_cm;
  if (min != null && max != null) {
    return `Рост: ${min}–${max} см`;
  }
  if (min != null) {
    return `Рост от ${min} см`;
  }
  if (max != null) {
    return `Рост до ${max} см`;
  }
  return "Рост не задан";
}

function formatBikeSize(bike: BikeRow): string {
  const label = bike.size_label;
  const frame = bike.frame_size_cm;
  if (label && frame) {
    return `Размер ${label} · Рама ${frame} см`;
  }
  if (label) {
    return `Размер ${label}`;
  }
  if (frame) {
    return `Рама ${frame} см`;
  }
  return "Размер не указан";
}
