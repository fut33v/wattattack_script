import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import classNames from "classnames";
import { useEffect, useMemo, useState, type FormEvent } from "react";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { BikeListResponse, BikeRow, TrainerListResponse, TrainerRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

const AXLE_OPTIONS = [
  { value: "ЭКС", label: "ЭКС" },
  { value: "ОСЬ", label: "ОСЬ" }
];

const CASSETTE_OPTIONS = ["7", "8", "9", "10", "11", "12"];

export default function TrainersPage() {
  const { session } = useAppContext();
  const queryClient = useQueryClient();
  const [selectedTrainerId, setSelectedTrainerId] = useState<number | null>(null);

  const listQuery = useQuery<TrainerListResponse>({
    queryKey: ["trainers"],
    queryFn: () => apiFetch<TrainerListResponse>("/api/trainers")
  });

  const bikesQuery = useQuery<BikeListResponse>({
    queryKey: ["bikes"],
    queryFn: () => apiFetch<BikeListResponse>("/api/bikes")
  });

  const trainers = listQuery.data?.items ?? [];
  const bikes = bikesQuery.data?.items ?? [];
  const bikesLoading = bikesQuery.isLoading;
  const bikesError = bikesQuery.isError;
  const isAdmin = session.isAdmin;

  useEffect(() => {
    if (trainers.length === 0) {
      if (selectedTrainerId !== null) {
        setSelectedTrainerId(null);
      }
      return;
    }
    const exists = selectedTrainerId !== null && trainers.some((trainer) => trainer.id === selectedTrainerId);
    if (!exists) {
      setSelectedTrainerId(trainers[0]?.id ?? null);
    }
  }, [trainers, selectedTrainerId]);

  const selectedTrainer = useMemo<TrainerRow | null>(
    () => trainers.find((item) => item.id === selectedTrainerId) ?? null,
    [selectedTrainerId, trainers]
  );

  const updateMutation = useMutation({
    mutationFn: ({ id, payload }: { id: number; payload: Record<string, unknown> }) =>
      apiFetch(`/api/trainers/${id}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["trainers"] });
      queryClient.invalidateQueries({ queryKey: ["bikes"] });
    }
  });

  const currentAxleType = selectedTrainer?.axle_types?.trim() ?? "";
  const isCustomAxleType =
    currentAxleType !== "" && !AXLE_OPTIONS.some((option) => option.value === currentAxleType);
  const currentCassette = selectedTrainer?.cassette?.trim() ?? "";
  const isCustomCassette = currentCassette !== "" && !CASSETTE_OPTIONS.includes(currentCassette);

  function handleSubmit(event: FormEvent<HTMLFormElement>, trainer: TrainerRow) {
    event.preventDefault();
    if (!isAdmin) return;
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};

    ["title", "display_name", "owner", "axle_types", "cassette"].forEach((key) => {
      const value = formData.get(key);
      if (typeof value === "string") {
        payload[key] = value.trim() ? value.trim() : null;
      }
    });

    const bikeValue = formData.get("bike_id");
    if (typeof bikeValue === "string") {
      const trimmed = bikeValue.trim();
      if (trimmed === "") {
        payload.bike_id = null;
      } else {
        const bikeId = Number(trimmed);
        if (!Number.isNaN(bikeId)) {
          payload.bike_id = bikeId;
        }
      }
    }

    if (Object.keys(payload).length === 0) {
      return;
    }

    updateMutation.mutate({ id: trainer.id, payload });
  }

  const listContent =
    trainers.length === 0 ? (
      <div className="empty-state">Тренажеры не найдены.</div>
    ) : (
      <div className="trainers-list-items">
        {trainers.map((trainer) => (
          <button
            key={trainer.id}
            type="button"
            className={classNames("trainer-list-item", { active: trainer.id === selectedTrainerId })}
            onClick={() => setSelectedTrainerId(trainer.id)}
          >
            <span className="trainer-list-title">{resolveTitle(trainer)}</span>
            <span className="trainer-list-meta">
              ID {trainer.id}
              {trainer.code ? ` · Код ${trainer.code}` : ""}
              {trainer.owner ? ` · ${trainer.owner}` : ""}
            </span>
            <span className="trainer-list-bike">
              {trainer.bike_title ? `Велосипед: ${trainer.bike_title}` : "Велосипед не назначен"}
            </span>
          </button>
        ))}
      </div>
    );

  return (
    <Panel title="Тренажеры" subtitle="Выберите станок слева и настройте параметры справа">
      {listQuery.isLoading ? (
        <div className="empty-state">Загружаем тренажеры…</div>
      ) : listQuery.isError ? (
        <div className="empty-state">Не удалось загрузить список тренажеров.</div>
      ) : (
        <div className="trainers-layout">
          <aside className="trainers-list">{listContent}</aside>
          <section className="trainer-details">
            {selectedTrainer ? (
              <form
                key={selectedTrainer.id}
                className="trainer-form"
                onSubmit={(event) => handleSubmit(event, selectedTrainer)}
              >
                <div className="form-grid">
                  <label>
                    Название
                    {isAdmin ? (
                      <input type="text" name="title" defaultValue={selectedTrainer.title ?? ""} />
                    ) : (
                      <span className="read-value">{selectedTrainer.title ?? "—"}</span>
                    )}
                  </label>
                  <label>
                    ID станка
                    <span className="read-value">{selectedTrainer.id}</span>
                  </label>
                  <label>
                    Отображаемое имя
                    {isAdmin ? (
                      <input type="text" name="display_name" defaultValue={selectedTrainer.display_name ?? ""} />
                    ) : (
                      <span className="read-value">{selectedTrainer.display_name ?? "—"}</span>
                    )}
                  </label>
                  <label>
                    Владелец
                    {isAdmin ? (
                      <input type="text" name="owner" defaultValue={selectedTrainer.owner ?? ""} />
                    ) : (
                      <span className="read-value">{selectedTrainer.owner ?? "—"}</span>
                    )}
                  </label>
                  <label>
                    Установленный велосипед
                    {isAdmin ? (
                      <select
                        name="bike_id"
                        defaultValue={selectedTrainer.bike_id ?? ""}
                        disabled={bikesLoading || bikesError}
                      >
                        <option value="">— Не назначен —</option>
                        {bikesLoading && <option disabled>Загружаем список…</option>}
                        {!bikesLoading &&
                          !bikesError &&
                          bikes.map((bike: BikeRow) => (
                            <option key={bike.id} value={bike.id}>
                              {bike.title}
                              {bike.owner ? ` (${bike.owner})` : ""}
                            </option>
                          ))}
                      </select>
                    ) : (
                      <span className="read-value">{selectedTrainer.bike_title ?? "Велосипед не назначен"}</span>
                    )}
                    {isAdmin && bikesError ? (
                      <div className="trainer-hint">Не удалось загрузить список велосипедов.</div>
                    ) : null}
                  </label>
                  <label>
                    Типы осей
                    {isAdmin ? (
                      <select name="axle_types" defaultValue={currentAxleType}>
                        <option value="">— Не указано —</option>
                        {AXLE_OPTIONS.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                        {isCustomAxleType && (
                          <option value={currentAxleType}>Другое: {currentAxleType}</option>
                        )}
                      </select>
                    ) : (
                      <span className="read-value">{currentAxleType || "—"}</span>
                    )}
                  </label>
                  <label>
                    Кассета
                    {isAdmin ? (
                      <select name="cassette" defaultValue={currentCassette}>
                        <option value="">— Не указано —</option>
                        {CASSETTE_OPTIONS.map((item) => (
                          <option key={item} value={item}>
                            {item}
                          </option>
                        ))}
                        {isCustomCassette && <option value={currentCassette}>Другое: {currentCassette}</option>}
                      </select>
                    ) : (
                      <span className="read-value">{currentCassette || "—"}</span>
                    )}
                  </label>
                  {selectedTrainer.notes ? (
                    <label>
                      Примечания
                      <span className="read-value">{selectedTrainer.notes}</span>
                    </label>
                  ) : null}
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
              </form>
            ) : (
              <div className="empty-state">Выберите тренажер слева, чтобы увидеть детали.</div>
            )}
          </section>
        </div>
      )}
    </Panel>
  );
}

function resolveTitle(trainer: TrainerRow): string {
  return (
    trainer.title?.trim() ||
    trainer.display_name?.trim() ||
    (trainer.code ? `Станок ${trainer.code}` : `Станок #${trainer.id}`)
  );
}
