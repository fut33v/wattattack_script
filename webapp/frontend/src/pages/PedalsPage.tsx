import React, { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiFetch } from "../lib/api";
import type { PedalListResponse, PedalRow } from "../lib/types";

const PEDAL_TYPE_LABEL: Record<string, string> = {
  platform: "топталки",
  road_look: "шоссе Look",
  road_shimano: "шоссе Shimano",
  mtb_shimano: "MTB Shimano"
};

const PEDAL_OPTIONS = Object.entries(PEDAL_TYPE_LABEL).map(([value, label]) => ({
  value,
  label
}));

export default function PedalsPage() {
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const [pedalType, setPedalType] = useState<string>("platform");

  const listQuery = useQuery<PedalListResponse>({
    queryKey: ["pedals"],
    queryFn: () => apiFetch<PedalListResponse>("/api/pedals")
  });

  const pedals = useMemo<PedalRow[]>(() => listQuery.data?.items ?? [], [listQuery.data]);

  const createMutation = useMutation({
    mutationFn: () =>
      apiFetch<{ item: PedalRow }>("/api/pedals", {
        method: "POST",
        body: JSON.stringify({ name, pedal_type: pedalType })
      }),
    onSuccess: () => {
      setName("");
      queryClient.invalidateQueries({ queryKey: ["pedals"] });
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (id: number) =>
      apiFetch<{ ok: boolean }>(`/api/pedals/${id}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["pedals"] });
    }
  });

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <p className="eyebrow">Инвентарь</p>
          <h1>Педали</h1>
          <p className="muted">
            Типы педалей, которые можно назначать клиентам и учитывать при посадке.
          </p>
        </div>
      </div>

      <div className="panel">
        <div className="panel-title">Добавить педали</div>
        <div className="form-grid">
          <label>
            <span className="form-label">Название</span>
            <input
              type="text"
              name="name"
              placeholder="Например, Look Keo Rental"
              value={name}
              onChange={(e) => setName(e.target.value)}
              disabled={createMutation.isPending}
              required
            />
          </label>
          <label>
            <span className="form-label">Тип</span>
            <select
              name="pedal_type"
              value={pedalType}
              onChange={(e) => setPedalType(e.target.value)}
              disabled={createMutation.isPending}
            >
              {PEDAL_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </label>
          <div className="form-actions">
            <button
              type="button"
              className="button primary"
              onClick={() => createMutation.mutate()}
              disabled={!name.trim() || createMutation.isPending}
            >
              {createMutation.isPending ? "Сохраняем…" : "Сохранить"}
            </button>
          </div>
        </div>
        {createMutation.isError && (
          <p className="error">Не удалось сохранить педали. Проверьте уникальность названия.</p>
        )}
      </div>

      <div className="panel">
        <div className="panel-title">
          Список педалей {listQuery.isLoading && <span className="muted">Загружаем…</span>}
        </div>
        {listQuery.isError && <p className="error">Не удалось загрузить список педалей.</p>}
        {pedals.length === 0 && !listQuery.isLoading ? (
          <p className="muted">Список пуст — добавьте хотя бы один набор педалей.</p>
        ) : (
          <div className="detail-grid">
            {pedals.map((pedal) => (
              <div key={pedal.id} className="detail-card">
                <div className="detail-card__header">
                  <span className="pill pill-muted">
                    {PEDAL_TYPE_LABEL[pedal.pedal_type] ?? pedal.pedal_type}
                  </span>
                  <strong>{pedal.name}</strong>
                </div>
                <div className="detail-card__controls">
                  <span className="muted">id={pedal.id}</span>
                  <div className="detail-card__controls--right">
                    <button
                      className="button ghost danger"
                      onClick={() => deleteMutation.mutate(pedal.id)}
                      disabled={deleteMutation.isPending}
                    >
                      Удалить
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
