import { FormEvent, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch, ApiError } from "../lib/api";
import type { BikeListResponse, BikeRow, ClientRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";
import StateScreen from "../components/StateScreen";

interface ClientResponse {
  item: ClientRow;
}

const PEDAL_OPTIONS = [
  "топталки (под кроссовки)",
  "контакты шоссе Look",
  "контакты шоссе Shimano",
  "контакты MTB Shimano",
  "принесу свои"
] as const;

const GENDER_OPTIONS = [
  { value: "", label: "Не указано" },
  { value: "male", label: "Мужской" },
  { value: "female", label: "Женский" }
] as const;

export default function ClientCreatePage() {
  const { session } = useAppContext();
  const navigate = useNavigate();
  const [formError, setFormError] = useState<string | null>(null);

  const bikesQuery = useQuery<BikeListResponse>({
    queryKey: ["bikes"],
    queryFn: () => apiFetch<BikeListResponse>("/api/bikes")
  });

  const createMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch<ClientResponse>("/api/clients", {
        method: "POST",
        body: JSON.stringify(payload)
      }),
    onSuccess: (data) => {
      if (data?.item?.id) {
        navigate(`/clients/${data.item.id}`);
      } else {
        navigate("/clients");
      }
    },
    onError: (error) => {
      const message = error instanceof ApiError ? error.message : "Не удалось создать клиента.";
      setFormError(message);
    }
  });

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFormError(null);
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};

    const firstName = formData.get("first_name");
    const lastName = formData.get("last_name");
    if (typeof firstName !== "string" && typeof lastName !== "string") {
      setFormError("Укажите имя или фамилию клиента.");
      return;
    }

    const textFields = ["first_name", "last_name", "pedals", "goal", "favorite_bike", "saddle_height", "gender"] as const;
    textFields.forEach((field) => {
      const value = formData.get(field);
      if (typeof value === "string") {
        payload[field] = value.trim() || null;
      }
    });

    const numberFields = ["weight", "height", "ftp"] as const;
    numberFields.forEach((field) => {
      const value = formData.get(field);
      if (typeof value === "string") {
        const trimmed = value.trim();
        if (trimmed === "") {
          payload[field] = null;
        } else {
          const normalized = trimmed.replace(",", ".");
          const numberValue = Number(normalized);
          if (!Number.isNaN(numberValue)) {
            payload[field] = numberValue;
          }
        }
      }
    });

    if (!payload.first_name && !payload.last_name) {
      setFormError("Укажите хотя бы имя или фамилию клиента.");
      return;
    }

    createMutation.mutate(payload);
  }

  const bikes = useMemo<BikeRow[]>(() => bikesQuery.data?.items ?? [], [bikesQuery.data]);

  if (!session.isAdmin) {
    return <StateScreen title="Недостаточно прав" message="Создавать клиентов могут только администраторы." />;
  }

  return (
    <Panel
      title="Новый клиент"
      subtitle="Заполните данные клиента и сохраните карточку"
      headerExtra={
        <Link className="button" to="/clients">
          ← К списку
        </Link>
      }
    >
      <form className="form-grid" onSubmit={handleSubmit}>
        <label>
          Имя
          <input type="text" name="first_name" placeholder="Имя клиента" />
        </label>
        <label>
          Фамилия
          <input type="text" name="last_name" placeholder="Фамилия клиента" />
        </label>
        <label>
          Пол
          <select name="gender" defaultValue="">
            {GENDER_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          Рост (см)
          <input type="number" name="height" min={0} step="1" />
        </label>
        <label>
          Вес (кг)
          <input type="number" name="weight" min={0} step="0.1" />
        </label>
        <label>
          FTP
          <input type="number" name="ftp" min={0} step="1" />
        </label>
        <label>
          Любимый велосипед
          <select name="favorite_bike" defaultValue="" disabled={bikesQuery.isLoading}>
            <option value="">— Не выбран —</option>
            {bikes.map((bike) => (
              <option key={bike.id} value={bike.title}>
                {bike.title}
                {bike.owner ? ` (${bike.owner})` : ""}
              </option>
            ))}
          </select>
          {bikesQuery.isError && <span className="trainer-hint">Не удалось загрузить список велосипедов.</span>}
        </label>
        <label>
          Педали
          <select name="pedals" defaultValue="">
            <option value="">— Не выбрано —</option>
            {PEDAL_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <label>
          Высота седла
          <input type="text" name="saddle_height" placeholder="Например, 72.5" />
        </label>
        <label>
          Цель
          <textarea name="goal" rows={3} placeholder="Цель клиента" />
        </label>
        {formError && <div className="form-error">{formError}</div>}
        <div className="form-actions">
          <button type="submit" className="button" disabled={createMutation.isPending}>
            {createMutation.isPending ? "Создаём…" : "Создать клиента"}
          </button>
          <Link className="button secondary" to="/clients">
            Отмена
          </Link>
        </div>
      </form>
    </Panel>
  );
}
