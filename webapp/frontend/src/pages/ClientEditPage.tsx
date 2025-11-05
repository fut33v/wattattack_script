import { useMemo } from "react";
import { useNavigate, useParams, Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import { apiFetch, ApiError } from "../lib/api";
import type { BikeListResponse, BikeRow, ClientRow } from "../lib/types";
import StateScreen from "../components/StateScreen";

const PEDAL_OPTIONS = [
  "топталки (под кроссовки)",
  "контакты шоссе Look",
  "контакты шоссе Shimano",
  "контакты MTB Shimano",
  "принесу свои"
] as const;

const GENDER_LABELS: Record<string, string> = {
  male: "Мужской",
  female: "Женский"
};

const GENDER_OPTIONS = [
  { value: "", label: "Не указано" },
  { value: "male", label: "Мужской" },
  { value: "female", label: "Женский" }
] as const;

function formatGender(value: string | null | undefined): string {
  if (!value) return "—";
  const key = value.toLowerCase();
  return GENDER_LABELS[key] ?? value;
}

interface ClientResponse {
  item: ClientRow;
}

export default function ClientEditPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const clientId = useMemo(() => Number(id), [id]);

  const isIdValid = Number.isInteger(clientId) && clientId > 0;

  const clientQuery = useQuery<ClientResponse>({
    queryKey: ["client", clientId],
    queryFn: () => apiFetch<ClientResponse>(`/api/clients/${clientId}`),
    enabled: isIdValid
  });

  const bikesQuery = useQuery<BikeListResponse>({
    queryKey: ["bikes"],
    queryFn: () => apiFetch<BikeListResponse>("/api/bikes")
  });

  const updateMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) =>
      apiFetch(`/api/clients/${clientId}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["clients"], exact: false });
      queryClient.invalidateQueries({ queryKey: ["client", clientId] });
    }
  });

  if (!isIdValid) {
    return <StateScreen title="Некорректный ID клиента" message="Проверьте ссылку и попробуйте снова." />;
  }

  if (clientQuery.isLoading) {
    return <StateScreen title="Загрузка клиента" message="Получаем данные…" />;
  }

  if (clientQuery.isError || !clientQuery.data) {
    return <StateScreen title="Ошибка" message="Не удалось загрузить данные клиента." action={<Link className="button" to="/clients">Назад к списку</Link>} />;
  }

  const client = clientQuery.data.item;
  const bikes = bikesQuery.data?.items ?? [];
  const favoriteBikeValue = client.favorite_bike ?? "";
  const isCustomFavoriteBike =
    favoriteBikeValue !== "" && !bikes.some((bike: BikeRow) => bike.title === favoriteBikeValue);
  const pedalsValue = client.pedals ?? "";
  const isCustomPedals = pedalsValue !== "" && !PEDAL_OPTIONS.includes(pedalsValue as (typeof PEDAL_OPTIONS)[number]);

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};

    const numericFields = ["weight", "ftp", "height"] as const;
    numericFields.forEach((field) => {
      const value = formData.get(field);
      if (value === null || value === "") {
        payload[field] = null;
        return;
      }
      const num = Number(value);
      if (!Number.isNaN(num)) {
        payload[field] = num;
      }
    });

    const textFields = ["first_name", "last_name", "favorite_bike", "pedals", "goal", "gender", "saddle_height"] as const;
    textFields.forEach((field) => {
      const value = formData.get(field);
      if (typeof value === "string") {
        payload[field] = value.trim() || null;
      }
    });

    updateMutation.mutate(payload);
  }

  return (
    <Panel
      title={`Клиент #${client.id}`}
      subtitle={client.full_name || [client.first_name, client.last_name].filter(Boolean).join(" ") || "Без имени"}
      headerExtra={
        <button className="button" type="button" onClick={() => navigate(-1)}>
          ← Назад
        </button>
      }
    >
      <div className="detail-grid">
        <section className="detail-card">
          <h3>Основные данные</h3>
          <dl>
            <div>
              <dt>Полное имя</dt>
              <dd>{client.full_name ?? "—"}</dd>
            </div>
            <div>
              <dt>Имя</dt>
              <dd>{client.first_name ?? "—"}</dd>
            </div>
            <div>
              <dt>Фамилия</dt>
              <dd>{client.last_name ?? "—"}</dd>
            </div>
            <div>
              <dt>Пол</dt>
              <dd>{formatGender(client.gender)}</dd>
            </div>
            <div>
              <dt>Рост</dt>
              <dd>{client.height ? `${client.height} см` : "—"}</dd>
            </div>
            <div>
              <dt>Седло</dt>
              <dd>{client.saddle_height ?? "—"}</dd>
            </div>
            <div>
              <dt>Цель</dt>
              <dd>{client.goal ?? "—"}</dd>
            </div>
            <div>
              <dt>Анкета заполнена</dt>
              <dd>{client.submitted_at ? dayjs(client.submitted_at).format("DD.MM.YYYY HH:mm") : "—"}</dd>
            </div>
          </dl>
        </section>

        <section className="detail-card">
          <h3>Редактирование</h3>
          <form className="form-grid" onSubmit={handleSubmit}>
            <label>
              Вес (кг)
              <input type="number" step="0.1" name="weight" defaultValue={client.weight ?? ""} />
            </label>
            <label>
              FTP
              <input type="number" step="1" name="ftp" defaultValue={client.ftp ?? ""} />
            </label>
            <label>
              Рост (см)
              <input type="number" step="1" min={0} name="height" defaultValue={client.height ?? ""} />
            </label>
            <label>
              Имя
              <input type="text" name="first_name" defaultValue={client.first_name ?? ""} />
            </label>
            <label>
              Фамилия
              <input type="text" name="last_name" defaultValue={client.last_name ?? ""} />
            </label>
            <label>
              Пол
              <select name="gender" defaultValue={client.gender ?? ""}>
                {GENDER_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Любимый велосипед
              <select name="favorite_bike" defaultValue={favoriteBikeValue} disabled={bikesQuery.isLoading}>
                <option value="">— Не выбран —</option>
                {bikes.map((bike) => (
                  <option key={bike.id} value={bike.title}>
                    {bike.title}
                    {bike.owner ? ` (${bike.owner})` : ""}
                  </option>
                ))}
                {isCustomFavoriteBike && <option value={favoriteBikeValue}>Другой: {favoriteBikeValue}</option>}
              </select>
              {bikesQuery.isError && <span className="trainer-hint">Не удалось загрузить список велосипедов.</span>}
            </label>
            <label>
              Педали
              <select name="pedals" defaultValue={pedalsValue}>
                <option value="">— Не выбрано —</option>
                {PEDAL_OPTIONS.map((label) => (
                  <option key={label} value={label}>
                    {label}
                  </option>
                ))}
                {isCustomPedals && <option value={pedalsValue}>Другие: {pedalsValue}</option>}
              </select>
            </label>
            <label>
              Высота седла
              <input type="text" name="saddle_height" defaultValue={client.saddle_height ?? ""} />
            </label>
            <label>
              Цель
              <textarea name="goal" rows={3} defaultValue={client.goal ?? ""} />
            </label>
            <div className="form-actions">
              <button type="submit" className="button" disabled={updateMutation.isPending}>
                {updateMutation.isPending ? "Сохраняем…" : "Сохранить"}
              </button>
              <Link className="button" to="/clients">
                К списку
              </Link>
            </div>
            {updateMutation.isError && (
              <div className="form-error">
                {(updateMutation.error as ApiError)?.message ?? "Не удалось сохранить изменения."}
              </div>
            )}
            {updateMutation.isSuccess && <div className="muted">Изменения сохранены.</div>}
          </form>
        </section>
      </div>
    </Panel>
  );
}
