import { useMemo } from "react";
import { useNavigate, useParams, Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import dayjs from "dayjs";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { ClientRow } from "../lib/types";
import StateScreen from "../components/StateScreen";

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

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const payload: Record<string, unknown> = {};

    const numericFields = ["weight", "ftp"] as const;
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

    const textFields = ["favorite_bike", "pedals"] as const;
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
              <dd>{client.gender ?? "—"}</dd>
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
              Любимый велосипед
              <input type="text" name="favorite_bike" defaultValue={client.favorite_bike ?? ""} />
            </label>
            <label>
              Педали
              <input type="text" name="pedals" defaultValue={client.pedals ?? ""} />
            </label>
            <div className="form-actions">
              <button type="submit" className="button" disabled={updateMutation.isPending}>
                {updateMutation.isPending ? "Сохраняем…" : "Сохранить"}
              </button>
              <Link className="button" to="/clients">
                К списку
              </Link>
            </div>
            {updateMutation.isSuccess && <div className="muted">Изменения сохранены.</div>}
          </form>
        </section>
      </div>
    </Panel>
  );
}
