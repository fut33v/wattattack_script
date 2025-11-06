import { FormEvent, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type { InstructorListResponse, InstructorRow } from "../lib/types";
import { useAppContext } from "../lib/AppContext";

import "../styles/schedule.css";

export default function InstructorsPage() {
  const { session } = useAppContext();
  const isAdmin = session.isAdmin;
  const queryClient = useQueryClient();
  const [nameValue, setNameValue] = useState("");

  const listQuery = useQuery<InstructorListResponse>({
    queryKey: ["instructors"],
    queryFn: () => apiFetch<InstructorListResponse>("/api/instructors")
  });

  const createMutation = useMutation({
    mutationFn: (fullName: string) =>
      apiFetch("/api/instructors", {
        method: "POST",
        body: JSON.stringify({ full_name: fullName })
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["instructors"] });
      setNameValue("");
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (id: number) =>
      apiFetch(`/api/instructors/${id}`, {
        method: "DELETE"
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["instructors"] });
    }
  });

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!isAdmin) return;
    const trimmed = nameValue.trim();
    if (!trimmed) return;
    createMutation.mutate(trimmed);
  }

  const instructors = listQuery.data?.items ?? [];

  return (
    <Panel title="Инструкторы" subtitle="Управляйте списком инструкторов для расписания">
      <div className="instructors-layout">
        <section className="instructors-section">
          <h2 className="section-heading">Список</h2>
          {listQuery.isLoading ? (
            <div className="empty-state">Загружаем инструкторов…</div>
          ) : listQuery.isError ? (
            <div className="empty-state">Не удалось загрузить список инструкторов.</div>
          ) : instructors.length === 0 ? (
            <div className="empty-state">Инструкторы не добавлены.</div>
          ) : (
            <ul className="instructors-list">
              {instructors.map((item: InstructorRow) => (
                <li key={item.id} className="instructor-item">
                  <span className="instructor-name">{item.full_name}</span>
                  {isAdmin ? (
                    <button
                      type="button"
                      className="btn danger"
                      onClick={() => deleteMutation.mutate(item.id)}
                      disabled={deleteMutation.isPending}
                    >
                      Удалить
                    </button>
                  ) : null}
                </li>
              ))}
            </ul>
          )}
        </section>
        {isAdmin ? (
          <section className="instructors-section">
            <h2 className="section-heading">Добавить</h2>
            <form className="instructors-form" onSubmit={handleSubmit}>
              <label>
                Имя и фамилия
                <input
                  type="text"
                  value={nameValue}
                  onChange={(event) => setNameValue(event.target.value)}
                  placeholder="Например, Евгений Балакин"
                />
              </label>
              <button type="submit" className="btn primary" disabled={createMutation.isPending}>
                Добавить
              </button>
            </form>
          </section>
        ) : null}
      </div>
    </Panel>
  );
}
