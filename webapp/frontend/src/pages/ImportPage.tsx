import { useMemo, useState } from "react";
import { useMutation } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch, ApiError } from "../lib/api";

interface ImportResponse {
  status: string;
  restored_at: string;
  applied_bytes: number;
  duration_ms: number;
  stdout?: string;
  stderr?: string;
  filename?: string;
  database_url_source?: string;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} Б`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} КБ`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} МБ`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} ГБ`;
}

export default function ImportPage() {
  const [file, setFile] = useState<File | null>(null);

  const importMutation = useMutation({
    mutationFn: (payload: File) => {
      const formData = new FormData();
      formData.append("file", payload);
      return apiFetch<ImportResponse>("/api/backup/import", { method: "POST", body: formData });
    }
  });

  const lastResult = importMutation.data;
  const errorMessage = useMemo(() => {
    if (!importMutation.isError) return "";
    const err = importMutation.error as ApiError;
    const detail = (err.body as any)?.detail;
    if (typeof detail === "string") return detail;
    if (detail?.message) return detail.message;
    return err.message || "Не удалось выполнить импорт.";
  }, [importMutation.error, importMutation.isError]);

  function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    if (!file) return;
    importMutation.mutate(file);
  }

  return (
    <div className="import-page">
      <Panel
        title="Импорт бэкапа"
        subtitle="Загрузите .sql или .sql.gz; данные применятся к текущей базе"
      >
        <div className="pill pill-muted" style={{ marginBottom: "0.8rem", display: "inline-flex" }}>
          ⚠️ Импорт заменяет состояние БД. Делайте только с проверенными файлами.
        </div>
        <form className="import-form" onSubmit={handleSubmit}>
          <div className="form-group">
            <label>
              Файл бэкапа
              <input
                type="file"
                accept=".sql,.gz,.sql.gz"
                onChange={(event) => setFile(event.target.files?.[0] ?? null)}
              />
            </label>
            <p className="form-hint">Размер ограничен 512 МБ по умолчанию. Используйте свежие дампы.</p>
          </div>

          <div className="form-actions">
            <button className="button primary" type="submit" disabled={!file || importMutation.isPending}>
              {importMutation.isPending ? "Импортируем…" : "Импортировать"}
            </button>
          </div>
        </form>

        {importMutation.isError && (
          <div className="form-message error" role="alert">
            {errorMessage}
          </div>
        )}

        {lastResult && (
          <div className="import-result">
            <div className="import-meta">
              <span className="pill">OK</span>
              <span className="pill pill-muted">
                {formatBytes(lastResult.applied_bytes)} • {Math.round(lastResult.duration_ms / 100) / 10}s
              </span>
              {lastResult.filename && <span className="pill pill-muted">{lastResult.filename}</span>}
              <span className="muted">
                {new Date(lastResult.restored_at).toLocaleString("ru-RU")} • {lastResult.database_url_source ?? "DB_*"}
              </span>
            </div>
            {lastResult.stdout && (
              <div className="import-log" aria-label="stdout">
                <div className="muted" style={{ marginBottom: "0.35rem", fontSize: "0.85rem" }}>
                  Вывод psql
                </div>
                <pre>{lastResult.stdout}</pre>
              </div>
            )}
            {lastResult.stderr && lastResult.stderr.trim() && (
              <div className="import-log import-log--error" aria-label="stderr">
                <div className="muted" style={{ marginBottom: "0.35rem", fontSize: "0.85rem" }}>
                  Предупреждения
                </div>
                <pre>{lastResult.stderr}</pre>
              </div>
            )}
          </div>
        )}
      </Panel>
    </div>
  );
}
