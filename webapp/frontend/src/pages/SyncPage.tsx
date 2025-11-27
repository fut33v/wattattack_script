import { ChangeEvent, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";

interface SyncResponse {
  processed: number;
  updated: number;
  fit_downloaded: number;
  accounts: Record<string, Record<string, number>>;
}

interface SyncStatus {
  running: boolean;
  started_at?: string | null;
  finished_at?: string | null;
  accounts_total: number;
  accounts_done: number;
  current_account?: string | null;
  log: string[];
  summary: Record<string, Record<string, number>>;
  processed: number;
  updated: number;
  fit_downloaded: number;
  error?: string | null;
}

interface StravaCandidate {
  client_id: number;
  client_name?: string | null;
  tg_user_id: number;
  strava_athlete_name?: string | null;
  pending: number;
  with_fit: number;
  last_activity_at?: string | null;
}

interface StravaCandidatesResponse {
  items: StravaCandidate[];
  straver_configured?: boolean;
}

interface StravaStatus {
  running: boolean;
  started_at?: string | null;
  finished_at?: string | null;
  users_total: number;
  users_done: number;
  current_user?: string | null;
  log: string[];
  summary: Record<string, Record<string, number>>;
  uploaded: number;
  skipped: number;
  error?: string | null;
}

interface LegacyImportResponse {
  processed: number;
  stored: number;
  matched: number;
  skipped: number;
  errors?: string[];
  log?: string[];
  created_reservations?: number;
}

interface LegacyNormalizeResponse {
  legacy_accounts: string[];
  updated: number;
  migrated_rows: number;
  moved_files: number;
  conflicts: number;
  errors?: string[];
}

interface IntervalsCandidate {
  client_id: number;
  client_name?: string | null;
  tg_user_id: number;
  pending: number;
  with_fit: number;
  last_activity_at?: string | null;
}

interface IntervalsCandidatesResponse {
  items: IntervalsCandidate[];
}

interface IntervalsStatus {
  running: boolean;
  started_at?: string | null;
  finished_at?: string | null;
  users_total: number;
  users_done: number;
  current_user?: string | null;
  log: string[];
  summary: Record<string, Record<string, number>>;
  uploaded: number;
  skipped: number;
  error?: string | null;
}

function formatDateTime(value?: string | null): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export default function SyncPage() {
  const [log, setLog] = useState<string>("");
  const [status, setStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [lastRun, setLastRun] = useState<string>("");
  const [progress, setProgress] = useState<string>("");
  const [currentAccount, setCurrentAccount] = useState<string>("");

  const [stravaLog, setStravaLog] = useState<string>("");
  const [stravaStatus, setStravaStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [stravaLastRun, setStravaLastRun] = useState<string>("");
  const [stravaProgress, setStravaProgress] = useState<string>("");
  const [selectedUsers, setSelectedUsers] = useState<number[]>([]);
  const [intervalsLog, setIntervalsLog] = useState<string>("");
  const [intervalsStatus, setIntervalsStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [intervalsLastRun, setIntervalsLastRun] = useState<string>("");
  const [intervalsProgress, setIntervalsProgress] = useState<string>("");
  const [intervalsSelectedUsers, setIntervalsSelectedUsers] = useState<number[]>([]);
  const [legacyFile, setLegacyFile] = useState<File | null>(null);
  const [legacyStatus, setLegacyStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [legacySummary, setLegacySummary] = useState<string>("");
  const [legacyErrors, setLegacyErrors] = useState<string[]>([]);
  const [legacyLog, setLegacyLog] = useState<string>("");
  const [legacyOpen, setLegacyOpen] = useState(false);
  const [normalizeSummary, setNormalizeSummary] = useState<string>("");
  const [normalizeErrors, setNormalizeErrors] = useState<string[]>([]);
  const [normalizeStatus, setNormalizeStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const clearLogs = useMutation({
    mutationFn: () => apiFetch<{ status: string }>("/api/sync/status/clear", { method: "POST" }),
    onSuccess: () => {
      setLog("");
      setStravaLog("");
      setProgress("");
      setStravaProgress("");
      setIntervalsLog("");
      setIntervalsProgress("");
    },
  });

  const statusQuery = useQuery<SyncStatus>({
    queryKey: ["sync-status"],
    queryFn: () => apiFetch<SyncStatus>("/api/sync/status"),
    refetchInterval: (query) => (query.state.data?.running ? 2000 : false),
    refetchIntervalInBackground: true,
  });
  const syncMutation = useMutation({
    mutationFn: () => apiFetch<SyncResponse>("/api/sync/activities", { method: "POST" }),
    onMutate: () => {
      setStatus("running");
      setLog("Старт синхронизации…");
      setProgress("");
      setCurrentAccount("");
    },
    onSuccess: () => {
      setStatus("running"); // switch to polling state
      setLastRun(new Date().toLocaleString());
      statusQuery.refetch();
    },
    onError: (error: any) => {
      setStatus("error");
      setLog(`Ошибка: ${error?.message ?? "неизвестно"}`);
    },
  });

  const stravaCandidatesQuery = useQuery<StravaCandidatesResponse>({
    queryKey: ["strava-candidates"],
    queryFn: () => apiFetch<StravaCandidatesResponse>("/api/sync/strava/candidates"),
  });

  const stravaStatusQuery = useQuery<StravaStatus>({
    queryKey: ["strava-status"],
    queryFn: () => apiFetch<StravaStatus>("/api/sync/strava/status"),
    refetchInterval: (query) => (query.state.data?.running ? 2000 : false),
    refetchIntervalInBackground: true,
  });
  const intervalsCandidatesQuery = useQuery<IntervalsCandidatesResponse>({
    queryKey: ["intervals-candidates"],
    queryFn: () => apiFetch<IntervalsCandidatesResponse>("/api/sync/intervals/candidates"),
  });
  const intervalsStatusQuery = useQuery<IntervalsStatus>({
    queryKey: ["intervals-status"],
    queryFn: () => apiFetch<IntervalsStatus>("/api/sync/intervals/status"),
    refetchInterval: (query) => (query.state.data?.running ? 2000 : false),
    refetchIntervalInBackground: true,
  });

  const stravaBackfill = useMutation({
    mutationFn: (payload: { tg_user_ids: number[] }) =>
      apiFetch("/api/sync/strava/backfill", { method: "POST", body: payload }),
    onMutate: () => {
      setStravaStatus("running");
      setStravaLog("Стартуем загрузку в Strava…");
    },
    onSuccess: () => {
      stravaStatusQuery.refetch();
    },
    onError: (error: any) => {
      setStravaStatus("error");
      setStravaLog(`Ошибка: ${error?.message ?? "неизвестно"}`);
    },
  });
  const intervalsBackfill = useMutation({
    mutationFn: (payload: { tg_user_ids: number[] }) =>
      apiFetch("/api/sync/intervals/backfill", { method: "POST", body: payload }),
    onMutate: () => {
      setIntervalsStatus("running");
      setIntervalsLog("Стартуем загрузку в Intervals…");
    },
    onSuccess: () => {
      intervalsStatusQuery.refetch();
    },
    onError: (error: any) => {
      setIntervalsStatus("error");
      setIntervalsLog(`Ошибка: ${error?.message ?? "неизвестно"}`);
    },
  });

  const legacyImport = useMutation({
    mutationFn: (file: File) => {
      const formData = new FormData();
      formData.append("file", file);
      return apiFetch<LegacyImportResponse>("/api/sync/legacy/import", { method: "POST", body: formData });
    },
    onMutate: () => {
      setLegacyStatus("running");
      setLegacySummary("Импортируем историю…");
      setLegacyErrors([]);
      setLegacyLog("");
    },
    onSuccess: (data) => {
      setLegacyStatus("done");
      setLegacySummary(
        `Обработано: ${data.processed}. Записано: ${data.stored}. По имени сопоставлено: ${data.matched}. Создано бронирований: ${data.created_reservations ?? 0}. Пропущено: ${data.skipped}.`,
      );
      setLegacyErrors(data.errors?.slice(0, 15) ?? []);
      setLegacyLog((data.log || []).join("\n"));
    },
    onError: (error: any) => {
      setLegacyStatus("error");
      setLegacySummary(`Ошибка: ${error?.message ?? "неизвестно"}`);
      setLegacyLog("");
    },
  });

  const normalizeAccounts = useMutation({
    mutationFn: () => apiFetch<LegacyNormalizeResponse>("/api/sync/legacy/normalize_accounts", { method: "POST" }),
    onMutate: () => {
      setNormalizeStatus("running");
      setNormalizeSummary("Переименовываем аккаунты krutilkavn → krutilka_...");
      setNormalizeErrors([]);
    },
    onSuccess: (data) => {
      setNormalizeStatus("done");
      setNormalizeSummary(
        `Обновлено аккаунтов: ${data.updated}. Перенесено строк: ${data.migrated_rows}. FIT перемещено: ${data.moved_files}. Конфликты: ${data.conflicts}.`,
      );
      setNormalizeErrors(data.errors?.slice(0, 15) ?? []);
    },
    onError: (error: any) => {
      setNormalizeStatus("error");
      setNormalizeSummary(`Ошибка: ${error?.message ?? "неизвестно"}`);
    },
  });

  useEffect(() => {
    const data = statusQuery.data;
    if (!data) return;
    const lines = (data.log || []).join("\n");
    setLog(lines);
    setCurrentAccount(data.current_account || "");
    if (data.accounts_total > 0) {
      setProgress(
        `${data.accounts_done}/${data.accounts_total}` +
          (data.current_account ? ` · текущий: ${data.current_account}` : ""),
      );
    } else {
      setProgress("");
    }
    if (data.running) {
      setStatus("running");
    } else if (data.finished_at) {
      setStatus(data.error ? "error" : "done");
      setLastRun(new Date().toLocaleString());
    }
  }, [statusQuery.data]);

  useEffect(() => {
    const data = stravaStatusQuery.data;
    if (!data) return;
    const lines = (data.log || []).join("\n");
    setStravaLog(lines);
    if (data.users_total > 0) {
      setStravaProgress(
        `${data.users_done}/${data.users_total}` +
          (data.current_user ? ` · текущий: ${data.current_user}` : ""),
      );
    } else {
      setStravaProgress("");
    }
    if (data.running) {
      setStravaStatus("running");
    } else if (data.finished_at) {
      setStravaStatus(data.error ? "error" : "done");
      setStravaLastRun(new Date().toLocaleString());
      stravaCandidatesQuery.refetch();
    }
  }, [stravaStatusQuery.data, stravaCandidatesQuery.refetch]);

  useEffect(() => {
    const data = intervalsStatusQuery.data;
    if (!data) return;
    const lines = (data.log || []).join("\n");
    setIntervalsLog(lines);
    if (data.users_total > 0) {
      setIntervalsProgress(
        `${data.users_done}/${data.users_total}` +
          (data.current_user ? ` · текущий: ${data.current_user}` : ""),
      );
    } else {
      setIntervalsProgress("");
    }
    if (data.running) {
      setIntervalsStatus("running");
    } else if (data.finished_at) {
      setIntervalsStatus(data.error ? "error" : "done");
      setIntervalsLastRun(new Date().toLocaleString());
      intervalsCandidatesQuery.refetch();
    }
  }, [intervalsStatusQuery.data, intervalsCandidatesQuery.refetch]);

  const buttonDisabled = syncMutation.isPending || statusQuery.data?.running;
  const buttonLabel = useMemo(() => {
    if (syncMutation.isPending || statusQuery.data?.running) return "Синхронизируем…";
    return "Синхронизировать старые активности";
  }, [syncMutation.isPending, statusQuery.data?.running]);

  const candidates = stravaCandidatesQuery.data?.items ?? [];
  const straverConfigured = stravaCandidatesQuery.data?.straver_configured ?? true;

  const stravaButtonDisabled =
    !straverConfigured ||
    candidates.length === 0 ||
    selectedUsers.length === 0 ||
    stravaBackfill.isPending ||
    stravaStatusQuery.data?.running;
  const stravaButtonLabel = useMemo(() => {
    if (stravaBackfill.isPending || stravaStatusQuery.data?.running) return "Загружаем…";
    return "Загрузить выбранным";
  }, [stravaBackfill.isPending, stravaStatusQuery.data?.running]);

  function toggleUser(userId: number) {
    setSelectedUsers((prev) => (prev.includes(userId) ? prev.filter((id) => id !== userId) : [...prev, userId]));
  }

  function selectAllUsers() {
    const candidates = stravaCandidatesQuery.data?.items ?? [];
    setSelectedUsers(candidates.map((item) => item.tg_user_id));
  }

  function startStravaBackfill() {
    if (selectedUsers.length === 0) return;
    stravaBackfill.mutate({ tg_user_ids: selectedUsers });
  }

  const intervalsCandidates = intervalsCandidatesQuery.data?.items ?? [];
  const intervalsButtonDisabled =
    intervalsCandidates.length === 0 ||
    intervalsSelectedUsers.length === 0 ||
    intervalsBackfill.isPending ||
    intervalsStatusQuery.data?.running;
  const intervalsButtonLabel = useMemo(() => {
    if (intervalsBackfill.isPending || intervalsStatusQuery.data?.running) return "Загружаем…";
    return "Отправить выбранных";
  }, [intervalsBackfill.isPending, intervalsStatusQuery.data?.running]);

  function toggleIntervalsUser(userId: number) {
    setIntervalsSelectedUsers((prev) => (prev.includes(userId) ? prev.filter((id) => id !== userId) : [...prev, userId]));
  }

  function selectAllIntervalsUsers() {
    setIntervalsSelectedUsers(intervalsCandidates.map((item) => item.tg_user_id));
  }

  function startIntervalsBackfill() {
    if (intervalsSelectedUsers.length === 0) return;
    intervalsBackfill.mutate({ tg_user_ids: intervalsSelectedUsers });
  }

  function handleLegacyFileChange(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0] ?? null;
    setLegacyFile(file);
    setLegacyStatus("idle");
    setLegacySummary("");
    setLegacyErrors([]);
    setLegacyLog("");
  }

  function startLegacyImport() {
    if (!legacyFile) return;
    legacyImport.mutate(legacyFile);
  }

  function runNormalizeAccounts() {
    normalizeAccounts.mutate();
  }

  return (
    <>
      <Panel
        title="Синхронизация WattAttack"
        subtitle="Обновление старых активностей и архив FIT-файлов"
        headerExtra={
          <div className="sync-row">
            <button type="button" className="button" onClick={() => syncMutation.mutate()} disabled={buttonDisabled}>
              {buttonLabel}
            </button>
            <button type="button" className="button ghost" onClick={() => clearLogs.mutate()} disabled={clearLogs.isPending}>
              {clearLogs.isPending ? "Очищаем…" : "Очистить логи"}
            </button>
          </div>
        }
      >
        <div className="sync-content">
          <div className="sync-row">
            <p>
              Проходит по всем аккаунтам WattAttack, подтягивает пропущенные метаданные, сопоставляет по расписанию и
              скачивает FIT-файлы в архив.
            </p>
            <div className="sync-row">
              <div className={`sync-status sync-status--${status}`}>
                {status === "idle" && "Готово к запуску"}
                {status === "running" && "Синхронизация…"}
                {status === "done" && "Готово"}
                {status === "error" && "Ошибка"}
              </div>
              {progress && <div className="meta-hint">{progress}</div>}
            </div>
          </div>
          {lastRun && <div className="meta-hint">Последний запуск: {lastRun}</div>}
          {log ? <pre className="sync-log">{log}</pre> : <div className="empty-state">Лог появится тут.</div>}
        </div>
      </Panel>

      <Panel
        title="Дозагрузка в Strava"
        subtitle="Выберите пользователей со Strava, чтобы отправить им архивные FIT-файлы"
        headerExtra={
          <div className="sync-row">
            <button type="button" className="button" onClick={selectAllUsers} disabled={candidates.length === 0}>
              Выбрать всех
            </button>
            <button type="button" className="button" onClick={startStravaBackfill} disabled={stravaButtonDisabled}>
              {stravaButtonLabel}
            </button>
          </div>
        }
      >
        <div className="sync-content">
          {!straverConfigured && (
            <div className="form-error">Straver не настроен: проверьте STRAVER_BASE_URL и STRAVER_INTERNAL_SECRET.</div>
          )}
          <p>
            Берёт FIT-файлы из архива, находит активности без отметки Strava и загружает их от имени выбранных
            пользователей.
          </p>
          <div className="sync-row">
            <div className={`sync-status sync-status--${stravaStatus}`}>
              {stravaStatus === "idle" && "Готово к запуску"}
              {stravaStatus === "running" && "Загружаем…"}
              {stravaStatus === "done" && "Готово"}
              {stravaStatus === "error" && "Ошибка"}
            </div>
            {stravaProgress && <div className="meta-hint">{stravaProgress}</div>}
          </div>
          {stravaCandidatesQuery.isLoading ? (
            <div className="empty-state">Загружаем список Strava-связок…</div>
          ) : candidates.length === 0 ? (
            <div className="empty-state">
              {straverConfigured
                ? "Нет подключенных пользователей Strava."
                : "Straver недоступен, список пуст."}
            </div>
          ) : (
            <div className="strava-list">
              {candidates.map((item) => {
                const checked = selectedUsers.includes(item.tg_user_id);
                return (
                  <div className="strava-row" key={item.tg_user_id}>
                    <label className="strava-check">
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleUser(item.tg_user_id)}
                        aria-label={`Выбрать ${item.client_name || item.tg_user_id}`}
                      />
                      <span className="id-chip">#{item.client_id}</span>
                      <span className="strava-name">{item.client_name || "Без имени"}</span>
                    </label>
                    <div className="strava-meta">
                      <span className="strava-pill">
                        Strava: {item.strava_athlete_name ? item.strava_athlete_name : "подключена"}
                      </span>
                      <span className="strava-pill">К загрузке: {item.pending}</span>
                      <span className="strava-pill">FIT в архиве: {item.with_fit}</span>
                      {item.last_activity_at ? (
                        <span className="meta-hint">Последняя: {formatDateTime(item.last_activity_at)}</span>
                      ) : null}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          {stravaLastRun && <div className="meta-hint">Последняя дозагрузка: {stravaLastRun}</div>}
          {stravaLog ? <pre className="sync-log">{stravaLog}</pre> : <div className="empty-state">Лог появится тут.</div>}
        </div>
      </Panel>

      <Panel
        title="Дозагрузка в Intervals.icu"
        subtitle="Отправка архива FIT-файлов подключенным Intervals пользователям"
        headerExtra={
          <div className="sync-row">
            <button
              type="button"
              className="button"
              onClick={selectAllIntervalsUsers}
              disabled={intervalsCandidates.length === 0}
            >
              Выбрать всех
            </button>
            <button
              type="button"
              className="button"
              onClick={startIntervalsBackfill}
              disabled={intervalsButtonDisabled}
            >
              {intervalsButtonLabel}
            </button>
          </div>
        }
      >
        <div className="sync-content">
          <p>Берёт FIT-файлы из архива, ищет неотправленные в Intervals и загружает их выбранным пользователям.</p>
          <div className="sync-row">
            <div className={`sync-status sync-status--${intervalsStatus}`}>
              {intervalsStatus === "idle" && "Готово к запуску"}
              {intervalsStatus === "running" && "Загружаем…"}
              {intervalsStatus === "done" && "Готово"}
              {intervalsStatus === "error" && "Ошибка"}
            </div>
            {intervalsProgress && <div className="meta-hint">{intervalsProgress}</div>}
          </div>
          {intervalsCandidatesQuery.isLoading ? (
            <div className="empty-state">Загружаем список Intervals-связок…</div>
          ) : intervalsCandidates.length === 0 ? (
            <div className="empty-state">Нет подключенных пользователей Intervals.</div>
          ) : (
            <div className="strava-list">
              {intervalsCandidates.map((item) => {
                const checked = intervalsSelectedUsers.includes(item.tg_user_id);
                return (
                  <div className="strava-row" key={`intervals-${item.tg_user_id}`}>
                    <label className="strava-check">
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleIntervalsUser(item.tg_user_id)}
                        aria-label={`Выбрать ${item.client_name || item.tg_user_id}`}
                      />
                      <span className="id-chip">#{item.client_id}</span>
                      <span className="strava-name">{item.client_name || "Без имени"}</span>
                    </label>
                    <div className="strava-meta">
                      <span className="strava-pill">К загрузке: {item.pending}</span>
                      <span className="strava-pill">FIT в архиве: {item.with_fit}</span>
                      {item.last_activity_at ? (
                        <span className="meta-hint">Последняя: {formatDateTime(item.last_activity_at)}</span>
                      ) : null}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          {intervalsLastRun && <div className="meta-hint">Последняя дозагрузка: {intervalsLastRun}</div>}
          {intervalsLog ? (
            <pre className="sync-log">{intervalsLog}</pre>
          ) : (
            <div className="empty-state">Лог Intervals появится тут.</div>
          )}
        </div>
      </Panel>

      <Panel
        title="Импорт legacy истории"
        subtitle="Загрузите history_legacy.json (экспорт или массив объектов), чтобы добавить старые активности в расписание"
        headerExtra={
          <button type="button" className="button" onClick={() => setLegacyOpen((v) => !v)}>
            {legacyOpen ? "Скрыть" : "Показать"}
          </button>
        }
      >
        {legacyOpen && (
          <div className="sync-content">
            <p>
              Берём сообщения с файлами <code>activity_*.fit</code> из экспорта Telegram, вытаскиваем дату, атлета и
              метрики, а затем пытаемся привязать к клиенту по имени и времени тренировки.
            </p>
            <div className="sync-row">
              <input type="file" accept=".json" onChange={handleLegacyFileChange} />
              <button
                type="button"
                className="button"
                onClick={startLegacyImport}
                disabled={!legacyFile || legacyImport.isPending}
              >
                {legacyImport.isPending ? "Импортируем…" : "Импортировать файл"}
              </button>
              <div className={`sync-status sync-status--${legacyStatus}`}>
                {legacyStatus === "idle" && "Файл не выбран"}
                {legacyStatus === "running" && "Импорт…"}
                {legacyStatus === "done" && "Готово"}
                {legacyStatus === "error" && "Ошибка"}
              </div>
            </div>
            {legacySummary && <div className="meta-hint">{legacySummary}</div>}
            {legacyLog ? <pre className="sync-log">{legacyLog}</pre> : <div className="empty-state">Лог появится тут.</div>}
            {legacyErrors.length > 0 ? (
              <pre className="sync-log">{legacyErrors.join("\n")}</pre>
            ) : (
              <div className="empty-state">Ошибки появятся тут.</div>
            )}
            <div className="sync-row">
              <button type="button" className="button" onClick={runNormalizeAccounts} disabled={normalizeAccounts.isPending}>
                {normalizeAccounts.isPending ? "Переименовываем…" : "Нормализовать аккаунты krutilkavn → krutilka_"}
              </button>
              <div className={`sync-status sync-status--${normalizeStatus}`}>
                {normalizeStatus === "idle" && "Готово"}
                {normalizeStatus === "running" && "В работе…"}
                {normalizeStatus === "done" && "Готово"}
                {normalizeStatus === "error" && "Ошибка"}
              </div>
            </div>
            {normalizeSummary && <div className="meta-hint">{normalizeSummary}</div>}
            {normalizeErrors.length > 0 ? (
              <pre className="sync-log">{normalizeErrors.join("\n")}</pre>
            ) : (
              <div className="empty-state">Ошибки нормализации появятся тут.</div>
            )}
          </div>
        )}
      </Panel>
    </>
  );
}
