import { useEffect, useMemo, useState } from "react";
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

  return (
    <>
      <Panel
        title="Синхронизация WattAttack"
        subtitle="Обновление старых активностей и архив FIT-файлов"
        headerExtra={
          <button type="button" className="button" onClick={() => syncMutation.mutate()} disabled={buttonDisabled}>
            {buttonLabel}
          </button>
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
    </>
  );
}
