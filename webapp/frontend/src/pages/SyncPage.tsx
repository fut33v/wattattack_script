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

export default function SyncPage() {
  const [log, setLog] = useState<string>("");
  const [status, setStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [lastRun, setLastRun] = useState<string>("");
  const [progress, setProgress] = useState<string>("");
  const [currentAccount, setCurrentAccount] = useState<string>("");

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
    onSuccess: (data) => {
      setStatus("running"); // switch to polling state
      setLastRun(new Date().toLocaleString());
      statusQuery.refetch();
    },
    onError: (error: any) => {
      setStatus("error");
      setLog(`Ошибка: ${error?.message ?? "неизвестно"}`);
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

  const buttonDisabled = syncMutation.isPending || statusQuery.data?.running;
  const buttonLabel = useMemo(() => {
    if (syncMutation.isPending || statusQuery.data?.running) return "Синхронизируем…";
    return "Синхронизировать старые активности";
  }, [syncMutation.isPending, statusQuery.data?.running]);

  return (
    <Panel
      title="Синхронизация"
      subtitle="Обновление старых активностей и архив FIT-файлов"
      headerExtra={
        <button
          type="button"
          className="button"
          onClick={() => syncMutation.mutate()}
          disabled={buttonDisabled}
        >
          {buttonLabel}
        </button>
      }
    >
      <div className="sync-content">
        <div className="sync-row">
          <p>
            Проходит по всем аккаунтам WattAttack, подтягивает пропущенные метаданные, сопоставляет по
            расписанию и скачивает FIT-файлы в архив.
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
  );
}
