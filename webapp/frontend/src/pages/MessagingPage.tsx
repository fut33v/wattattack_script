import { FormEvent, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";

import "../styles/messaging.css";

interface BroadcastResponse {
  sent: number;
  failed: number;
  total: number;
  message: string;
}

export default function MessagingPage() {
  const [message, setMessage] = useState("");
  const [isScheduled, setIsScheduled] = useState(false);
  const [scheduledTime, setScheduledTime] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [sendResult, setSendResult] = useState<BroadcastResponse | null>(null);
  const [sendError, setSendError] = useState<string | null>(null);

  const linksQuery = useQuery({
    queryKey: ["client-links"],
    queryFn: () => apiFetch<any>("/api/client-links"),
    staleTime: 60000 // 1 minute
  });

  const broadcastMutation = useMutation({
    mutationFn: (data: { message: string; sendAt?: string }) =>
      apiFetch<BroadcastResponse>("/api/messages/broadcast", {
        method: "POST",
        body: JSON.stringify(data)
      }),
    onSuccess: (data) => {
      setSendResult(data);
      setSendError(null);
      setMessage("");
    },
    onError: (error: any) => {
      setSendError(error.message || "Не удалось отправить сообщение");
      setSendResult(null);
    },
    onSettled: () => {
      setIsSending(false);
    }
  });

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    
    if (!message.trim()) {
      setSendError("Введите текст сообщения");
      return;
    }
    
    setIsSending(true);
    setSendResult(null);
    setSendError(null);
    
    const data: { message: string; sendAt?: string } = { message: message.trim() };
    
    if (isScheduled && scheduledTime) {
      data.sendAt = new Date(scheduledTime).toISOString();
    }
    
    broadcastMutation.mutate(data);
  }

  function formatScheduledTime() {
    if (!scheduledTime) return "";
    try {
      const date = new Date(scheduledTime);
      return date.toLocaleString("ru-RU");
    } catch {
      return scheduledTime;
    }
  }

  const linkedUsersCount = linksQuery.data?.items?.length || 0;

  return (
    <Panel title="Рассылка сообщений" subtitle="Отправка сообщений всем пользователям через krutilkavnbot">
      <div className="messaging-page">
        <div className="messaging-stats">
          <div className="stat-card">
            <div className="stat-label">Подключенные пользователи</div>
            <div className="stat-value">{linkedUsersCount}</div>
          </div>
        </div>

        <form className="messaging-form" onSubmit={handleSubmit}>
          <div className="form-group">
            <label htmlFor="message">
              Текст сообщения
              <div className="form-hint">
                Сообщение будет отправлено всем {linkedUsersCount} подключенным пользователям
              </div>
            </label>
            <textarea
              id="message"
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              placeholder="Введите текст сообщения..."
              rows={6}
              disabled={isSending}
              maxLength={4096}
            />
            <div className="char-count">
              {message.length}/4096
            </div>
          </div>

          <div className="form-group">
            <label>
              <input
                type="checkbox"
                checked={isScheduled}
                onChange={(e) => setIsScheduled(e.target.checked)}
                disabled={isSending}
              />
              Отправить по расписанию
            </label>
            
            {isScheduled && (
              <div className="scheduled-time-input">
                <label htmlFor="scheduledTime">Время отправки</label>
                <input
                  type="datetime-local"
                  id="scheduledTime"
                  value={scheduledTime}
                  onChange={(e) => setScheduledTime(e.target.value)}
                  disabled={isSending}
                  min={new Date().toISOString().slice(0, 16)}
                />
                {scheduledTime && (
                  <div className="scheduled-preview">
                    Будет отправлено: {formatScheduledTime()}
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="form-actions">
            <button
              type="submit"
              className="button primary"
              disabled={isSending || broadcastMutation.isPending || linkedUsersCount === 0}
            >
              {isSending || broadcastMutation.isPending ? "Отправка..." : "Отправить сообщение"}
            </button>
            
            {sendResult && (
              <div className="form-message success">
                {sendResult.message}
              </div>
            )}
            
            {sendError && (
              <div className="form-message error">
                {sendError}
              </div>
            )}
          </div>
        </form>

        <div className="messaging-info">
          <h3>Информация</h3>
          <ul>
            <li>Сообщения отправляются через бот <strong>krutilkavnbot</strong></li>
            <li>Пользователи получают сообщения как личные сообщения в Telegram</li>
            <li>Отправка по расписанию будет реализована в следующих версиях</li>
          </ul>
        </div>
      </div>
    </Panel>
  );
}