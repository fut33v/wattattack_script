import { FormEvent, useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";

interface NotificationSettings {
  reminder_hours: number;
}

export default function NotificationSettingsPage() {
  const queryClient = useQueryClient();
  const [reminderHours, setReminderHours] = useState(4);
  const [isSaving, setIsSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const settingsQuery = useQuery<{ settings: NotificationSettings }>({
    queryKey: ["notification-settings"],
    queryFn: () => apiFetch<{ settings: NotificationSettings }>("/api/schedule/notification-settings")
  });

  useEffect(() => {
    if (settingsQuery.data?.settings?.reminder_hours) {
      setReminderHours(settingsQuery.data.settings.reminder_hours);
    }
  }, [settingsQuery.data]);

  const updateMutation = useMutation({
    mutationFn: (data: { reminder_hours: number }) =>
      apiFetch<{ settings: NotificationSettings }>("/api/schedule/notification-settings", {
        method: "POST",
        body: JSON.stringify(data)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["notification-settings"] });
      setSaveSuccess(true);
      setSaveError(null);
      setTimeout(() => setSaveSuccess(false), 3000);
    },
    onError: (error: any) => {
      setSaveError(error.message || "Не удалось сохранить настройки");
      setSaveSuccess(false);
    },
    onSettled: () => {
      setIsSaving(false);
    }
  });

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSaving(true);
    updateMutation.mutate({ reminder_hours: reminderHours });
  }

  return (
    <Panel title="Настройки уведомлений" subtitle="Настройка времени отправки уведомлений о тренировках">
      {settingsQuery.isLoading ? (
        <div className="empty-state">Загружаем настройки…</div>
      ) : (
        <div className="notification-settings-form">
          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label htmlFor="reminderHours">
                Время уведомления
                <div className="form-hint">
                  За сколько часов до тренировки отправлять уведомление клиенту
                </div>
              </label>
              <div className="input-with-unit">
                <input
                  type="number"
                  id="reminderHours"
                  min="1"
                  max="168"
                  value={reminderHours}
                  onChange={(e) => setReminderHours(Number(e.target.value))}
                  disabled={isSaving}
                />
                <span className="unit">часов</span>
              </div>
            </div>
            
            <div className="form-actions">
              <button 
                type="submit" 
                className="button primary"
                disabled={isSaving || updateMutation.isPending}
              >
                {isSaving || updateMutation.isPending ? "Сохранение…" : "Сохранить"}
              </button>
              
              {saveSuccess && (
                <div className="form-message success">
                  Настройки успешно сохранены!
                </div>
              )}
              
              {saveError && (
                <div className="form-message error">
                  {saveError}
                </div>
              )}
            </div>
          </form>
          
          <div className="settings-info">
            <h3>Информация</h3>
            <p>
              Уведомления отправляются клиентам через бот <strong>clientbot</strong> за указанное 
              количество часов до начала тренировки.
            </p>
            <p>
              Система отслеживает отправленные уведомления, чтобы избежать дублирования.
            </p>
          </div>
        </div>
      )}
    </Panel>
  );
}