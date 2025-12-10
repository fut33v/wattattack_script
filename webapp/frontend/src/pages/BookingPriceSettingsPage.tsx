import { FormEvent, useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";

interface BookingPriceSettings {
  price_instructor_rub: number;
  price_self_service_rub: number;
}

export default function BookingPriceSettingsPage() {
  const queryClient = useQueryClient();
  const [priceInstructor, setPriceInstructor] = useState(700);
  const [priceSelfService, setPriceSelfService] = useState(500);
  const [isSaving, setIsSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const settingsQuery = useQuery<{ settings: BookingPriceSettings }>({
    queryKey: ["booking-price-settings"],
    queryFn: () => apiFetch<{ settings: BookingPriceSettings }>("/api/schedule/booking-settings")
  });

  useEffect(() => {
    if (settingsQuery.data?.settings) {
      setPriceInstructor(settingsQuery.data.settings.price_instructor_rub);
      setPriceSelfService(settingsQuery.data.settings.price_self_service_rub);
    }
  }, [settingsQuery.data]);

  const updateMutation = useMutation({
    mutationFn: (data: { price_instructor_rub: number; price_self_service_rub: number }) =>
      apiFetch<{ settings: BookingPriceSettings }>("/api/schedule/booking-settings", {
        method: "POST",
        body: JSON.stringify(data)
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["booking-price-settings"] });
      setSaveSuccess(true);
      setSaveError(null);
      setTimeout(() => setSaveSuccess(false), 3000);
    },
    onError: (error: any) => {
      setSaveError(error.message || "Не удалось сохранить цены");
      setSaveSuccess(false);
    },
    onSettled: () => {
      setIsSaving(false);
    }
  });

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSaving(true);
    updateMutation.mutate({
      price_instructor_rub: priceInstructor,
      price_self_service_rub: priceSelfService
    });
  }

  return (
    <Panel title="Стоимость тренировок" subtitle="Настройка цен для самокрутки и занятий с инструктором">
      {settingsQuery.isLoading ? (
        <div className="empty-state">Загружаем цены…</div>
      ) : settingsQuery.isError ? (
        <div className="form-message error">Не удалось загрузить цены. Попробуйте обновить страницу.</div>
      ) : (
        <div className="notification-settings-form">
          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label htmlFor="priceSelfService">
                Самокрутка
                <div className="form-hint">Сколько стоит самостоятельная тренировка</div>
              </label>
              <div className="input-with-unit">
                <input
                  type="number"
                  id="priceSelfService"
                  min="100"
                  step="50"
                  value={priceSelfService}
                  onChange={(e) => setPriceSelfService(Number(e.target.value))}
                  disabled={isSaving}
                />
                <span className="unit">₽</span>
              </div>
            </div>

            <div className="form-group">
              <label htmlFor="priceInstructor">
                С инструктором
                <div className="form-hint">Цена занятия, когда слот ведёт инструктор</div>
              </label>
              <div className="input-with-unit">
                <input
                  type="number"
                  id="priceInstructor"
                  min="100"
                  step="50"
                  value={priceInstructor}
                  onChange={(e) => setPriceInstructor(Number(e.target.value))}
                  disabled={isSaving}
                />
                <span className="unit">₽</span>
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

              {saveSuccess && <div className="form-message success">Цены обновлены</div>}
              {saveError && <div className="form-message error">{saveError}</div>}
            </div>
          </form>

          <div className="settings-info">
            <h3>Как используется</h3>
            <p>
              Эти цены показываем клиенту сразу после записи в ботах. Для самокрутки подставляем цену самокрутки,
              для слотов с инструктором — цену тренировки с инструктором.
            </p>
            <p>Изменения вступают в силу мгновенно и применяются ко всем новым записям.</p>
          </div>
        </div>
      )}
    </Panel>
  );
}
