import { FormEvent, useEffect, useMemo, useState, type ChangeEvent, type ClipboardEvent } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";
import type {
  ClientLinkListResponse,
  ClientLinkRow,
  RaceListResponse,
  RaceDetailResponse
} from "../lib/types";

import "../styles/messaging.css";

interface BroadcastResponse {
  sent: number;
  failed: number;
  total: number;
  message: string;
}

interface MessagingFilters {
  sendAt?: string;
  clientIds?: number[];
  raceId?: number;
  imageUrl?: string;
  filterNoBookingToday?: boolean;
  filterNoBookingTomorrow?: boolean;
}

interface BookingFilterResponse {
  todayIds: number[];
  tomorrowIds: number[];
}

export default function MessagingPage() {
  const [message, setMessage] = useState("");
  const [isScheduled, setIsScheduled] = useState(false);
  const [scheduledTime, setScheduledTime] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [sendResult, setSendResult] = useState<BroadcastResponse | null>(null);
  const [sendError, setSendError] = useState<string | null>(null);
  const [selectedClientIds, setSelectedClientIds] = useState<number[]>([]);
  const [clientSearch, setClientSearch] = useState("");
  const [selectedRaceId, setSelectedRaceId] = useState<number | null>(null);
  const [raceFilterOpen, setRaceFilterOpen] = useState(false);
  const [bookingFilterOpen, setBookingFilterOpen] = useState(false);
  const [imageFile, setImageFile] = useState<File | null>(null);
  const [imageUrl, setImageUrl] = useState("");
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const [filterNoBookingToday, setFilterNoBookingToday] = useState(false);
  const [filterNoBookingTomorrow, setFilterNoBookingTomorrow] = useState(false);

  const linksQuery = useQuery({
    queryKey: ["client-links"],
    queryFn: () => apiFetch<ClientLinkListResponse>("/api/client-links"),
    staleTime: 60000 // 1 minute
  });

  const racesQuery = useQuery({
    queryKey: ["races"],
    queryFn: () => apiFetch<RaceListResponse>("/api/races"),
    staleTime: 60000
  });

  const raceDetailQuery = useQuery({
    queryKey: ["race-detail", selectedRaceId],
    queryFn: () => apiFetch<RaceDetailResponse>(`/api/races/${selectedRaceId}`),
    enabled: Boolean(selectedRaceId),
    staleTime: 30000
  });

  const bookingFiltersQuery = useQuery({
    queryKey: ["booking-filters"],
    queryFn: () => apiFetch<BookingFilterResponse>("/api/messages/booking-filters"),
    staleTime: 60000
  });

  const broadcastMutation = useMutation({
    mutationFn: (
      data:
        | FormData
        | { message: string; sendAt?: string; clientIds?: number[]; raceId?: number; imageUrl?: string }
    ) =>
      apiFetch<BroadcastResponse>("/api/messages/broadcast", {
        method: "POST",
        body: data as any
      }),
    onSuccess: (data) => {
      setSendResult(data);
      setSendError(null);
      setMessage("");
      setImageFile(null);
      setImageUrl("");
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

    const hasText = Boolean(message.trim());
    const hasImage = Boolean(imageFile) || Boolean(imageUrl.trim());
    if (!hasText && !hasImage) {
      setSendError("Добавьте текст сообщения или изображение");
      return;
    }

    setIsSending(true);
    setSendResult(null);
    setSendError(null);

    const data: MessagingFilters & { message: string } = {
      message: message.trim()
    };

    if (isScheduled && scheduledTime) {
      data.sendAt = new Date(scheduledTime).toISOString();
    }

    if (selectedClientIds.length > 0) {
      data.clientIds = selectedClientIds;
    }

    if (selectedRaceId) {
      data.raceId = selectedRaceId;
    }

    if (filterNoBookingToday) {
      data.filterNoBookingToday = true;
    }
    if (filterNoBookingTomorrow) {
      data.filterNoBookingTomorrow = true;
    }

    const shouldSendAsForm = Boolean(imageFile || imageUrl);

    if (shouldSendAsForm) {
      const formData = new FormData();
      formData.append("message", data.message);
      if (data.sendAt) formData.append("sendAt", data.sendAt);
      if (data.clientIds) formData.append("clientIds", JSON.stringify(data.clientIds));
      if (data.raceId) formData.append("raceId", String(data.raceId));
      if (imageFile) formData.append("image", imageFile);
      if (imageUrl) formData.append("imageUrl", imageUrl.trim());
      broadcastMutation.mutate(formData);
      return;
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

  function matchesSearch(link: ClientLinkRow, normalized: string) {
    const clientName = (link.client_name || link.tg_full_name || "").toLowerCase();
    const username = (link.tg_username || "").toLowerCase();
    const idValue = String(link.client_id);
    return clientName.includes(normalized) || username.includes(normalized) || idValue.includes(normalized);
  }

  function formatClientLabel(link: ClientLinkRow) {
    const name = link.client_name || link.tg_full_name || "Без имени";
    const username = link.tg_username ? `@${link.tg_username}` : null;
    return [name, username].filter(Boolean).join(" · ");
  }

  function toggleClient(clientId: number) {
    setSelectedClientIds((prev) =>
      prev.includes(clientId) ? prev.filter((id) => id !== clientId) : [...prev, clientId]
    );
  }

  function handleSelectAllClients() {
    if (!linksQuery.data?.items) return;
    const allIds = linksQuery.data.items.map((item) => item.client_id);
    setSelectedClientIds(allIds);
  }

  function handleClearSelection() {
    setSelectedClientIds([]);
  }

  function handleFileChange(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0] ?? null;
    setImageFile(file);
    if (file) {
      setImageUrl("");
    }
  }

  function handleClearFile() {
    setImageFile(null);
  }

  function handleImageUrlChange(event: ChangeEvent<HTMLInputElement>) {
    const value = event.target.value;
    setImageUrl(value);
    if (value) {
      setImageFile(null);
    }
  }

  function handlePasteImage(event: ClipboardEvent<HTMLTextAreaElement>) {
    const items = event.clipboardData?.items;
    if (!items) return;

    for (const item of items) {
      if (item.kind === "file") {
        const file = item.getAsFile();
        if (file) {
          setImageFile(file);
          setImageUrl("");
          setSendError(null);
        }
        break;
      }
    }
  }

  useEffect(() => {
    if (imageFile) {
      const nextUrl = URL.createObjectURL(imageFile);
      setImagePreview(nextUrl);
      return () => URL.revokeObjectURL(nextUrl);
    }
    if (imageUrl.trim()) {
      setImagePreview(imageUrl.trim());
      return;
    }
    setImagePreview(null);
  }, [imageFile, imageUrl]);

  function formatRaceLabel(title: string, dateIso: string) {
    const dateLabel = formatRaceDate(dateIso);
    return dateLabel ? `${title} — ${dateLabel}` : title;
  }

  function formatRaceDate(dateIso: string) {
    if (!dateIso) return "";
    const parsed = new Date(dateIso);
    return Number.isNaN(parsed.getTime()) ? dateIso : parsed.toLocaleDateString("ru-RU");
  }

  const links = linksQuery.data?.items ?? [];
  const linkedUsersCount = links.length;
  const normalizedSearch = clientSearch.trim().toLowerCase();

  const races = racesQuery.data?.items ?? [];
  const sortedRaces = useMemo(() => {
    return [...races].sort((a, b) => new Date(b.race_date).getTime() - new Date(a.race_date).getTime());
  }, [races]);
  const selectedRace = sortedRaces.find((race) => race.id === selectedRaceId) || null;
  const raceFilterActive = Boolean(selectedRaceId);
  const raceFilterLoaded = raceDetailQuery.isSuccess;

  const raceParticipantIds = useMemo(() => {
    const registrations = raceDetailQuery.data?.item?.registrations || [];
    return new Set(
      registrations
        .filter((reg) => {
          const status = (reg.status || "").toLowerCase();
          return status === "approved" || status === "pending";
        })
        .map((reg) => reg.client_id)
        .filter((id): id is number => Boolean(id))
    );
  }, [raceDetailQuery.data]);

  const bookedToday = useMemo(() => new Set(bookingFiltersQuery.data?.todayIds ?? []), [bookingFiltersQuery.data]);
  const bookedTomorrow = useMemo(() => new Set(bookingFiltersQuery.data?.tomorrowIds ?? []), [bookingFiltersQuery.data]);

  const filteredLinks = useMemo(() => {
    let scoped = links;
    const applyRaceFilter = raceFilterActive && raceFilterLoaded && raceParticipantIds.size > 0;
    if (applyRaceFilter) {
      scoped = scoped.filter((link) => raceParticipantIds.has(link.client_id));
    }

    if (filterNoBookingToday || filterNoBookingTomorrow) {
      scoped = scoped.filter((link) => {
        const id = link.client_id;
        if (filterNoBookingToday && bookedToday.has(id)) return false;
        if (filterNoBookingTomorrow && bookedTomorrow.has(id)) return false;
        return true;
      });
    }

    if (!normalizedSearch) return scoped;
    return scoped.filter((link) => matchesSearch(link, normalizedSearch));
  }, [
    links,
    normalizedSearch,
    raceFilterActive,
    raceFilterLoaded,
    raceParticipantIds,
    filterNoBookingToday,
    filterNoBookingTomorrow,
    bookedToday,
    bookedTomorrow
  ]);

  const selectedCount = selectedClientIds.length > 0 ? selectedClientIds.length : filteredLinks.length;

  const activeRecipientFilters = useMemo(() => {
    const filters: string[] = [];
    if (raceFilterActive && selectedRace) {
      filters.push(`гонка: ${formatRaceLabel(selectedRace.title, selectedRace.race_date)}`);
    }
    if (filterNoBookingToday) {
      filters.push("без брони сегодня");
    }
    if (filterNoBookingTomorrow) {
      filters.push("без брони завтра");
    }
    return filters;
  }, [raceFilterActive, selectedRace, filterNoBookingToday, filterNoBookingTomorrow]);

  useEffect(() => {
    setSelectedClientIds((prev) => prev.filter((id) => filteredLinks.some((link) => link.client_id === id)));
  }, [filteredLinks]);

  return (
    <Panel title="Рассылка сообщений" subtitle="Отправка сообщений всем пользователям через clientbot">
      <div className="messaging-page">
        <div className="messaging-stats">
          <div className="stat-card">
            <div className="stat-label">Подключенные пользователи</div>
            <div className="stat-value">{linkedUsersCount}</div>
          </div>
        </div>

        <form className="messaging-form" onSubmit={handleSubmit}>
          <details className="form-group collapsible" open={raceFilterOpen} onToggle={(e) => setRaceFilterOpen(e.currentTarget.open)}>
            <summary>
              Фильтр по гонке
              <span className="summary-hint">
                {raceFilterActive && selectedRace
                  ? `Активен: ${formatRaceLabel(selectedRace.title, selectedRace.race_date)}`
                  : "(опционально)"}
              </span>
            </summary>
            <div className="collapsible-content">
              <div className="form-hint">
                Выберите гонку, чтобы отправить сообщение только участникам (pending/approved). Совмещается с выбором клиентов.
              </div>
              <select
                id="raceFilter"
                value={selectedRaceId ?? ""}
                onChange={(e) => setSelectedRaceId(e.target.value ? Number(e.target.value) : null)}
                disabled={isSending || racesQuery.isLoading}
              >
                <option value="">Без фильтра по гонке</option>
                {sortedRaces.map((race) => (
                  <option key={race.id} value={race.id}>
                    {formatRaceLabel(race.title, race.race_date)}
                  </option>
                ))}
              </select>
              {racesQuery.isLoading && <div className="form-hint">Загрузка списка гонок...</div>}
              {racesQuery.isError && <div className="form-message error">Не удалось загрузить гонки</div>}
              {raceFilterActive && selectedRace && (
                <div className="form-hint">
                  Активен фильтр гонки: {formatRaceLabel(selectedRace.title, selectedRace.race_date)}
                </div>
              )}
            </div>
          </details>

          <details
            className="form-group collapsible"
            open={bookingFilterOpen}
            onToggle={(e) => setBookingFilterOpen(e.currentTarget.open)}
          >
            <summary>
              Фильтр по брони
              <span className="summary-hint">
                {filterNoBookingToday || filterNoBookingTomorrow
                  ? [
                      filterNoBookingToday ? "без брони сегодня" : null,
                      filterNoBookingTomorrow ? "без брони завтра" : null
                    ]
                      .filter(Boolean)
                      .join(" · ")
                  : "(опционально)"}
              </span>
            </summary>
            <div className="collapsible-content">
              <div className="form-hint">
                Исключаем клиентов, у которых уже есть брони в расписании на выбранные дни.
              </div>
              <div className="filter-buttons">
                <button
                  type="button"
                  className={`button toggle ${filterNoBookingToday ? "active" : ""}`}
                  onClick={() => setFilterNoBookingToday((prev) => !prev)}
                  aria-pressed={filterNoBookingToday}
                  disabled={isSending}
                >
                  Без брони сегодня
                </button>
                <button
                  type="button"
                  className={`button toggle ${filterNoBookingTomorrow ? "active" : ""}`}
                  onClick={() => setFilterNoBookingTomorrow((prev) => !prev)}
                  aria-pressed={filterNoBookingTomorrow}
                  disabled={isSending}
                >
                  Без брони завтра
                </button>
              </div>
            </div>
          </details>

          <div className="form-group">
            <label>Получатели</label>
            <div className="recipient-actions">
              <div className="recipient-summary">
                {selectedClientIds.length > 0
                  ? `Выбрано ${selectedClientIds.length} из ${filteredLinks.length}`
                  : filteredLinks.length === 0
                    ? "Нет получателей"
                    : `Без выбора — всем ${filteredLinks.length} пользователям`}
                {raceFilterActive && selectedRace ? ` · Гонка: ${formatRaceLabel(selectedRace.title, selectedRace.race_date)}` : ""}
              </div>
              {activeRecipientFilters.length > 0 && (
                <div className="filter-badges">
                  {activeRecipientFilters.map((label) => (
                    <span className="filter-badge" key={label}>{label}</span>
                  ))}
                </div>
              )}
              <div className="recipient-buttons">
                <button
                  type="button"
                  className="button ghost"
                  onClick={handleSelectAllClients}
                  disabled={isSending || linkedUsersCount === 0}
                >
                  Выбрать всех
                </button>
                <button
                  type="button"
                  className="button ghost"
                  onClick={handleClearSelection}
                  disabled={isSending || selectedClientIds.length === 0}
                >
                  Сбросить выбор
                </button>
              </div>
            </div>
            <input
              type="search"
              placeholder="Поиск по клиенту или username"
              value={clientSearch}
              onChange={(e) => setClientSearch(e.target.value)}
              disabled={isSending || linksQuery.isLoading}
              className="recipient-search"
            />
            <div className="recipient-list">
              {linksQuery.isLoading ? (
                <div className="recipient-empty">Загрузка подключенных пользователей...</div>
              ) : filteredLinks.length === 0 ? (
                <div className="recipient-empty">Не нашлись клиенты по фильтру</div>
              ) : (
                filteredLinks.map((link) => {
                  const isSelected = selectedClientIds.includes(link.client_id);
                  return (
                    <label key={link.client_id} className="recipient-row">
                      <input
                        type="checkbox"
                        checked={isSelected}
                        onChange={() => toggleClient(link.client_id)}
                        disabled={isSending}
                      />
                      <div className="recipient-meta">
                        <div className="recipient-name">{formatClientLabel(link)}</div>
                        <div className="recipient-sub">ID {link.client_id}</div>
                      </div>
                    </label>
                  );
                })
              )}
          </div>
            <div className="form-hint">
              Если ничего не выбрано, сообщение получат все показанные ниже получатели.
              {" "}
              {raceFilterActive ? "Фильтр по гонке сужает список до участников." : ""}
            </div>
          </div>

          <div className="form-group">
            <label>Изображение (опционально)</label>
            <div className="upload-row">
              <input
                type="file"
                accept="image/*"
                onChange={handleFileChange}
                disabled={isSending}
              />
              {imageFile && (
                <div className="file-chip">
                  <span className="file-name">{imageFile.name}</span>
                  <button type="button" className="chip-close" onClick={handleClearFile} disabled={isSending}>
                    ×
                  </button>
                </div>
              )}
            </div>
            <input
              type="url"
              placeholder="URL картинки для Telegram"
              value={imageUrl}
              onChange={handleImageUrlChange}
              disabled={isSending}
            />
            <div className="form-hint">
              Telegram должен иметь доступ к файлу по URL. При вводе URL выбранный файл сбрасывается и наоборот. Можно вставить
              изображение прямо из буфера (Ctrl/Cmd+V) в поле сообщения.
            </div>
            {imagePreview && (
              <div className="image-preview">
                <img src={imagePreview} alt="Превью отправляемого изображения" />
                <button type="button" className="chip-close" onClick={handleClearFile} disabled={isSending}>
                  ×
                </button>
              </div>
            )}
          </div>

          <div className="form-group">
            <label htmlFor="message">
              Текст сообщения
              <div className="form-hint">
                {selectedClientIds.length > 0
                  ? `Сообщение получат ${selectedCount} выбранных клиентов${raceFilterActive ? ' (участники выбранной гонки)' : ''}`
                  : `Сообщение будет отправлено всем ${filteredLinks.length} получателям${raceFilterActive ? ' — участникам гонки' : ''}`}
              </div>
            </label>
            <textarea
              id="message"
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              onPaste={handlePasteImage}
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
                disabled
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
            <li>Сообщения отправляются через бота для записи в Крутилку</li>
            <li>Пользователи получают сообщения как личные сообщения в Telegram</li>
            <li>Отправка по расписанию будет реализована в следующих версиях</li>
            </ul>
          </div>
        </div>
    </Panel>
  );
}
