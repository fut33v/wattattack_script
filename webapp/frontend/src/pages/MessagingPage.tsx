import { FormEvent, useEffect, useMemo, useState, type ChangeEvent, type ClipboardEvent } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import Panel from "../components/Panel";
import { ApiError, apiFetch } from "../lib/api";
import { useConfig } from "../lib/hooks";
import type {
  ClientLinkListResponse,
  RaceListResponse,
  RaceDetailResponse,
  VkClientLinkListResponse
} from "../lib/types";

import "../styles/messaging.css";

interface BroadcastResponse {
  sent: number;
  failed: number;
  total: number;
  message: string;
  errors?: string[];
  sentTelegram?: number;
  sentVk?: number;
}

interface MessagingFilters {
  sendAt?: string;
  clientIds?: number[];
  raceId?: number;
  useMarkdownV2?: boolean;
  imageUrl?: string;
  filterRaceUnpaid?: boolean;
  filterGender?: "male" | "female" | "unknown";
  filterNoBookingToday?: boolean;
  filterNoBookingTomorrow?: boolean;
  filterHasBookingToday?: boolean;
  filterHasBookingTomorrow?: boolean;
  filterBookingDate?: string;
  filterSlotId?: number;
}

interface BookingFilterResponse {
  todayIds: number[];
  tomorrowIds: number[];
  dateIds?: number[];
  dateLabel?: string;
  slotIds?: number[];
  slotLabel?: string;
}

interface BookingSlotOption {
  id: number;
  slot_date: string;
  start_time: string;
  end_time: string;
  label?: string | null;
  week_start_date?: string | null;
  instructor_name?: string | null;
}

type BookingIncludeMode = "none" | "today" | "tomorrow" | "date" | "slot";

type RecipientLink = {
  client_id: number;
  client_name?: string | null;
  gender?: string | null;
  tg_user_id?: number | null;
  tg_username?: string | null;
  tg_full_name?: string | null;
  vk_user_id?: number | null;
  vk_username?: string | null;
  vk_full_name?: string | null;
  is_blocked?: boolean | null;
  hasTelegram: boolean;
  hasVk: boolean;
};

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
  const [raceUnpaidOnly, setRaceUnpaidOnly] = useState(false);
  const [raceFilterOpen, setRaceFilterOpen] = useState(false);
  const [bookingFilterOpen, setBookingFilterOpen] = useState(false);
  const [genderFilterOpen, setGenderFilterOpen] = useState(false);
  const [imageFile, setImageFile] = useState<File | null>(null);
  const [imageUrl, setImageUrl] = useState("");
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const [filterNoBookingToday, setFilterNoBookingToday] = useState(false);
  const [filterNoBookingTomorrow, setFilterNoBookingTomorrow] = useState(false);
  const [bookingIncludeMode, setBookingIncludeMode] = useState<BookingIncludeMode>("none");
  const [bookingDate, setBookingDate] = useState("");
  const [bookingSlotId, setBookingSlotId] = useState<number | null>(null);
  const [genderFilter, setGenderFilter] = useState<"all" | "male" | "female" | "unknown">("all");
  const [useMarkdownV2, setUseMarkdownV2] = useState(false);
  const [includeBlocked, setIncludeBlocked] = useState(false);
  const [sendTelegram, setSendTelegram] = useState(true);
  const [sendVk, setSendVk] = useState(false);
  const [vkToggleTouched, setVkToggleTouched] = useState(false);

  const configQuery = useConfig();
  const vkBroadcastEnabled = configQuery.data?.vkBroadcastEnabled ?? false;

  const linksQuery = useQuery({
    queryKey: ["client-links"],
    queryFn: () => apiFetch<ClientLinkListResponse>("/api/client-links"),
    staleTime: 60000 // 1 minute
  });

  const vkLinksQuery = useQuery({
    queryKey: ["vk-client-links"],
    queryFn: () => apiFetch<VkClientLinkListResponse>("/api/vk-client-links"),
    staleTime: 60000
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
    queryKey: [
      "booking-filters",
      bookingIncludeMode === "date" ? bookingDate : null,
      bookingIncludeMode === "slot" ? bookingSlotId : null
    ],
    queryFn: () => {
      const params = new URLSearchParams();
      if (bookingIncludeMode === "date" && bookingDate) {
        params.set("filter_date", bookingDate);
      }
      if (bookingIncludeMode === "slot" && bookingSlotId) {
        params.set("slot_id", String(bookingSlotId));
      }
      const suffix = params.toString() ? `?${params.toString()}` : "";
      return apiFetch<BookingFilterResponse>(`/api/messages/booking-filters${suffix}`);
    },
    staleTime: 60000
  });

  const bookingSlotsQuery = useQuery({
    queryKey: ["booking-slots"],
    queryFn: () => apiFetch<{ items: BookingSlotOption[] }>("/api/messages/booking-slots"),
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
      setSendError(formatSendError(error));
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

    if (!sendTelegram && !sendVkActive) {
      setSendError("Выберите хотя бы один канал отправки");
      return;
    }

    setIsSending(true);
    setSendResult(null);
    setSendError(null);

    const data: MessagingFilters & { message: string; sendTelegram: boolean; sendVk: boolean } = {
      message: message.trim(),
      sendTelegram,
      sendVk: sendVkActive
    };

    if (isScheduled && scheduledTime) {
      data.sendAt = new Date(scheduledTime).toISOString();
    }

    if (selectedClientIds.length > 0) {
      data.clientIds = selectedClientIds;
    }

    if (selectedRaceId) {
      data.raceId = selectedRaceId;
      if (raceUnpaidOnly) {
        data.filterRaceUnpaid = true;
      }
    }

    if (filterNoBookingToday) {
      data.filterNoBookingToday = true;
    }
    if (filterNoBookingTomorrow) {
      data.filterNoBookingTomorrow = true;
    }

    if (bookingIncludeMode === "today") {
      data.filterHasBookingToday = true;
    }
    if (bookingIncludeMode === "tomorrow") {
      data.filterHasBookingTomorrow = true;
    }
    if (bookingIncludeMode === "date" && bookingDate) {
      data.filterBookingDate = bookingDate;
    }
    if (bookingIncludeMode === "slot" && bookingSlotId) {
      data.filterSlotId = bookingSlotId;
    }
    if (useMarkdownV2) {
      data.useMarkdownV2 = true;
    }
    if (genderFilter !== "all") {
      data.filterGender = genderFilter;
    }
    if (includeBlocked) {
      // backend still attempts sends; UI filter controls inclusion
    }

    const shouldSendAsForm = Boolean(imageFile || imageUrl);

    if (shouldSendAsForm) {
      const formData = new FormData();
      formData.append("message", data.message);
      if (data.sendAt) formData.append("sendAt", data.sendAt);
      if (data.clientIds) formData.append("clientIds", JSON.stringify(data.clientIds));
      if (data.raceId) formData.append("raceId", String(data.raceId));
      if (data.filterRaceUnpaid) formData.append("filterRaceUnpaid", "true");
      if (data.filterNoBookingToday) formData.append("filterNoBookingToday", "true");
      if (data.filterNoBookingTomorrow) formData.append("filterNoBookingTomorrow", "true");
      if (data.filterHasBookingToday) formData.append("filterHasBookingToday", "true");
      if (data.filterHasBookingTomorrow) formData.append("filterHasBookingTomorrow", "true");
      if (data.filterBookingDate) formData.append("filterBookingDate", data.filterBookingDate);
      if (data.filterSlotId) formData.append("filterSlotId", String(data.filterSlotId));
      if (data.filterGender) formData.append("filterGender", data.filterGender);
      if (data.useMarkdownV2) formData.append("markdownV2", "true");
      formData.append("sendTelegram", sendTelegram ? "true" : "false");
      formData.append("sendVk", sendVkActive ? "true" : "false");
      if (imageFile) formData.append("image", imageFile);
      if (imageUrl) formData.append("imageUrl", imageUrl.trim());
      broadcastMutation.mutate(formData);
      return;
    }

    broadcastMutation.mutate(data);
  }

  function formatSendError(error: unknown) {
    if (error instanceof ApiError) {
      const detail =
        typeof error.body === "string"
          ? error.body
          : (error.body as any)?.detail || (error.body as any)?.message || error.message;
      return `Ошибка отправки (${error.status}): ${detail}`;
    }
    if (error instanceof Error) {
      return `Ошибка отправки: ${error.message}`;
    }
    return "Не удалось отправить сообщение";
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

  function matchesSearch(link: RecipientLink, normalized: string) {
    const clientName = (link.client_name || link.tg_full_name || link.vk_full_name || "").toLowerCase();
    const usernameParts = [link.tg_username, link.vk_username]
      .filter(Boolean)
      .map((value) => String(value).toLowerCase());
    const idValue = String(link.client_id);
    return (
      clientName.includes(normalized) ||
      usernameParts.some((part) => part?.includes(normalized)) ||
      idValue.includes(normalized)
    );
  }

  function formatClientLabel(link: RecipientLink) {
    const name = link.client_name || link.tg_full_name || link.vk_full_name || "Без имени";
    const usernames: string[] = [];
    if (link.tg_username) usernames.push(`@${link.tg_username}`);
    if (link.vk_username) usernames.push(`vk.com/${link.vk_username}`);
    const channels: string[] = [];
    if (link.hasTelegram) channels.push("TG");
    if (link.hasVk) channels.push("VK");
    const details = [...usernames, channels.length ? channels.join("/") : null].filter(Boolean).join(" · ");
    return details ? `${name} · ${details}` : name;
  }

  function toggleClient(clientId: number) {
    setSelectedClientIds((prev) =>
      prev.includes(clientId) ? prev.filter((id) => id !== clientId) : [...prev, clientId]
    );
  }

  function handleSelectAllClients() {
    if (deliverableRecipients.length === 0) return;
    const allIds = deliverableRecipients.map((item) => item.client_id);
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

  function formatSlotLabel(slot: BookingSlotOption) {
    const base = `${slot.slot_date} · ${slot.start_time}-${slot.end_time}`;
    const extras = [slot.label, slot.instructor_name].filter(Boolean);
    return extras.length > 0 ? `${base} (${extras.join(" · ")})` : base;
  }

  function normalizeGender(value: string | null | undefined) {
    return (value ?? "").trim().toLowerCase();
  }

  function formatGenderFilterLabel(value: "all" | "male" | "female" | "unknown") {
    switch (value) {
      case "male":
        return "мужчины";
      case "female":
        return "женщины";
      case "unknown":
        return "без указания пола";
      default:
        return "все";
    }
  }

  const links = linksQuery.data?.items ?? [];
  const vkLinks = vkLinksQuery.data?.items ?? [];
  const normalizedSearch = clientSearch.trim().toLowerCase();

  const recipients = useMemo<RecipientLink[]>(() => {
    const map = new Map<number, RecipientLink>();

    links.forEach((link) => {
      const id = link.client_id;
      const existing = map.get(id);
      const baseName = link.client_name || link.tg_full_name;
      if (existing) {
        existing.hasTelegram = existing.hasTelegram || Boolean(link.tg_user_id);
        existing.tg_user_id = existing.tg_user_id ?? link.tg_user_id;
        existing.tg_username = existing.tg_username ?? link.tg_username;
        existing.tg_full_name = existing.tg_full_name ?? link.tg_full_name;
        existing.client_name = existing.client_name || baseName;
        existing.gender = existing.gender || link.gender;
        existing.is_blocked = existing.is_blocked ?? link.is_blocked;
      } else {
        map.set(id, {
          client_id: id,
          client_name: baseName,
          gender: link.gender,
          tg_user_id: link.tg_user_id,
          tg_username: link.tg_username,
          tg_full_name: link.tg_full_name,
          vk_user_id: null,
          vk_username: null,
          vk_full_name: null,
          is_blocked: link.is_blocked,
          hasTelegram: Boolean(link.tg_user_id),
          hasVk: false
        });
      }
    });

    vkLinks.forEach((link) => {
      const id = link.client_id;
      const existing = map.get(id);
      const baseName = link.client_name || link.vk_full_name;
      if (existing) {
        existing.hasVk = existing.hasVk || Boolean(link.vk_user_id);
        existing.vk_user_id = existing.vk_user_id ?? link.vk_user_id;
        existing.vk_username = existing.vk_username ?? link.vk_username;
        existing.vk_full_name = existing.vk_full_name ?? link.vk_full_name;
        existing.client_name = existing.client_name || baseName;
        existing.gender = existing.gender || link.gender;
      } else {
        map.set(id, {
          client_id: id,
          client_name: baseName,
          gender: link.gender,
          tg_user_id: null,
          tg_username: null,
          tg_full_name: null,
          vk_user_id: link.vk_user_id,
          vk_username: link.vk_username,
          vk_full_name: link.vk_full_name,
          is_blocked: false,
          hasTelegram: false,
          hasVk: Boolean(link.vk_user_id)
        });
      }
    });

    return Array.from(map.values()).sort((a, b) => {
      const aName = (a.client_name || a.tg_full_name || a.vk_full_name || "").toLowerCase();
      const bName = (b.client_name || b.tg_full_name || b.vk_full_name || "").toLowerCase();
      if (aName && bName) return aName.localeCompare(bName);
      if (aName) return -1;
      if (bName) return 1;
      return a.client_id - b.client_id;
    });
  }, [links, vkLinks]);

  const tgLinkedCount = links.length;
  const vkLinkedCount = vkLinks.length;

  const races = racesQuery.data?.items ?? [];
  const sortedRaces = useMemo(() => {
    return [...races].sort((a, b) => new Date(b.race_date).getTime() - new Date(a.race_date).getTime());
  }, [races]);
  const selectedRace = sortedRaces.find((race) => race.id === selectedRaceId) || null;
  const raceFilterActive = Boolean(selectedRaceId);
  const raceFilterLoaded = raceDetailQuery.isSuccess;

  const raceParticipantIds = useMemo(() => {
    const registrations = raceDetailQuery.data?.item?.registrations || [];
    const allowedStatuses = raceUnpaidOnly ? ["pending"] : ["approved", "pending"];
    return new Set(
      registrations
        .filter((reg) => {
          const status = (reg.status || "").toLowerCase();
          return allowedStatuses.includes(status);
        })
        .map((reg) => reg.client_id)
        .filter((id): id is number => Boolean(id))
    );
  }, [raceDetailQuery.data, raceUnpaidOnly]);

  const bookedToday = useMemo(() => new Set(bookingFiltersQuery.data?.todayIds ?? []), [bookingFiltersQuery.data]);
  const bookedTomorrow = useMemo(() => new Set(bookingFiltersQuery.data?.tomorrowIds ?? []), [bookingFiltersQuery.data]);
  const bookedOnDate = useMemo(() => new Set(bookingFiltersQuery.data?.dateIds ?? []), [bookingFiltersQuery.data]);
  const bookedOnSlot = useMemo(() => new Set(bookingFiltersQuery.data?.slotIds ?? []), [bookingFiltersQuery.data]);

  const bookingIncludeSet = useMemo<Set<number> | null>(() => {
    if (bookingIncludeMode === "today") return bookedToday;
    if (bookingIncludeMode === "tomorrow") return bookedTomorrow;
    if (bookingIncludeMode === "date") {
      if (!bookingDate || bookingFiltersQuery.data?.dateIds === undefined) return null;
      return bookedOnDate;
    }
    if (bookingIncludeMode === "slot") {
      if (!bookingSlotId || bookingFiltersQuery.data?.slotIds === undefined) return null;
      return bookedOnSlot;
    }
    return null;
  }, [bookingIncludeMode, bookingDate, bookingSlotId, bookedToday, bookedTomorrow, bookedOnDate, bookedOnSlot, bookingFiltersQuery.data]);

  const bookingSummaryLabel = useMemo(() => {
    switch (bookingIncludeMode) {
      case "today":
        return "с бронью сегодня";
      case "tomorrow":
        return "с бронью завтра";
      case "date":
        return bookingDate ? `с бронью на ${bookingDate}` : "выберите дату";
      case "slot": {
        if (!bookingSlotId) return "выберите слот";
        const option = bookingSlotsQuery.data?.items?.find((item) => item.id === bookingSlotId);
        return option ? `слот ${formatSlotLabel(option)}` : "выберите слот";
      }
      default:
        return "";
    }
  }, [bookingIncludeMode, bookingDate, bookingSlotId, bookingSlotsQuery.data]);

  const bookingSummary = useMemo(() => {
    const parts: string[] = [];
    if (bookingIncludeMode !== "none") {
      parts.push(bookingSummaryLabel || "фильтр по брони");
    }
    if (filterNoBookingToday) parts.push("без брони сегодня");
    if (filterNoBookingTomorrow) parts.push("без брони завтра");
    return parts.length ? parts.join(" · ") : "(опционально)";
  }, [bookingIncludeMode, bookingSummaryLabel, filterNoBookingToday, filterNoBookingTomorrow]);

  const bookingFilterInvalid =
    (bookingIncludeMode === "date" && !bookingDate) || (bookingIncludeMode === "slot" && !bookingSlotId);
  const bookingFilterPending =
    (bookingIncludeMode === "date" && Boolean(bookingDate) && bookingFiltersQuery.isFetching) ||
    (bookingIncludeMode === "slot" && Boolean(bookingSlotId) && bookingFiltersQuery.isFetching);

  const genderFilterLabel = formatGenderFilterLabel(genderFilter);
  const genderSummary = genderFilter === "all" ? "(любой)" : genderFilterLabel;
  const genderHint = genderFilter !== "all" ? ` — пол: ${genderFilterLabel}` : "";

  const sendVkActive = sendVk && vkBroadcastEnabled;
  const recipientsLoading = linksQuery.isLoading || vkLinksQuery.isLoading;

  const filteredRecipients = useMemo(() => {
    let scoped = recipients;
    const applyRaceFilter = raceFilterActive && raceFilterLoaded && raceParticipantIds.size > 0;
    if (applyRaceFilter) {
      scoped = scoped.filter((link) => raceParticipantIds.has(link.client_id));
    }

    if (genderFilter !== "all") {
      scoped = scoped.filter((link) => {
        const normalized = normalizeGender(link.gender);
        if (genderFilter === "unknown") {
          return normalized !== "male" && normalized !== "female";
        }
        return normalized === genderFilter;
      });
    }

    if (!includeBlocked) {
      scoped = scoped.filter((link) => !link.is_blocked);
    }

    if (bookingIncludeSet) {
      scoped = scoped.filter((link) => bookingIncludeSet.has(link.client_id));
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
    recipients,
    normalizedSearch,
    raceFilterActive,
    raceFilterLoaded,
    raceParticipantIds,
    genderFilter,
    bookingIncludeSet,
    filterNoBookingToday,
    filterNoBookingTomorrow,
    bookedToday,
    bookedTomorrow
  ]);

  const deliverableRecipients = useMemo(
    () =>
      filteredRecipients.filter((link) => {
        const viaTelegram = sendTelegram && link.hasTelegram;
        const viaVk = sendVkActive && link.hasVk;
        return viaTelegram || viaVk;
      }),
    [filteredRecipients, sendTelegram, sendVkActive]
  );

  const linkedUsersCount = deliverableRecipients.length;
  const selectedCount = selectedClientIds.length > 0 ? selectedClientIds.length : deliverableRecipients.length;

  const activeRecipientFilters = useMemo(() => {
    const filters: string[] = [];
    if (raceFilterActive && selectedRace) {
      filters.push(
        `гонка: ${formatRaceLabel(selectedRace.title, selectedRace.race_date)}${
          raceUnpaidOnly ? " (только не оплатившие)" : ""
        }`
      );
    }
    if (bookingIncludeMode !== "none") {
      if (bookingSummaryLabel) {
        filters.push(bookingSummaryLabel);
      } else {
        filters.push("фильтр по брони");
      }
    }
    if (filterNoBookingToday) {
      filters.push("без брони сегодня");
    }
    if (filterNoBookingTomorrow) {
      filters.push("без брони завтра");
    }
    if (genderFilter !== "all") {
      filters.push(`пол: ${genderFilterLabel}`);
    }
    if (!includeBlocked) {
      filters.push("без заблокированных");
    }
    return filters;
  }, [
    raceFilterActive,
    selectedRace,
    bookingIncludeMode,
    bookingSummaryLabel,
    filterNoBookingToday,
    filterNoBookingTomorrow,
    genderFilter,
    genderFilterLabel,
    includeBlocked
  ]);

  useEffect(() => {
    setSelectedClientIds((prev) => prev.filter((id) => deliverableRecipients.some((link) => link.client_id === id)));
  }, [deliverableRecipients]);

  useEffect(() => {
    if (!vkBroadcastEnabled) {
      setSendVk(false);
      return;
    }
    if (!vkToggleTouched && (vkLinksQuery.data?.items?.length ?? 0) > 0) {
      setSendVk(true);
    }
  }, [vkBroadcastEnabled, vkLinksQuery.data, vkToggleTouched]);

  useEffect(() => {
    if (!selectedRaceId) {
      setRaceUnpaidOnly(false);
    }
  }, [selectedRaceId]);

  useEffect(() => {
    if (bookingIncludeMode !== "none") {
      setFilterNoBookingToday(false);
      setFilterNoBookingTomorrow(false);
    }
  }, [bookingIncludeMode]);

  return (
    <Panel title="Рассылка сообщений" subtitle="Отправка сообщений всем пользователям через clientbot">
      <div className="messaging-page">
        <div className="messaging-stats">
          <div className="stat-card">
            <div className="stat-label">Получатели по фильтрам</div>
            <div className="stat-value">{linkedUsersCount}</div>
            <div className="stat-hint form-hint">С учетом выбранных каналов</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Telegram</div>
            <div className="stat-value">{tgLinkedCount}</div>
            <div className="stat-hint form-hint">{sendTelegram ? "включено" : "выключено"}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">VK</div>
            <div className="stat-value">{vkLinkedCount}</div>
            <div className="stat-hint form-hint">
              {vkBroadcastEnabled ? (sendVkActive ? "включено" : "выключено") : "токен не настроен"}
            </div>
          </div>
        </div>

        <form className="messaging-form" onSubmit={handleSubmit}>
          <details className="form-group collapsible" open={raceFilterOpen} onToggle={(e) => setRaceFilterOpen(e.currentTarget.open)}>
            <summary>
              Фильтр по гонке
              <span className="summary-hint">
                {raceFilterActive && selectedRace
                  ? `Активен: ${formatRaceLabel(selectedRace.title, selectedRace.race_date)}${
                      raceUnpaidOnly ? " · только не оплатившие" : ""
                    }`
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
                  {raceUnpaidOnly ? " · только не оплатившие" : ""}
                </div>
              )}
              {raceFilterActive && (
                <label className="checkbox-row">
                  <input
                    type="checkbox"
                    checked={raceUnpaidOnly}
                    onChange={(e) => setRaceUnpaidOnly(e.target.checked)}
                    disabled={isSending || raceDetailQuery.isLoading}
                  />
                  <span>Только не оплатившие (статус pending)</span>
                </label>
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
                {bookingSummary}
              </span>
            </summary>
            <div className="collapsible-content">
              <div className="form-hint">Фильтруем получателей по наличию или отсутствию брони.</div>
              <div className="filter-section">
                <div className="filter-section-title">Исключить с бронью</div>
                <div className="filter-buttons">
                  <button
                    type="button"
                    className={`button toggle ${filterNoBookingToday ? "active" : ""}`}
                    onClick={() => setFilterNoBookingToday((prev) => !prev)}
                    aria-pressed={filterNoBookingToday}
                    disabled={isSending || bookingIncludeMode !== "none"}
                  >
                    Без брони сегодня
                  </button>
                  <button
                    type="button"
                    className={`button toggle ${filterNoBookingTomorrow ? "active" : ""}`}
                    onClick={() => setFilterNoBookingTomorrow((prev) => !prev)}
                    aria-pressed={filterNoBookingTomorrow}
                    disabled={isSending || bookingIncludeMode !== "none"}
                  >
                    Без брони завтра
                  </button>
                </div>
                {bookingIncludeMode !== "none" && (
                  <div className="form-hint">Отключено, потому что выбран режим "с бронью" ниже.</div>
                )}
              </div>

              <div className="filter-section">
                <div className="filter-section-title">Отправить только тем, у кого есть бронь</div>
                <div className="filter-grid">
                  <label className="radio-row">
                    <input
                      type="radio"
                      name="bookingInclude"
                      checked={bookingIncludeMode === "none"}
                      onChange={() => setBookingIncludeMode("none")}
                      disabled={isSending}
                    />
                    <span>Не учитывать брони</span>
                  </label>
                  <label className="radio-row">
                    <input
                      type="radio"
                      name="bookingInclude"
                      checked={bookingIncludeMode === "today"}
                      onChange={() => setBookingIncludeMode("today")}
                      disabled={isSending}
                    />
                    <span>Есть бронь сегодня</span>
                  </label>
                  <label className="radio-row">
                    <input
                      type="radio"
                      name="bookingInclude"
                      checked={bookingIncludeMode === "tomorrow"}
                      onChange={() => setBookingIncludeMode("tomorrow")}
                      disabled={isSending}
                    />
                    <span>Есть бронь завтра</span>
                  </label>
                  <label className="radio-row">
                    <input
                      type="radio"
                      name="bookingInclude"
                      checked={bookingIncludeMode === "date"}
                      onChange={() => setBookingIncludeMode("date")}
                      disabled={isSending}
                    />
                    <span>Есть бронь в выбранный день</span>
                  </label>
                  <input
                    type="date"
                    value={bookingDate}
                    onChange={(e) => {
                      setBookingDate(e.target.value);
                      setBookingIncludeMode("date");
                    }}
                    disabled={isSending || bookingIncludeMode !== "date"}
                  />
                  <label className="radio-row">
                    <input
                      type="radio"
                      name="bookingInclude"
                      checked={bookingIncludeMode === "slot"}
                      onChange={() => setBookingIncludeMode("slot")}
                      disabled={isSending}
                    />
                    <span>Есть бронь в конкретном слоте</span>
                  </label>
                  <select
                    value={bookingSlotId ?? ""}
                    onChange={(e) => {
                      setBookingSlotId(e.target.value ? Number(e.target.value) : null);
                      setBookingIncludeMode("slot");
                    }}
                    disabled={isSending || bookingIncludeMode !== "slot" || bookingSlotsQuery.isLoading}
                  >
                    <option value="">Выберите слот</option>
                    {(bookingSlotsQuery.data?.items ?? []).map((slot) => (
                      <option key={slot.id} value={slot.id}>
                        {formatSlotLabel(slot)}
                      </option>
                    ))}
                  </select>
                  {bookingSlotsQuery.isError && (
                    <div className="form-message error">Не удалось загрузить список слотов.</div>
                  )}
                </div>
                <div className="form-hint">
                  Включаем клиентов, у которых есть бронь в выбранные дни или слот.
                  {bookingSlotsQuery.isLoading ? " Загружаем слоты..." : ""}
                  {bookingIncludeMode === "date" && !bookingDate ? " Укажите дату, чтобы применить фильтр." : ""}
                </div>
              </div>
            </div>
          </details>

          <details
            className="form-group collapsible"
            open={genderFilterOpen}
            onToggle={(e) => setGenderFilterOpen(e.currentTarget.open)}
          >
            <summary>
              Фильтр по полу
              <span className="summary-hint">{genderSummary}</span>
            </summary>
            <div className="collapsible-content">
              <div className="form-hint">Сузьте рассылку по полу клиента.</div>
              <div className="filter-buttons">
                {[
                  { value: "all", label: "Все" },
                  { value: "male", label: "Мужчины" },
                  { value: "female", label: "Женщины" },
                  { value: "unknown", label: "Без указания" }
                ].map((option) => (
                  <button
                    key={option.value}
                    type="button"
                    className={`button toggle ${genderFilter === option.value ? "active" : ""}`}
                    onClick={() => setGenderFilter(option.value as typeof genderFilter)}
                    aria-pressed={genderFilter === option.value}
                    disabled={isSending}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
              <div className="form-hint">Данные берутся из карточки клиента.</div>
            </div>
          </details>

          <div className="form-group">
            <label className="checkbox-row">
              <input
                type="checkbox"
                checked={includeBlocked}
                onChange={(e) => setIncludeBlocked(e.target.checked)}
                disabled={isSending}
              />
              <span>Включать заблокированных бота (обычно не нужно)</span>
            </label>
            {!includeBlocked && <div className="form-hint">По умолчанию исключаем клиентов с флагом “bot blocked”.</div>}
          </div>

          <div className="form-group">
            <label>Каналы отправки</label>
            <div className="channel-toggles">
              <label className="checkbox-row">
                <input
                  type="checkbox"
                  checked={sendTelegram}
                  onChange={(e) => setSendTelegram(e.target.checked)}
                  disabled={isSending}
                />
                <span>Telegram</span>
              </label>
              <label className="checkbox-row">
                <input
                  type="checkbox"
                  checked={sendVkActive}
                  onChange={(e) => {
                    setVkToggleTouched(true);
                    setSendVk(e.target.checked);
                  }}
                  disabled={isSending || !vkBroadcastEnabled || vkLinkedCount === 0}
                />
                <span>ВКонтакте {vkBroadcastEnabled ? "" : "(недоступно)"}</span>
              </label>
            </div>
            <div className="form-hint">
              {sendVkActive
                ? "Сообщение уйдет и в Telegram, и в VK (если клиент подключен)."
                : vkBroadcastEnabled
                  ? "Отправка в VK выключена."
                  : "Добавьте VK токен, чтобы включить отправку."}
            </div>
          </div>

          <div className="form-group">
            <label>Получатели</label>
            <div className="recipient-actions">
              <div className="recipient-summary">
                {selectedClientIds.length > 0
                  ? `Выбрано ${selectedClientIds.length} из ${deliverableRecipients.length}`
                  : deliverableRecipients.length === 0
                    ? "Нет получателей"
                    : `Без выбора — всем ${deliverableRecipients.length} пользователям`}
                {raceFilterActive && selectedRace ? ` · Гонка: ${formatRaceLabel(selectedRace.title, selectedRace.race_date)}` : ""}
                {genderFilter !== "all" ? ` · Пол: ${genderFilterLabel}` : ""}
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
              disabled={isSending || recipientsLoading}
              className="recipient-search"
            />
            <div className="recipient-list">
              {recipientsLoading ? (
                <div className="recipient-empty">Загрузка подключенных пользователей...</div>
              ) : deliverableRecipients.length === 0 ? (
                <div className="recipient-empty">Не нашлись клиенты по фильтру или каналам</div>
              ) : (
                deliverableRecipients.map((link) => {
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
                        <div className="recipient-sub">
                          ID {link.client_id}
                          {link.hasTelegram && <span className="pill">TG</span>}
                          {link.hasVk && <span className="pill">VK</span>}
                          {link.is_blocked && <span className="pill pill-danger">Бот заблокирован</span>}
                        </div>
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
                  ? `Сообщение получат ${selectedCount} выбранных клиентов${raceFilterActive ? " (участники выбранной гонки)" : ""}${bookingIncludeMode !== "none" ? ` — ${bookingSummaryLabel || "фильтр по брони"}` : ""}${genderHint}`
                  : `Сообщение будет отправлено всем ${deliverableRecipients.length} получателям${raceFilterActive ? " — участникам гонки" : ""}${bookingIncludeMode !== "none" ? ` — ${bookingSummaryLabel || "фильтр по брони"}` : ""}${genderHint}`}
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

            <label className="checkbox-row">
              <input
                type="checkbox"
                checked={useMarkdownV2}
                onChange={(e) => setUseMarkdownV2(e.target.checked)}
                disabled={isSending}
              />
              <span>
                Использовать MarkdownV2 (Telegram)
                <div className="form-hint">Переключает parse_mode; текст должен быть корректно экранирован под MarkdownV2.</div>
              </span>
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
              disabled={
                isSending ||
                broadcastMutation.isPending ||
                linkedUsersCount === 0 ||
                (!sendTelegram && !sendVkActive) ||
                bookingFilterPending ||
                bookingFilterInvalid
              }
            >
              {isSending || broadcastMutation.isPending ? "Отправка..." : "Отправить сообщение"}
            </button>

            {bookingFilterPending && <div className="form-hint">Обновляем список клиентов с бронью…</div>}
            {bookingFilterInvalid && (
              <div className="form-message error">Заполните дату или слот, чтобы применить фильтр по брони.</div>
            )}
            
            {sendResult && (
              <div className="form-message success">
                <div>{sendResult.message}</div>
                {sendResult.errors && sendResult.errors.length > 0 && (
                  <div className="form-hint">
                    Детали: {sendResult.errors.join(" | ")}
                  </div>
                )}
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
            <li>Сообщения отправляются через бота для записи в Крутилку и в VK (при включенной опции)</li>
            <li>Получатели берутся из связок клиента с Telegram/VK; фильтры и выбор применяются одинаково</li>
            <li>Отправка по расписанию будет реализована в следующих версиях</li>
            </ul>
          </div>
        </div>
    </Panel>
  );
}
