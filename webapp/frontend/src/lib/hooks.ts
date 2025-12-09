import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "./api";
import type { ConfigResponse, SessionResponse } from "./types";

export function useConfig() {
  return useQuery<ConfigResponse>({
    queryKey: ["config"],
    queryFn: () => apiFetch<ConfigResponse>("/api/config"),
    staleTime: Infinity
  });
}

export function useSession() {
  return useQuery<SessionResponse>({
    queryKey: ["session"],
    queryFn: () => apiFetch<SessionResponse>("/api/session"),
    retry: false
  });
}

export function usePaginatedQuery<T extends { pagination: { totalPages: number } }>(
  queryKey: (string | number)[],
  page: number,
  fetcher: () => Promise<T>,
  setPage: (value: number) => void
) {
  const query = useQuery<T>({ queryKey: [...queryKey, page], queryFn: fetcher, placeholderData: (prev) => prev });
  const pagination = query.data?.pagination;
  const itemsLength = (query.data as any)?.items?.length ?? 0;

  useEffect(() => {
    if (pagination && page > 1 && itemsLength === 0 && !query.isFetching) {
      setPage(Math.max(page - 1, 1));
    }
  }, [itemsLength, pagination, page, query.isFetching, setPage]);

  return query;
}
