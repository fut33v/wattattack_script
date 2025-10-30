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
