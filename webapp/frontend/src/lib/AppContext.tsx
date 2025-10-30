import { createContext, useContext } from "react";
import type { ReactNode } from "react";
import type { ConfigResponse, SessionResponse } from "./types";

export interface AppContextValue {
  session: SessionResponse;
  config: ConfigResponse;
}

const AppContext = createContext<AppContextValue | undefined>(undefined);

export function AppContextProvider({ value, children }: { value: AppContextValue; children: ReactNode }) {
  return <AppContext.Provider value={value}>{children}</AppContext.Provider>;
}

export function useAppContext() {
  const value = useContext(AppContext);
  if (!value) {
    throw new Error("AppContext is not available");
  }
  return value;
}
