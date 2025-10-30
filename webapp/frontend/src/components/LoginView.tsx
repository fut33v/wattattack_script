import { useEffect, useRef } from "react";
import { useLocation } from "react-router-dom";

import type { ConfigResponse } from "../lib/types";

interface LoginViewProps {
  config: ConfigResponse;
}

function getQueryParam(locationSearch: string, key: string): string | null {
  const params = new URLSearchParams(locationSearch);
  return params.get(key);
}

export default function LoginView({ config }: LoginViewProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const location = useLocation();
  const error = getQueryParam(location.search, "error");

  useEffect(() => {
    if (!config.loginBotUsername) return;

    const existing = containerRef.current;
    if (!existing) return;

    existing.innerHTML = "";

    const script = document.createElement("script");
    script.src = "https://telegram.org/js/telegram-widget.js?22";
    script.async = true;
    script.setAttribute("data-telegram-login", config.loginBotUsername);
    script.setAttribute("data-size", "large");
    script.setAttribute("data-userpic", "false");
    script.setAttribute("data-request-access", "write");
    const authUrl = `${window.location.origin}/auth/telegram?next=${encodeURIComponent("/app")}`;
    script.setAttribute("data-auth-url", authUrl);
    existing.appendChild(script);

    return () => {
      existing.innerHTML = "";
    };
  }, [config.loginBotUsername]);

  return (
    <div className="login-screen">
      <div className="login-card">
        <h1>Крутилка Admin</h1>
        <p>Войдите через Telegram, чтобы продолжить.</p>
        <div ref={containerRef} className="login-widget" />
        {error && <div className="login-error">{error}</div>}
      </div>
    </div>
  );
}
