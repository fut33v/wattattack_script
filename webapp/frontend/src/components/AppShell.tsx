import { NavLink, useLocation } from "react-router-dom";
import type { ReactNode } from "react";
import classNames from "classnames";

import type { SessionResponse } from "../lib/types";

interface AppShellProps {
  session: SessionResponse;
  children: ReactNode;
  hideSidebar?: boolean;
}

const NAV_LINKS = [
  { to: "/dashboard", label: "Панель", adminOnly: false },
  { to: "/clients", label: "Клиенты", adminOnly: false },
  { to: "/schedule", label: "Расписание", adminOnly: false },
  { to: "/schedule/manage", label: "Редактор", adminOnly: true },
  { to: "/schedule/notifications", label: "Уведомления", adminOnly: true },
  { to: "/schedule/settings", label: "Настройки", adminOnly: true },
  { to: "/instructors", label: "Инструкторы", adminOnly: true },
  { to: "/bikes", label: "Велосипеды", adminOnly: false },
  { to: "/trainers", label: "Тренажеры", adminOnly: false },
  { to: "/links", label: "Связки", adminOnly: true },
  { to: "/admins", label: "Админы", adminOnly: true }
] as const;

export default function AppShell({ session, children, hideSidebar = false }: AppShellProps) {
  const location = useLocation();
  const isAdmin = session.isAdmin;

  const filteredLinks = NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));

  async function handleLogout() {
    await fetch("/api/logout", { method: "GET", credentials: "include" });
    window.location.href = "/app";
  }

  const shellClass = classNames("app-shell", { "app-shell--full": hideSidebar });
  const mainClass = classNames("main-area", { "main-area--full": hideSidebar });

  return (
    <div className={shellClass}>
      {!hideSidebar && (
        <aside className="sidebar">
          <div className="brand">
            <span className="brand-accent" />
            <div>
              <div className="brand-title">Крутилка</div>
              <div className="brand-sub">Admin Portal</div>
            </div>
          </div>
          <nav className="nav">
            {filteredLinks.map((link) => (
              <NavLink
                key={link.to}
                to={link.to}
                className={({ isActive }) =>
                  classNames("nav-link", {
                    active: isActive || location.pathname === link.to
                  })
                }
              >
                {link.label}
              </NavLink>
            ))}
          </nav>
          <div className="sidebar-footer">
            <div className="user-card">
              <div className="avatar">
                {session.user.photo_url ? (
                  <img src={session.user.photo_url} alt={session.user.display_name ?? "user"} />
                ) : (
                  <span>{(session.user.display_name ?? "?").slice(0, 1)}</span>
                )}
              </div>
              <div>
                <div className="user-name">{session.user.display_name ?? session.user.username ?? session.user.id}</div>
                <div className="user-meta">{session.isAdmin ? "Администратор" : "Пользователь"}</div>
              </div>
            </div>
            <button className="logout-button" onClick={handleLogout}>
              Выйти
            </button>
          </div>
        </aside>
      )}
      <main className={mainClass}>
        {!hideSidebar && (
          <header className="main-header">
            <h1>Крутилка Admin</h1>
            <div className="main-meta">
              <span>Управление базой клиентов и инвентарем</span>
            </div>
          </header>
        )}
        <div className="main-content">{children}</div>
      </main>
    </div>
  );
}
