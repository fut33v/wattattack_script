import { NavLink, useLocation } from "react-router-dom";
import type { ReactNode } from "react";
import classNames from "classnames";

import type { SessionResponse } from "../lib/types";

interface AppShellProps {
  session: SessionResponse;
  children: ReactNode;
  hideSidebar?: boolean;
}

interface NavLinkConfig {
  to: string;
  label: string;
  adminOnly: boolean;
}

const PRIMARY_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/dashboard", label: "Панель", adminOnly: false },
  { to: "/clients", label: "Клиенты", adminOnly: false },
  { to: "/schedule/manage", label: "Расписание", adminOnly: true },
  { to: "/races", label: "Гонки", adminOnly: true },
  { to: "/schedule/settings", label: "Настройки", adminOnly: true },
  { to: "/instructors", label: "Инструкторы", adminOnly: true },
  { to: "/admins", label: "Админы", adminOnly: true }
] as const;

const MESSAGE_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/messaging", label: "Рассылка", adminOnly: true },
  { to: "/messages", label: "Сообщения", adminOnly: true }
] as const;

const SERVICE_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/schedule/notifications", label: "Уведомления", adminOnly: true },
  { to: "/activities", label: "Активности", adminOnly: true },
  { to: "/sync", label: "Синхронизация", adminOnly: true },
  { to: "/links", label: "Связки", adminOnly: true }
] as const;

const TECH_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/bikes", label: "Велосипеды", adminOnly: false },
  { to: "/trainers", label: "Тренажеры", adminOnly: false }
] as const;

export default function AppShell({ session, children, hideSidebar = false }: AppShellProps) {
  const location = useLocation();
  const isAdmin = session.isAdmin;

  const filteredPrimaryLinks = PRIMARY_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredMessageLinks = MESSAGE_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredServiceLinks = SERVICE_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredTechLinks = TECH_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));

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
            {filteredPrimaryLinks.map((link) => (
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
            {filteredMessageLinks.length > 0 && (
              <div className="nav-section">
                <div className="nav-section-title">Сообщения</div>
                {filteredMessageLinks.map((link) => (
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
              </div>
            )}
            {filteredTechLinks.length > 0 && (
              <div className="nav-section">
                <div className="nav-section-title">Технический</div>
                {filteredTechLinks.map((link) => (
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
              </div>
            )}
            {filteredServiceLinks.length > 0 && (
              <div className="nav-section">
                <div className="nav-section-title">Сервисный</div>
                {filteredServiceLinks.map((link) => (
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
              </div>
            )}
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
