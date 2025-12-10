import { NavLink, useLocation } from "react-router-dom";
import { useEffect, useState } from "react";
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
  external?: boolean;
}

const PRIMARY_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/dashboard", label: "🏠 Панель", adminOnly: false },
  { to: "/stats", label: "📈 Статистика", adminOnly: true },
  { to: "/clients", label: "👥 Клиенты", adminOnly: false },
  { to: "/schedule", label: "📅 Расписание", adminOnly: true },
  { to: "/schedule/manage", label: "🗂 Недели", adminOnly: true },
  { to: "/races", label: "🏁 Гонки", adminOnly: true },
  { to: "/instructors", label: "🧑‍🏫 Инструкторы", adminOnly: true },
  { to: "/admins", label: "🔑 Админы", adminOnly: true }
] as const;

const MESSAGE_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/messaging", label: "📨 Рассылка", adminOnly: true },
  { to: "/messages", label: "✉️ Сообщения", adminOnly: true }
] as const;

const SERVICE_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/pulse", label: "❤️ Pulse", adminOnly: true },
  { to: "/activities", label: "🚴 Активности", adminOnly: true },
  { to: "/sync", label: "🔄 Синхронизация", adminOnly: true },
  { to: "/links", label: "🧩 Связки", adminOnly: true },
  { to: "/groups", label: "🏷 Группы", adminOnly: true }
] as const;

const SETTINGS_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/schedule/settings", label: "⚙️ Настройки расписания", adminOnly: true },
  { to: "/import", label: "📥 Импорт", adminOnly: true },
  { to: "/wattattack/accounts", label: "⚡️ Аккаунты WattAttack", adminOnly: true }
] as const;

const PUBLIC_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/leaderboard", label: "🌐 Лидерборд", adminOnly: false, external: true }
] as const;

const TECH_NAV_LINKS: readonly NavLinkConfig[] = [
  { to: "/bikes", label: "🚲 Велосипеды", adminOnly: false },
  { to: "/trainers", label: "💺 Тренажеры", adminOnly: false }
] as const;

export default function AppShell({ session, children, hideSidebar = false }: AppShellProps) {
  const location = useLocation();
  const isAdmin = session.isAdmin;
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    function handleResize() {
      setIsMobile(typeof window !== "undefined" && window.innerWidth < 960);
    }
    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (isMobile) {
      setSidebarOpen(false);
    } else {
      setSidebarOpen(true);
    }
  }, [isMobile]);

  useEffect(() => {
    if (isMobile) {
      setSidebarOpen(false);
    }
  }, [location.pathname, isMobile]);

  const filteredPrimaryLinks = PRIMARY_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredMessageLinks = MESSAGE_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredServiceLinks = SERVICE_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredSettingsLinks = SETTINGS_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredTechLinks = TECH_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));
  const filteredPublicLinks = PUBLIC_NAV_LINKS.filter((link) => (link.adminOnly ? isAdmin : true));

  async function handleLogout() {
    await fetch("/api/logout", { method: "GET", credentials: "include" });
    window.location.href = "/app";
  }

  const shellClass = classNames("app-shell", { "app-shell--full": hideSidebar });
  const mainClass = classNames("main-area", { "main-area--full": hideSidebar });

  return (
    <div className={shellClass}>
      {!hideSidebar && sidebarOpen && (
        <aside className="sidebar">
          <div className="brand">
            <span className="brand-accent" />
            <div>
              <div className="brand-title">КРУТИЛКА</div>
              <div className="brand-sub">АДМИНКА</div>
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
            {filteredSettingsLinks.length > 0 && (
              <div className="nav-section">
                <div className="nav-section-title">Настройки</div>
                {filteredSettingsLinks.map((link) => (
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
            {filteredPublicLinks.length > 0 && (
              <div className="nav-section">
                <div className="nav-section-title">Публично</div>
                {filteredPublicLinks.map((link) =>
                  link.external ? (
                    <a key={link.to} href={link.to} target="_blank" rel="noreferrer" className="nav-link">
                      {link.label}
                    </a>
                  ) : (
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
                  )
                )}
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
            <div>
              <h1>АДМИНКА КРУТИЛКИ</h1>
              <div className="main-meta">
                <span>Управление базой клиентов и инвентарем</span>
              </div>
            </div>
            {isMobile ? (
              <div className="header-actions">
                <button className="btn ghost" onClick={() => setSidebarOpen((prev) => !prev)}>
                  {sidebarOpen ? "Скрыть меню" : "Открыть меню"}
                </button>
              </div>
            ) : null}
          </header>
        )}
        <div className="main-content">{children}</div>
      </main>
    </div>
  );
}
