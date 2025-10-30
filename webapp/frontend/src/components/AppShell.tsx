import { NavLink, useLocation } from "react-router-dom";
import type { ReactNode } from "react";
import classNames from "classnames";

import type { SessionResponse } from "../lib/types";

interface AppShellProps {
  session: SessionResponse;
  children: ReactNode;
}

const links = [
  { to: "/dashboard", label: "Панель" },
  { to: "/clients", label: "Клиенты" },
  { to: "/bikes", label: "Велосипеды" },
  { to: "/trainers", label: "Тренажеры" },
  { to: "/links", label: "Связки" },
  { to: "/admins", label: "Админы" }
];

export function AppShell({ session, children }: AppShellProps) {
  const location = useLocation();
  const isAdmin = session.isAdmin;

  const filteredLinks = links.filter((link) => {
    if (["/links", "/admins"].includes(link.to)) {
      return isAdmin;
    }
    return true;
  });

  async function handleLogout() {
    await fetch("/api/logout", { method: "GET", credentials: "include" });
    window.location.href = "/app";
  }

  return (
    <div className="app-shell">
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
                  active: isActive || location.pathname.startsWith(link.to)
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
      <main className="main-area">
        <header className="main-header">
          <h1>Крутилка Admin</h1>
          <div className="main-meta">
            <span>Управление базой клиентов и инвентарем</span>
          </div>
        </header>
        <div className="main-content">{children}</div>
      </main>
    </div>
  );
}

export default AppShell;
