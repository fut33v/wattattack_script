import { useState } from "react";

import "../styles/settings.css";

import ImportPage from "./ImportPage";
import NotificationSettingsPage from "./NotificationSettingsPage";
import WattattackAccountsPage from "./WattattackAccountsPage";

interface SettingsSection {
  id: string;
  title: string;
  subtitle: string;
  content: JSX.Element;
}

export default function SettingsPage() {
  const sections: SettingsSection[] = [
    {
      id: "schedule-settings",
      title: "Уведомления расписания",
      subtitle: "Время отправки напоминаний и запись в очередь",
      content: <NotificationSettingsPage />
    },
    {
      id: "backup-import",
      title: "Импорт бэкапа",
      subtitle: "Загрузка .sql / .sql.gz в текущую БД",
      content: <ImportPage />
    },
    {
      id: "wattattack-accounts",
      title: "Аккаунты WattAttack",
      subtitle: "Логины, пароли и сопоставление станков",
      content: <WattattackAccountsPage />
    }
  ];

  const [expanded, setExpanded] = useState<Record<string, boolean>>(
    sections.reduce<Record<string, boolean>>((acc, section) => {
      acc[section.id] = false;
      return acc;
    }, {})
  );

  function toggle(id: string) {
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }));
  }

  return (
    <div className="settings-page">
      <section className="settings-hero">
        <div className="settings-hero-text">
          <p className="settings-kicker">Центр управления</p>
          <h1>Настройки</h1>
          <p className="settings-lead">
            Сюда собрали рабочие настройки: уведомления расписания, восстановление из бэкапа и доступы WattAttack.
            Все под рукой, чтобы быстрее включать/проверять сервисы.
          </p>
          <div className="settings-actions">
            {sections.map((section) => (
              <button
                key={section.id}
                className="btn ghost"
                onClick={() => toggle(section.id)}
                aria-expanded={expanded[section.id]}
              >
                {expanded[section.id] ? "Свернуть" : "Развернуть"} — {section.title}
              </button>
            ))}
          </div>
        </div>
        <div className="settings-hero-meta">
          <div className="settings-pill">Технический раздел</div>
          <div className="settings-pill settings-pill--muted">Доступно только администраторам</div>
          <div className="settings-checklist">
            <span>✓</span>
            <div>
              <div className="settings-check-title">Напоминания</div>
              <div className="settings-check-sub">Время отправки, запись в очередь</div>
            </div>
          </div>
          <div className="settings-checklist">
            <span>✓</span>
            <div>
              <div className="settings-check-title">Резервное восстановление</div>
              <div className="settings-check-sub">.sql / .sql.gz в рабочую БД</div>
            </div>
          </div>
          <div className="settings-checklist">
            <span>✓</span>
            <div>
              <div className="settings-check-title">Аккаунты WattAttack</div>
              <div className="settings-check-sub">ID, пароли и сопоставление станков</div>
            </div>
          </div>
        </div>
      </section>

      <div className="settings-stack">
        {sections.map((section) => (
          <section key={section.id} id={section.id} className="settings-section">
            <button
              type="button"
              className="settings-section-header"
              onClick={() => toggle(section.id)}
              aria-expanded={expanded[section.id]}
            >
              <div>
                <div className="settings-section-title">{section.title}</div>
                <div className="settings-section-subtitle">{section.subtitle}</div>
              </div>
              <span className="settings-section-toggle">{expanded[section.id] ? "▲" : "▼"}</span>
            </button>
            {expanded[section.id] ? <div className="settings-section-body">{section.content}</div> : null}
          </section>
        ))}
      </div>
    </div>
  );
}
