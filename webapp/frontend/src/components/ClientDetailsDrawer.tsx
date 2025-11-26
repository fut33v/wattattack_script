import dayjs from "dayjs";
import type { ClientRow } from "../lib/types";

interface ClientDetailsDrawerProps {
  client: ClientRow | null;
  onClose: () => void;
  onEdit: (client: ClientRow) => void;
  onDelete: (client: ClientRow) => void;
  isDeleting?: boolean;
  formatGender?: (value: string | null | undefined) => string;
}

function formatMetric(label: string, value: string | number | null | undefined) {
  if (value === null || value === undefined || value === "") return "—";
  return `${value} ${label}`;
}

export default function ClientDetailsDrawer({
  client,
  onClose,
  onEdit,
  onDelete,
  isDeleting,
  formatGender
}: ClientDetailsDrawerProps) {
  if (!client) return null;

  const submittedAt = client.submitted_at
    ? dayjs(client.submitted_at).format("DD.MM.YY HH:mm")
    : null;

  const name =
    client.full_name ||
    [client.first_name, client.last_name].filter(Boolean).join(" ") ||
    `Клиент #${client.id}`;

  return (
    // Боковая панель быстрого просмотра клиента
    <div className="client-drawer" role="dialog" aria-modal="true" aria-label={`Клиент ${name}`}>
      <div className="client-drawer__backdrop" onClick={onClose} />
      <aside className="client-drawer__panel">
        <header className="client-drawer__header">
          <div>
            <div className="client-drawer__eyebrow">Клиент</div>
            <div className="client-drawer__title">{name}</div>
          </div>
          <button type="button" className="client-drawer__close" onClick={onClose} aria-label="Закрыть">
            ✕
          </button>
        </header>
        <div className="client-drawer__body">
          <div className="client-drawer__stats">
            <div className="client-drawer__stat">
              <span className="label">Пол</span>
              <span className="value">{formatGender ? formatGender(client.gender) : client.gender ?? "—"}</span>
            </div>
            <div className="client-drawer__stat">
              <span className="label">Рост</span>
              <span className="value">{formatMetric("см", client.height)}</span>
            </div>
            <div className="client-drawer__stat">
              <span className="label">Вес</span>
              <span className="value">{formatMetric("кг", client.weight)}</span>
            </div>
            <div className="client-drawer__stat">
              <span className="label">FTP</span>
              <span className="value">{client.ftp ?? "—"}</span>
            </div>
          </div>

          <dl className="client-drawer__details">
            <div className="detail">
              <dt>Педали</dt>
              <dd>{client.pedals ?? "—"}</dd>
            </div>
            <div className="detail">
              <dt>Анкета</dt>
              <dd>{submittedAt ?? "—"}</dd>
            </div>
            <div className="detail">
              <dt>Любимый велосипед</dt>
              <dd>{client.favorite_bike ?? "—"}</dd>
            </div>
            <div className="detail">
              <dt>Цель</dt>
              <dd>{client.goal ?? "—"}</dd>
            </div>
          </dl>
        </div>
        <div className="client-drawer__actions">
          <button
            type="button"
            className="button"
            onClick={() => onEdit(client)}
          >
            Редактировать
          </button>
          <button
            type="button"
            className="button danger"
            onClick={() => onDelete(client)}
            disabled={isDeleting}
          >
            Удалить
          </button>
        </div>
      </aside>
    </div>
  );
}
