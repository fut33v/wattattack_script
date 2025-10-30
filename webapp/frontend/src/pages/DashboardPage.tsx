import { useQuery } from "@tanstack/react-query";
import Panel from "../components/Panel";
import { apiFetch } from "../lib/api";

interface SummaryResponse {
  clients: number;
  bikes: number;
  trainers: number;
  admins: number;
  links: number;
}

const cards = [
  { key: "clients", title: "Клиенты", accent: "#4f9bff" },
  { key: "bikes", title: "Велосипеды", accent: "#43d29f" },
  { key: "trainers", title: "Тренажеры", accent: "#ffb648" },
  { key: "links", title: "Связки", accent: "#ff6b8b" },
  { key: "admins", title: "Админы", accent: "#7c53ff" }
] as const;

export default function DashboardPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["summary"],
    queryFn: () => apiFetch<SummaryResponse>("/api/summary")
  });

  return (
    <Panel title="Панель" subtitle="Краткий обзор базы и инвентаря">
      <div className="summary-grid">
        {cards.map((card) => (
          <div key={card.key} className="summary-card" style={{
            background: `linear-gradient(135deg, rgba(18, 25, 42, 0.95), rgba(18, 25, 42, 0.75))`,
            borderColor: `${card.accent}33`
          }}>
            <div className="summary-label">{card.title}</div>
            <div className="summary-value" style={{ color: card.accent }}>
              {isLoading ? "…" : data?.[card.key] ?? 0}
            </div>
          </div>
        ))}
      </div>
    </Panel>
  );
}
