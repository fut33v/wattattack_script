import type { ReactNode } from "react";

interface StateScreenProps {
  title: string;
  message?: string;
  action?: ReactNode;
}

export function StateScreen({ title, message, action }: StateScreenProps) {
  return (
    <div className="state-screen">
      <div className="state-card">
        <h2>{title}</h2>
        {message && <p>{message}</p>}
        {action && <div className="state-action">{action}</div>}
      </div>
    </div>
  );
}

export default StateScreen;
