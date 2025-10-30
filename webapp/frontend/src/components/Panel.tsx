import type { ReactNode } from "react";
import classNames from "classnames";

interface PanelProps {
  title?: string;
  subtitle?: string;
  headerExtra?: ReactNode;
  children: ReactNode;
  className?: string;
}

export function Panel({ title, subtitle, headerExtra, children, className }: PanelProps) {
  return (
    <section className={classNames("panel", className)}>
      {(title || subtitle || headerExtra) && (
        <header className="panel-header">
          <div>
            {title && <h2>{title}</h2>}
            {subtitle && <p>{subtitle}</p>}
          </div>
          {headerExtra && <div className="panel-extra">{headerExtra}</div>}
        </header>
      )}
      <div className="panel-body">{children}</div>
    </section>
  );
}

export default Panel;
