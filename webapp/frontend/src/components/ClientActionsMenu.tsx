import { useEffect, useRef, useState } from "react";
import classNames from "classnames";

interface ClientActionsMenuProps {
  onEdit: () => void;
  onDelete: () => void;
  disabled?: boolean;
}

export default function ClientActionsMenu({ onEdit, onDelete, disabled }: ClientActionsMenuProps) {
  const [open, setOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (!menuRef.current) return;
      if (menuRef.current.contains(event.target as Node)) return;
      setOpen(false);
    }

    function handleEscape(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
      }
    }

    if (open) {
      document.addEventListener("mousedown", handleClickOutside);
      document.addEventListener("keydown", handleEscape);
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("keydown", handleEscape);
    };
  }, [open]);

  return (
    <div className="client-actions-menu" ref={menuRef}>
      <button
        type="button"
        className="client-actions-trigger"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={(event) => {
          event.stopPropagation();
          setOpen((prev) => !prev);
        }}
        disabled={disabled}
      >
        ⋮
      </button>
      {open && (
        <div className="client-actions-popover" role="menu" onClick={(event) => event.stopPropagation()}>
          <button
            type="button"
            className="client-actions-item"
            role="menuitem"
            onClick={() => {
              setOpen(false);
              onEdit();
            }}
          >
            Редактировать
          </button>
          <button
            type="button"
            className={classNames("client-actions-item", "danger")}
            role="menuitem"
            onClick={() => {
              setOpen(false);
              onDelete();
            }}
            disabled={disabled}
          >
            Удалить
          </button>
        </div>
      )}
    </div>
  );
}
