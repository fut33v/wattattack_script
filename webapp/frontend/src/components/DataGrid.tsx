import type { ReactNode } from "react";
import classNames from "classnames";

export interface Column<T> {
  key: keyof T | string;
  title: string;
  render?: (item: T) => ReactNode;
  className?: string;
  sortable?: boolean;
}

interface SelectionConfig<T> {
  selectedKeys: Set<string | number>;
  onToggle: (key: string | number, item: T) => void;
  onToggleAll?: (checked: boolean) => void;
  isAllSelected?: boolean;
}

interface DataGridProps<T> {
  items: T[];
  columns: Column<T>[];
  emptyMessage?: ReactNode;
  getRowKey: (item: T) => string | number;
  actions?: (item: T) => ReactNode;
  tableClassName?: string;
  selection?: SelectionConfig<T>;
  sortState?: {
    sortKey: string | null;
    direction: "asc" | "desc";
    onSort: (key: string) => void;
  };
}

export function DataGrid<T>({
  items,
  columns,
  emptyMessage,
  getRowKey,
  actions,
  tableClassName,
  selection,
  sortState,
}: DataGridProps<T>) {
  return (
    <div className="table-container">
      <table className={classNames("data-table", tableClassName)}>
        <thead>
          <tr>
            {selection && (
              <th className="selection-col">
                <input
                  type="checkbox"
                  checked={selection.isAllSelected ?? false}
                  onChange={(e) => selection.onToggleAll?.(e.target.checked)}
                  aria-label="Выбрать все"
                />
              </th>
            )}
            {columns.map((column) => (
              <th
                key={String(column.key)}
                className={classNames(column.className, column.sortable && "sortable-col")}
                onClick={
                  column.sortable && sortState
                    ? () => sortState.onSort(String(column.key))
                    : undefined
                }
              >
                <span className="th-label">{column.title}</span>
                {column.sortable && sortState?.sortKey === column.key && (
                  <span className="sort-indicator">
                    {sortState.direction === "asc" ? "▲" : "▼"}
                  </span>
                )}
              </th>
            ))}
            {actions && <th className="actions-col">Действия</th>}
          </tr>
        </thead>
        <tbody>
          {items.length === 0 ? (
            <tr>
              <td colSpan={columns.length + (actions ? 1 : 0) + (selection ? 1 : 0)}>
                {emptyMessage ?? "Нет данных"}
              </td>
            </tr>
          ) : (
            items.map((item) => (
              <tr key={getRowKey(item)}>
                {selection && (
                  <td className="selection-cell">
                    <input
                      type="checkbox"
                      checked={selection.selectedKeys.has(getRowKey(item))}
                      onChange={() => selection.onToggle(getRowKey(item), item)}
                      aria-label="Выбрать строку"
                    />
                  </td>
                )}
                {columns.map((column) => (
                  <td key={String(column.key)} className={classNames(column.className)}>
                    {column.render ? column.render(item) : (item as any)[column.key]}
                  </td>
                ))}
                {actions && <td className="actions-cell">{actions(item)}</td>}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export default DataGrid;
