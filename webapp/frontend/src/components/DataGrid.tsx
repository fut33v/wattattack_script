import type { ReactNode } from "react";
import classNames from "classnames";

export interface Column<T> {
  key: keyof T | string;
  title: string;
  render?: (item: T) => ReactNode;
  className?: string;
}

interface DataGridProps<T> {
  items: T[];
  columns: Column<T>[];
  emptyMessage?: ReactNode;
  getRowKey: (item: T) => string | number;
  actions?: (item: T) => ReactNode;
  tableClassName?: string;
}

export function DataGrid<T>({ items, columns, emptyMessage, getRowKey, actions, tableClassName }: DataGridProps<T>) {
  return (
    <div className="table-container">
      <table className={classNames("data-table", tableClassName)}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={String(column.key)} className={column.className}>
                {column.title}
              </th>
            ))}
            {actions && <th className="actions-col">Действия</th>}
          </tr>
        </thead>
        <tbody>
          {items.length === 0 ? (
            <tr>
              <td colSpan={columns.length + (actions ? 1 : 0)}>{emptyMessage ?? "Нет данных"}</td>
            </tr>
          ) : (
            items.map((item) => (
              <tr key={getRowKey(item)}>
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
