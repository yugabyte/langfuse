import type { FilterState, OrderByState } from "@langfuse/shared";
import type { ColumnOrderState, VisibilityState } from "@tanstack/react-table";

export type TableViewPresetState = {
  filters: FilterState;
  columnOrder: ColumnOrderState;
  columnVisibility: VisibilityState;
  orderBy: OrderByState;
  searchQuery?: string | null;
};
