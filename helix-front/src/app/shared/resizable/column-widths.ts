// Column widths a user picked, kept per table in localStorage. Storage can be
// missing or full (private windows, quota), so every access fails soft.
const PREFIX = 'helix-columns:';

export function readWidths(table: string): Record<string, number> {
  try {
    const parsed = JSON.parse(localStorage.getItem(PREFIX + table) || '{}');
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? parsed
      : {};
  } catch (e) {
    return {};
  }
}

export function writeWidths(
  table: string,
  widths: Record<string, number> | null,
): void {
  try {
    if (widths && Object.keys(widths).length) {
      localStorage.setItem(PREFIX + table, JSON.stringify(widths));
    } else {
      localStorage.removeItem(PREFIX + table);
    }
  } catch (e) {
    // a width that is not remembered is not worth an error
  }
}

// For ngx-datatable lists: bind [width]="widths.get('Name', 260)" and
// (resize)="widths.save($event)" so a dragged column keeps its width next time.
// The bound widths are read once: changing a [width] input makes ngx-datatable
// rebuild and refit every column, which moves the column the user just dropped.
export class ColumnWidths {
  private readonly widths: Record<string, number>;

  constructor(private readonly table: string) {
    this.widths = readWidths(table);
  }

  get(column: string, fallback: number): number {
    const width = this.widths[column];
    return typeof width === 'number' && width > 0 ? width : fallback;
  }

  save(event: { column: { name?: string }; newValue: number }): void {
    if (event?.column?.name && event.newValue > 0) {
      writeWidths(this.table, {
        ...readWidths(this.table),
        [event.column.name]: Math.round(event.newValue),
      });
    }
  }
}
