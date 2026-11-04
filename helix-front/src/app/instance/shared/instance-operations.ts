import {
  InstanceOperationEntry,
  InstanceOperationState,
  InstanceSummary,
} from './instance.model';

export interface OperationRow {
  entry: InstanceOperationEntry;
  // The last stored operation is the one that takes effect.
  active: boolean;
  // null when no time was recorded and the config creation time is unknown
  atMillis: number | null;
  // A timestamp of -1 means the operation was set when the instance config was created,
  // for example the INIT operation of an instance that just joined.
  fromConfigCreation: boolean;
}

export function operationRows(
  summary: InstanceSummary | undefined,
  effective: InstanceOperationState,
): OperationRow[] {
  const entries = summary?.instanceOperations || [];
  const rows: OperationRow[] = entries.map((entry, index) => {
    const stamped = entry.timestamp > 0;
    return {
      entry,
      active: index === entries.length - 1,
      atMillis: stamped ? entry.timestamp : summary?.createdAtMillis ?? null,
      fromConfigCreation: !stamped && !!summary?.createdAtMillis,
    };
  });

  // The legacy HELIX_ENABLED=false flag disables an instance without storing an operation, so
  // the effective operation can differ from the last stored one.
  const last = entries[entries.length - 1];
  if (effective !== 'ENABLE' && last?.operation !== effective) {
    rows.forEach((row) => (row.active = false));
    rows.push({
      entry: { operation: effective, source: 'LEGACY', reason: '', timestamp: -1 },
      active: true,
      atMillis: null,
      fromConfigCreation: false,
    });
  }
  return rows;
}

export function formatUtc(millis: number): string {
  return new Date(millis).toISOString().slice(0, 16).replace('T', ' ') + ' UTC';
}

export function operationDetail(row: OperationRow): string {
  const when =
    row.atMillis === null
      ? 'time not recorded'
      : formatUtc(row.atMillis) +
        (row.fromConfigCreation ? ' (instance config created)' : '');
  return row.entry.reason ? `${when} · ${row.entry.reason}` : when;
}
