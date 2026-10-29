export interface HistoryZone {
  key: string;
  label: string;
  tz?: string;
}

export const HISTORY_ZONES: HistoryZone[] = [
  { key: 'local', label: 'Local' },
  { key: 'utc', label: 'UTC', tz: 'UTC' },
  { key: 'pst', label: 'PST/PDT', tz: 'America/Los_Angeles' },
  { key: 'ist', label: 'IST', tz: 'Asia/Kolkata' },
];

export function formatTime(ms: number, tz?: string): string {
  if (ms == null || isNaN(ms)) {
    return '';
  }
  const parts: { [type: string]: string } = {};
  new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    hourCycle: 'h23',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  } as Intl.DateTimeFormatOptions)
    .formatToParts(new Date(ms))
    .forEach((p) => (parts[p.type] = p.value));
  const millis = String(Math.abs(ms % 1000)).padStart(3, '0');
  return (
    `${parts.year}-${parts.month}-${parts.day} ` +
    `${parts.hour}:${parts.minute}:${parts.second}.${millis}`
  );
}

// Offline entries are stored as UTC strings like 2026-10-27T08:21:42:114
export function parseOfflineDate(value: string): number | null {
  const m = /^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d):(\d{3})$/.exec(value);
  return m
    ? Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6], +m[7])
    : null;
}
