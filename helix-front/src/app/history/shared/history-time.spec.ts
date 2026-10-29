import { describe, expect, it } from '@jest/globals';

import { formatTime, parseOfflineDate } from './history-time';

describe('history time', () => {
  // 2026-10-27T08:21:42.114Z
  const ms = Date.UTC(2026, 9, 27, 8, 21, 42, 114);

  it('formats the same instant in each zone', () => {
    expect(formatTime(ms, 'UTC')).toBe('2026-10-27 08:21:42.114');
    expect(formatTime(ms, 'Asia/Kolkata')).toBe('2026-10-27 13:51:42.114');
    expect(formatTime(ms, 'America/Los_Angeles')).toBe(
      '2026-10-27 01:21:42.114',
    );
  });

  it('pads milliseconds and handles midnight as 00', () => {
    expect(formatTime(Date.UTC(2026, 0, 1, 0, 0, 0, 5), 'UTC')).toBe(
      '2026-01-01 00:00:00.005',
    );
  });

  it('parses offline entries as UTC and rejects bad ones', () => {
    expect(parseOfflineDate('2026-10-27T08:21:42:114')).toBe(ms);
    expect(parseOfflineDate('not a date')).toBeNull();
  });
});
