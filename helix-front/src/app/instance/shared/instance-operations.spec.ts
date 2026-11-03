import { describe, expect, it } from '@jest/globals';

import { InstanceSummary } from './instance.model';
import { operationDetail, operationRows } from './instance-operations';

const summary = (
  instanceOperations: InstanceSummary['instanceOperations'],
  createdAtMillis?: number,
): InstanceSummary => ({
  host: 'h',
  port: '1',
  domain: '',
  tags: [],
  capacity: {},
  instanceOperations,
  createdAtMillis,
});

describe('operationRows', () => {
  it('lists every source and marks the last stored operation active', () => {
    const rows = operationRows(
      summary([
        { operation: 'DISABLE', source: 'USER', reason: 'disk', timestamp: 60000 },
        { operation: 'EVACUATE', source: 'AUTOMATION', reason: '', timestamp: 120000 },
      ]),
      'EVACUATE',
    );
    expect(rows.map((row) => row.active)).toEqual([false, true]);
    expect(operationDetail(rows[0])).toBe('1970-01-01 00:01 UTC · disk');
    expect(operationDetail(rows[1])).toBe('1970-01-01 00:02 UTC');
  });

  it('dates a -1 timestamp by the config creation time', () => {
    const [row] = operationRows(
      summary([{ operation: 'UNKNOWN', source: 'USER', reason: 'INIT', timestamp: -1 }], 3600000),
      'UNKNOWN',
    );
    expect(row.atMillis).toBe(3600000);
    expect(operationDetail(row)).toBe('1970-01-01 01:00 UTC (instance config created) · INIT');
  });

  it('does not invent a time when the creation time is unknown', () => {
    const [row] = operationRows(
      summary([{ operation: 'UNKNOWN', source: 'USER', reason: 'INIT', timestamp: -1 }]),
      'UNKNOWN',
    );
    expect(row.atMillis).toBeNull();
    expect(operationDetail(row)).toBe('time not recorded · INIT');
  });

  it('adds a legacy row when the effective operation is not a stored one', () => {
    const rows = operationRows(
      summary([{ operation: 'ENABLE', source: 'USER', reason: '', timestamp: 60000 }]),
      'DISABLE',
    );
    expect(rows.map((row) => [row.entry.operation, row.entry.source, row.active])).toEqual([
      ['ENABLE', 'USER', false],
      ['DISABLE', 'LEGACY', true],
    ]);
  });

  it('shows nothing for an enabled instance with no stored operation', () => {
    expect(operationRows(summary([]), 'ENABLE')).toEqual([]);
  });
});
