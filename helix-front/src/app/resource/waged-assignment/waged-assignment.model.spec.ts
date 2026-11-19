import { describe, expect, it } from '@jest/globals';

import { compareAssignment } from './waged-assignment.model';

describe('compareAssignment', () => {
  it('lines up every partition with the external view and names the difference', () => {
    const rows = compareAssignment(
      {
        p_10: { a: 'MASTER', b: 'SLAVE' },
        p_2: { a: 'SLAVE', c: 'MASTER' },
        p_1: { b: 'MASTER', c: 'SLAVE' },
        p_3: { a: 'MASTER' },
      },
      {
        p_10: { b: 'SLAVE', a: 'MASTER' },
        p_2: { a: 'SLAVE', b: 'MASTER' },
        p_1: { b: 'SLAVE', c: 'MASTER' },
        p_3: { a: 'MASTER', d: 'SLAVE' },
        p_4: { a: 'MASTER' },
      },
    );

    expect(rows.map((r) => [r.partition, r.difference])).toEqual([
      ['p_1', '2 states differ'],
      ['p_2', '1 replica not placed yet, 1 extra replica still placed'],
      ['p_3', '1 extra replica still placed'],
      ['p_4', 'Not in the saved assignment'],
      ['p_10', null],
    ]);
    expect(rows[4].other).toHaveLength(2);
    expect(rows[4].saved).toEqual([
      { instance: 'a', state: 'MASTER' },
      { instance: 'b', state: 'SLAVE' },
    ]);
  });

  it('counts a replaced replica on both sides, with any state change', () => {
    const [moved, both] = compareAssignment(
      { p_0: { a: 'MASTER', b: 'SLAVE' }, p_1: { a: 'MASTER', b: 'SLAVE' } },
      { p_0: { a: 'MASTER', c: 'SLAVE' }, p_1: { a: 'SLAVE', c: 'MASTER' } },
    );
    expect(moved.difference).toBe(
      '1 replica not placed yet, 1 extra replica still placed',
    );
    expect(both.difference).toBe(
      '1 replica not placed yet, 1 extra replica still placed, 1 state differs',
    );
  });

  it('names the map it compares with', () => {
    const [row] = compareAssignment(
      { p_0: { a: 'LEADER' } },
      {},
      'ideal state',
    );
    expect(row.difference).toBe('Not in the ideal state');
  });

  it('lists only the saved placement when not comparing', () => {
    const rows = compareAssignment({ p_0: { a: 'LEADER' } });
    expect(rows).toEqual([
      {
        partition: 'p_0',
        saved: [{ instance: 'a', state: 'LEADER' }],
        other: [],
        difference: null,
      },
    ]);
  });
});
