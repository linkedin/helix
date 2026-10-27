import { describe, expect, it } from '@jest/globals';
import { isZNRecord } from './znrecord.model';

describe('ZNRecord response validation', () => {
  it('accepts nullable ZNRecord fields without silently removing them', () => {
    const record = {
      id: 'Orders',
      simpleFields: { REBALANCE_STRATEGY: null, REPLICAS: '3' },
      listFields: { Orders_0: null },
      mapFields: { Orders_0: { node: null }, Orders_1: null },
    };
    expect(isZNRecord(record)).toBe(true);
  });

  it('rejects missing containers and non-string state values', () => {
    expect(isZNRecord({ id: 'Orders' })).toBe(false);
    expect(
      isZNRecord({
        id: 'Orders',
        simpleFields: {},
        listFields: {},
        mapFields: { Orders_0: { node: 3 } },
      }),
    ).toBe(false);
  });
});
