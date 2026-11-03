import { describe, expect, it } from '@jest/globals';

import { snapshotFixture } from '../testing/snapshot.fixture';
import { bandOf, buildHeatmap } from './heatmap.model';

describe('heat map model', () => {
  it('bands ratios and never puts unknown in a low band', () => {
    expect([null, 0, 0.49, 0.5, 0.75, 0.9, 1, 1.01].map(bandOf)).toEqual([
      'unknown',
      'low',
      'low',
      'mid',
      'high',
      'hot',
      'hot',
      'over',
    ]);
  });

  it('groups by zone, worst zone first, with flags in the label', () => {
    const zones = buildHeatmap(snapshotFixture(), 'CU', false);

    expect(zones.map((z) => [z.label, z.worst, z.tiles.length])).toEqual([
      ['z3', 1.2, 2],
      ['z2', 0.9, 1],
      ['z1', 0.3, 1],
    ]);
    const [n3, n4] = zones[0].tiles;
    expect([n3.band, n3.text, n4.band, n4.text]).toEqual([
      'over',
      '120%',
      'unknown',
      'unknown',
    ]);
    expect(n3.weight).toBeGreaterThan(zones[2].tiles[0].weight);
    expect(n4.weight).toBeGreaterThan(0);
    expect(n3.label).toBe('n3, z3, CU 1200 / 1000 (120%), 2 ERROR replicas');
    expect(n4.label).toBe('n4, z3, CU unknown, not live, operation DISABLE');
  });

  it('uses top state usage when asked', () => {
    const zones = buildHeatmap(snapshotFixture(), 'DISK', true);

    expect(zones.map((z) => [z.label, z.worst])).toEqual([
      ['z3', 0.6],
      ['z2', 0.45],
      ['z1', 0.15],
    ]);
    expect(zones[0].tiles[0].band).toBe('mid');
    expect(zones[0].tiles[0].label).toContain(
      'DISK top state 1200 / 2000 (60%)',
    );
  });

  it('treats max 0 or a missing key as unknown and groups an empty zone', () => {
    const snapshot = snapshotFixture();
    snapshot.instances[0].max = { CU: 0, DISK: 2000 };
    snapshot.instances[1].max = { DISK: 2000 };
    snapshot.instances.forEach((i) => (i.zone = i.name === 'n3' ? 'z3' : ''));
    const zones = buildHeatmap(snapshot, 'CU', false);

    expect(zones.map((z) => z.label)).toEqual(['z3', 'No fault zone']);
    expect(zones[1].worst).toBeNull();
    expect(zones[1].tiles.map((t) => [t.instance.name, t.band])).toEqual([
      ['n1', 'unknown'],
      ['n2', 'unknown'],
      ['n4', 'unknown'],
    ]);
  });
});
