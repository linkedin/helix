import { describe, expect, it } from '@jest/globals';

import { snapshotFixture, withResources } from '../testing/snapshot.fixture';
import {
  bandOf,
  buildHeatmap,
  instanceDetail,
  shadeOf,
  shortNames,
} from './heatmap.model';

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

  it('steps load in 5% and median distance in 10%, never guessing unknown', () => {
    expect(
      [null, 0, 0.49, 0.84, 0.999, 1, 1.2].map((r) => shadeOf(r, 'load', null)),
    ).toEqual(['unknown', 'l0', 'l9', 'l16', 'l19', 'l20', 'over']);
    expect(
      [0.5, 0.55, 0.45, 0.74, 0.25, 2].map((r) => shadeOf(r, 'median', 0.5)),
    ).toEqual(['p0', 'p1', 'm1', 'p5', 'm5', 'p5']);
    expect(shadeOf(0.5, 'median', null)).toBe('unknown');
    expect([0, 0.3].map((r) => shadeOf(r, 'median', 0))).toEqual(['p0', 'p5']);
    expect(shadeOf(null, 'median', 0.5)).toBe('unknown');
  });

  it('shortens names to the part that tells them apart', () => {
    const short = shortNames([
      'dc1-app3223.example.com_11932',
      'dc1-app33495.example.com_11932',
    ]);
    expect(short('dc1-app3223.example.com_11932')).toBe('3223');
    expect(short('dc1-app33495.example.com_11932')).toBe('33495');
    const ports = shortNames([
      'participant.local_25000',
      'participant.local_25001',
    ]);
    expect(ports('participant.local_25001')).toBe('25001');
    expect(shortNames(['n1', 'n2'])('n2')).toBe('n2');
    expect(shortNames(['only'])('only')).toBe('only');
    const hosts = [
      'host-1.example.com_11932',
      'host-10.example.com_11932',
      'host-12.example.com_11932',
    ];
    for (const list of [hosts, [...hosts].reverse()]) {
      expect(list.map(shortNames(list))).toEqual(
        list.map((n) => n.split('.')[0].slice(5)),
      );
    }
    expect(shortNames(['same', 'same'])('same')).toBe('same');
  });

  it('orders zones by name and instances hottest first, with zone stats', () => {
    const map = buildHeatmap(snapshotFixture(), 'CU', false);

    expect(map.zones.map((z) => [z.label, z.mean, z.max, z.shade])).toEqual([
      ['z1', 0.3, 0.3, 'l6'],
      ['z2', 0.9, 0.9, 'l18'],
      ['z3', 1.2, 1.2, 'over'],
    ]);
    const [n3, n4] = map.zones[2].tiles;
    expect([n3.shade, n3.text, n4.shade, n4.text]).toEqual([
      'over',
      '120%',
      'unknown',
      'unknown',
    ]);
    expect(n3.label).toBe('n3, z3, CU 1200 / 1000 (120%), 2 ERROR replicas');
    expect(n4.label).toBe('n4, z3, CU unknown, not live, operation DISABLE');
    expect([map.basis, map.reference]).toEqual(['median', 0.9]);
    expect(map.hottest.map((t) => t.instance.name)).toEqual(['n3', 'n2', 'n1']);
    expect(map.counts).toEqual({ low: 1, hot: 1, over: 1, unknown: 1 });
    expect(map.columns).toBe(4);
  });

  it('sorts a zone hottest first whatever order the snapshot lists it in', () => {
    const snapshot = snapshotFixture();
    const [n1] = snapshot.instances;
    snapshot.instances.push(
      { ...n1, name: 'n6', used: { CU: 300, DISK: 0 } },
      { ...n1, name: 'n5', used: { CU: 800, DISK: 0 } },
    );
    const map = buildHeatmap(snapshot, 'CU', false);

    expect(map.zones[0].tiles.map((t) => t.instance.name)).toEqual([
      'n5',
      'n1',
      'n6',
    ]);
    expect(map.zones[0].mean).toBeCloseTo(14 / 30);
  });

  it('colors by distance from the median when asked', () => {
    const map = buildHeatmap(snapshotFixture(), 'CU', false, 'median');

    expect(
      map.zones.flatMap((z) => z.tiles.map((t) => [t.instance.name, t.shade])),
    ).toEqual([
      ['n1', 'm5'],
      ['n2', 'p0'],
      ['n3', 'p3'],
      ['n4', 'unknown'],
    ]);
  });

  it('compares with the mean when most instances carry nothing', () => {
    const snapshot = snapshotFixture();
    snapshot.instances[0].used = { CU: 0, DISK: 0 };
    snapshot.instances[1].used = { CU: 0, DISK: 0 };
    snapshot.instances[2].used = { CU: 800, DISK: 0 };
    const map = buildHeatmap(snapshot, 'CU', false, 'median');

    expect([map.basis, map.reference]).toEqual(['mean', 0.8 / 3]);
    expect(
      map.zones.flatMap((z) => z.tiles.map((t) => [t.instance.name, t.shade])),
    ).toEqual([
      ['n1', 'm5'],
      ['n2', 'm5'],
      ['n3', 'p5'],
      ['n4', 'unknown'],
    ]);
  });

  it('ranks every instance in one block when not grouped by zone', () => {
    const map = buildHeatmap(snapshotFixture(), 'CU', false, 'load', false);

    expect(map.zones.map((z) => [z.label, z.max])).toEqual([
      ['All instances', 1.2],
    ]);
    expect(map.zones[0].mean).toBeCloseTo(0.8);
    expect(map.zones[0].tiles.map((t) => t.instance.name)).toEqual([
      'n3',
      'n2',
      'n1',
      'n4',
    ]);
    expect(map.zones[0].tiles[0].label).toBe(
      'n3, z3, CU 1200 / 1000 (120%), 2 ERROR replicas',
    );
  });

  it('uses top state usage when asked', () => {
    const map = buildHeatmap(snapshotFixture(), 'DISK', true);

    expect(map.zones.map((z) => [z.label, z.max])).toEqual([
      ['z1', 0.15],
      ['z2', 0.45],
      ['z3', 0.6],
    ]);
    expect(map.zones[2].tiles[0].shade).toBe('l12');
    expect(map.zones[2].tiles[0].label).toContain(
      'DISK top state 1200 / 2000 (60%)',
    );
  });

  it('treats max 0 or a missing key as unknown and puts no fault zone last', () => {
    const snapshot = snapshotFixture();
    snapshot.instances[0].max = { CU: 0, DISK: 2000 };
    snapshot.instances[1].max = { DISK: 2000 };
    snapshot.instances.forEach((i) => (i.zone = i.name === 'n3' ? 'z3' : ''));
    const map = buildHeatmap(snapshot, 'CU', false);

    expect(map.zones.map((z) => z.label)).toEqual(['z3', 'No fault zone']);
    expect([map.zones[1].mean, map.zones[1].max]).toEqual([null, null]);
    expect(map.zones[1].tiles.map((t) => [t.instance.name, t.shade])).toEqual([
      ['n1', 'unknown'],
      ['n2', 'unknown'],
      ['n4', 'unknown'],
    ]);
    expect(map.hottest.map((t) => t.instance.name)).toEqual(['n3']);
  });

  it('details every key and the top resources for the selected key', () => {
    const n3 = withResources().instances[2];
    const detail = instanceDetail(n3, ['CU', 'DISK'], 'DISK', true);

    expect(detail.keys).toEqual([
      { key: 'CU', ratio: 0.6, band: 'mid', text: '600 / 1000 (60%)' },
      { key: 'DISK', ratio: 0.6, band: 'mid', text: '1200 / 2000 (60%)' },
    ]);
    expect(detail.resources).toEqual([
      {
        name: 'db',
        used: '1600 DISK (80%)',
        replicas: '4 replicas, 1 top state',
      },
      {
        name: 'idx',
        used: '800 DISK (40%)',
        replicas: '2 replicas, 1 top state, 2 ERROR',
      },
      {
        name: 'legacy',
        used: 'not WAGED',
        replicas: '6 replicas, 1 top state',
      },
    ]);
  });
});
