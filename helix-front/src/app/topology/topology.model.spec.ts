import { describe, expect, it } from '@jest/globals';

import { snapshotFixture } from '../capacity/testing/snapshot.fixture';
import {
  average,
  buildTopology,
  groupIds,
  NO_DOMAIN_ID,
  parseDomain,
  TopologyNode,
  visibleRows,
} from './topology.model';

function tree(): TopologyNode[] {
  const [n1, n2, n3, n4] = snapshotFixture().instances;
  const instances = [
    n1,
    { ...n2, operation: 'EVACUATE' },
    n3,
    n4,
    { ...n1, name: 'n5' },
    { ...n1, name: 'n6', registered: false },
    { ...n1, name: 'n7' },
  ];
  const domains = new Map([
    ['n1', 'zone=z1,rack=r1,instance=n1'],
    ['n2', 'zone=z1,rack=r2,instance=n2'],
    // the last pair is not the instance name, so the host stays a group
    ['n3', 'zone=z10,host=h3'],
    ['n4', 'zone=z10,host=h3,instance=n4'],
    ['n5', 'zone'],
    ['n7', 'zone=z2,instance=n7'],
  ]);
  return buildTopology(instances, domains, ['CU', 'DISK']);
}

const ids = (nodes: { id: string }[]) => nodes.map((n) => n.id);

describe('topology model', () => {
  it('parses a domain in written order and rejects malformed ones', () => {
    expect(parseDomain(' zone=z1, rack = r2 ,instance=n1')).toEqual([
      ['zone', 'z1'],
      ['rack', 'r2'],
      ['instance', 'n1'],
    ]);
    const bad = [undefined, '', ' ', 'zone', 'zone=', '=z1', 'a=b,,c=d'];
    expect(bad.map((d) => parseDomain(d))).toEqual(bad.map(() => null));
  });

  it('nests instances by domain, sorts naturally and puts no domain last', () => {
    const [z1, z2, z10, none] = tree();
    expect(ids([z1, z2, z10, none])).toEqual([
      '/zone=z1',
      '/zone=z2',
      '/zone=z10',
      NO_DOMAIN_ID,
    ]);
    expect(ids(z1.children)).toEqual(['/zone=z1/rack=r1', '/zone=z1/rack=r2']);
    expect(ids(z1.children[0].children)).toEqual(['instance:n1']);
    expect(ids(z2.children)).toEqual(['instance:n7']);
    expect(ids(z10.children[0].children)).toEqual([
      'instance:n3',
      'instance:n4',
    ]);
    expect(z10.children[0].key).toBe('host');
    expect(none.label).toBe('no domain');
    expect(ids(none.children)).toEqual(['instance:n5', 'instance:n6']);
  });

  it('rolls up state, ERROR replicas and capacity without reading unknown as zero', () => {
    const [z1, , z10] = tree();
    expect(z1.stats).toMatchObject({
      instances: 2,
      live: 2,
      enabled: 1,
      disabled: 0,
      evacuating: 1,
      errorReplicas: 0,
    });
    expect(z1.stats.usage.CU).toEqual({
      worst: 0.9,
      used: 1200,
      max: 2000,
      unknown: 0,
    });
    expect(average(z1.stats.usage.CU)).toBe(0.6);

    // n4 is offline, disabled and has unknown capacity
    expect(z10.stats).toMatchObject({
      instances: 2,
      live: 1,
      enabled: 1,
      disabled: 1,
      errorReplicas: 2,
    });
    expect(z10.stats.usage.CU).toEqual({
      worst: 1.2,
      used: 1200,
      max: 1000,
      unknown: 1,
    });
    expect(average(z10.stats.usage.DISK)).toBe(1.2);

    const [onlyUnknown] = buildTopology(
      [snapshotFixture().instances[3]],
      new Map([['n4', 'zone=z3']]),
      ['CU'],
    );
    expect(onlyUnknown.stats.usage.CU.worst).toBeNull();
    expect(average(onlyUnknown.stats.usage.CU)).toBeNull();
  });

  it('shows expanded groups, and on search only matches with their ancestors', () => {
    const nodes = tree();
    expect(ids(visibleRows(nodes, new Set(), '').map((r) => r.node))).toEqual(
      ids(nodes),
    );
    const open = visibleRows(nodes, new Set(['/zone=z1']), '');
    expect(open.map((r) => [r.node.id, r.depth, r.open]).slice(0, 4)).toEqual([
      ['/zone=z1', 0, true],
      ['/zone=z1/rack=r1', 1, false],
      ['/zone=z1/rack=r2', 1, false],
      ['/zone=z2', 0, false],
    ]);
    expect(visibleRows(nodes, new Set(groupIds(nodes)), '')).toHaveLength(14);

    const search = (q: string) =>
      visibleRows(nodes, new Set(), q).map((r) => r.node.id);
    expect(search(' N2 ')).toEqual([
      '/zone=z1',
      '/zone=z1/rack=r2',
      'instance:n2',
    ]);
    expect(search('zone=z10')).toEqual([
      '/zone=z10',
      '/zone=z10/host=h3',
      'instance:n3',
      'instance:n4',
    ]);
    expect(search('nothing')).toEqual([]);
  });
});
