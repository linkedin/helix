import { describe, expect, it } from '@jest/globals';

import { CapacitySnapshot } from '../../capacity/shared/capacity-snapshot.model';
import { snapshotFixture } from '../../capacity/testing/snapshot.fixture';
import {
  Instance,
  InstanceOperationState,
  InstanceSummary,
} from '../../instance/shared/instance.model';
import { ResourceSummary } from '../../resource/shared/resource.model';
import { DiagnosticIssue, diagnosticIssues } from './diagnostic-rules';

type Row = CapacitySnapshot['instances'][0];

const row = (extra: Partial<Row> = {}): Row => ({
  ...snapshotFixture().instances[0],
  ...extra,
});

const snapshot = (extra: Partial<CapacitySnapshot> = {}) => ({
  ...snapshotFixture(),
  instances: [row()],
  ...extra,
});

const instance = (
  name: string,
  live = true,
  operation: InstanceOperationState = 'ENABLE',
  ...ops: [string, string, number?][]
) =>
  new Instance(name, 'Demo', operation === 'ENABLE', live, '', '', operation, {
    instanceOperations: ops.map(([source, reason, timestamp]) => ({
      operation,
      source,
      reason,
      timestamp: timestamp ?? Date.parse('2026-11-01T10:05:00Z'),
    })),
  } as InstanceSummary);

const withCreation = (i: Instance, createdAtMillis: number) =>
  new Instance(
    i.name,
    'Demo',
    i.enabled,
    i.liveInstance,
    '',
    '',
    i.operationState,
    {
      ...i.summary,
      createdAtMillis,
    } as InstanceSummary,
  );

const resource = (name: string, extra: Partial<ResourceSummary> = {}) =>
  ({
    name,
    rebalanceMode: 'FULL_AUTO',
    enabled: true,
    waged: true,
    hasExternalView: true,
    ...extra,
  }) as ResourceSummary;

const lines = (issues: DiagnosticIssue[]) =>
  issues.map(
    (i) =>
      `${i.severity} ${i.category} ${i.subject} /${i.link.join('/')}: ${i.title}. ${i.detail}`,
  );

describe('diagnosticIssues', () => {
  it('reports nothing for a healthy cluster', () => {
    expect(
      diagnosticIssues(snapshot(), [instance('n1')], [resource('db')]),
    ).toEqual([]);
  });

  it.each([
    [
      'critical cluster Demo /controller: No controller leader. No controller holds leadership, so nothing is rebalanced or recovered.',
      { controllerName: null },
    ],
    [
      'warning cluster Demo /controller: Cluster paused. The controller is paused and does not change any assignment.',
      { paused: true },
    ],
    [
      'warning cluster Demo /controller: Cluster in maintenance mode. Reason: upgrade',
      { maintenance: true, maintenanceReason: 'upgrade' },
    ],
    [
      'warning cluster Demo /controller: Cluster in maintenance mode. No reason recorded.',
      { maintenance: true },
    ],
  ])('%s', (expected, extra) => {
    expect(lines(diagnosticIssues(snapshot(extra), [], []))).toEqual([
      expected,
    ]);
  });

  it.each([
    [
      'info resource db /resources/db: Resource disabled. Disabled in its ideal state, so its replicas are taken offline.',
      { enabled: false, hasExternalView: false },
    ],
    [
      'critical resource db /resources/db: No external view. Enabled WAGED resource with no published external view, so no replica is confirmed serving.',
      { hasExternalView: false },
    ],
    [
      'critical resource db /resources/db: No external view. Enabled SEMI_AUTO resource with no published external view, so no replica is confirmed serving.',
      { hasExternalView: false, waged: false, rebalanceMode: 'SEMI_AUTO' },
    ],
  ])('%s', (expected, extra) => {
    expect(
      lines(diagnosticIssues(snapshot(), [], [resource('db', extra)])),
    ).toEqual([expected]);
  });

  it.each([
    [
      'warning instance n1 /instances/n1: Instance disabled. No stored operation matches, so this is the legacy HELIX_ENABLED flag. No source or reason recorded.',
      instance('n1', true, 'DISABLE'),
    ],
    [
      'warning instance n1 /instances/n1: Instance evacuating. Set by ops since 2026-11-01 10:05 UTC: decommission',
      instance('n1', true, 'EVACUATE', ['old', 'old'], ['ops', 'decommission']),
    ],
    [
      'warning instance n1 /instances/n1: Instance swapping in. No stored operation matches, so this is the legacy HELIX_ENABLED flag. No source or reason recorded.',
      instance('n1', true, 'SWAP_IN'),
    ],
    [
      'warning instance n1 /instances/n1: Instance operation UNKNOWN. Set by an unknown source, time not recorded.',
      instance('n1', true, 'UNKNOWN', ['', '', 0]),
    ],
    [
      'warning instance n1 /instances/n1: Instance operation UNKNOWN. Set by acm, time not recorded: joined',
      instance('n1', true, 'UNKNOWN', ['acm', 'joined', -1]),
    ],
    [
      'warning instance n1 /instances/n1: Instance operation UNKNOWN. Set by acm since 2026-10-30 08:00 UTC (instance config created): joined',
      withCreation(
        instance('n1', true, 'UNKNOWN', ['acm', 'joined', -1]),
        Date.parse('2026-10-30T08:00:00Z'),
      ),
    ],
    [
      'warning instance n1 /instances/n1: Instance disabled. No stored operation matches, so this is the legacy HELIX_ENABLED flag. No source or reason recorded.',
      new Instance('n1', 'Demo', false, true, '', '', 'DISABLE', {
        instanceOperations: [
          {
            operation: 'ENABLE',
            source: 'old-source',
            reason: 'old reason',
            timestamp: Date.parse('2026-11-01T10:05:00Z'),
          },
        ],
      } as InstanceSummary),
    ],
    [
      'critical instance n1 /instances/n1: Instance not live. Enabled, but has no live ZooKeeper session.',
      instance('n1', false),
    ],
    [
      'warning instance n1 /instances/n1: Instance not live. No live ZooKeeper session. Operation DISABLE. Set by acm since 2026-11-01 10:05 UTC: host repair',
      instance('n1', false, 'DISABLE', ['acm', 'host repair']),
    ],
  ])('%s', (expected, subject) => {
    expect(lines(diagnosticIssues(snapshot(), [subject], []))).toEqual([
      expected,
    ]);
  });

  it.each([
    [
      'critical errors n1 /instances/n1/diagnostics: 3 ERROR replicas. In the external view of db (2), idx (1).',
      3,
      { db: 2, idx: 1, ok: 0 },
    ],
    [
      'critical errors n1 /instances/n1/diagnostics: 1 ERROR replica. In the published external view.',
      1,
      {},
    ],
  ])('%s', (expected, errorReplicas, byResource: Record<string, number>) => {
    const resources = Object.entries(byResource).map(([name, errors]) => ({
      resourceName: name,
      errorReplicas: errors,
      waged: true,
      replicas: 1,
      topStateReplicas: 0,
      used: null,
    }));
    const issues = diagnosticIssues(
      snapshot({ instances: [row({ errorReplicas, resources })] }),
      [],
      [],
    );
    expect(lines(issues)).toEqual([expected]);
  });

  // unknown capacity is described, never rendered as a number
  it.each([
    [
      'warning capacity n1 /capacity: Over WAGED capacity. CU 1200 / 1000 (120%). Partition weights, not CPU or disk usage.',
      { used: { CU: 1200, DISK: 2000 } },
    ],
    [
      'warning capacity n1 /capacity: Over WAGED capacity. CU 5 / 0. Partition weights, not CPU or disk usage.',
      { max: { CU: 0, DISK: 2000 }, used: { CU: 5, DISK: 0 } },
    ],
    [
      'info capacity n1 /capacity: Capacity unknown. Capacity is not fully configured for CU, DISK.',
      { capacityStatus: 'UNKNOWN', max: null, used: null },
    ],
    [
      'info capacity n1 /capacity: Capacity unknown. A hosted WAGED partition weight cannot be resolved, so usage is unknown.',
      { capacityStatus: 'UNKNOWN', used: null },
    ],
    [
      'info capacity n1 /capacity: Capacity unknown. Hosts replicas in an external view but has no instance config.',
      { capacityStatus: 'UNKNOWN', registered: false, max: null, used: null },
    ],
  ] as [string, Partial<Row>][])('%s', (expected, extra) => {
    const issues = diagnosticIssues(
      snapshot({ instances: [row(extra)] }),
      [],
      [],
    );
    expect(lines(issues)).toEqual([expected]);
  });

  it.each([
    [{ used: { CU: 1000, DISK: 2000 } }],
    [{ capacityStatus: 'NOT_APPLICABLE', max: null, used: null }],
  ] as [Partial<Row>][])('no capacity issue for %j', (extra) => {
    const issues = diagnosticIssues(
      snapshot({ instances: [row(extra)] }),
      [],
      [],
    );
    expect(issues).toEqual([]);
  });

  it('orders by severity, then category, then subject', () => {
    const issues = diagnosticIssues(
      { ...snapshotFixture(), controllerName: null },
      [
        instance('h10', false),
        instance('h2', false),
        instance('n4', false, 'DISABLE'),
      ],
      [
        resource('old', { enabled: false }),
        resource('db', { hasExternalView: false }),
      ],
    );
    expect(
      issues.map((i) => `${i.severity} ${i.category} ${i.subject}`),
    ).toEqual([
      'critical cluster Demo',
      'critical errors n3',
      'critical resource db',
      'critical instance h2',
      'critical instance h10',
      'warning instance n4',
      'warning capacity n3',
      'info resource old',
      'info capacity n4',
    ]);
  });
});
