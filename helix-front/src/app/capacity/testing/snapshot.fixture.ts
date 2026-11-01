import { CapacitySnapshot } from '../shared/capacity-snapshot.model';

function node(
  name: string,
  zone: string,
  used: number,
  extra: Partial<CapacitySnapshot['instances'][0]> = {},
): CapacitySnapshot['instances'][0] {
  return {
    name,
    zone,
    registered: true,
    live: true,
    enabled: true,
    operation: 'ENABLE',
    capacityStatus: 'KNOWN',
    max: { CU: 1000, DISK: 2000 },
    used: { CU: used, DISK: used * 2 },
    topStateUsed: { CU: used / 2, DISK: used },
    wagedPartitions: 10,
    nonWagedPartitions: 2,
    wagedTopStatePartitions: 4,
    nonWagedTopStatePartitions: 1,
    errorReplicas: 0,
    warnings: [],
    resources: [],
    ...extra,
  };
}

export function snapshotFixture(): CapacitySnapshot {
  return {
    cluster: 'Demo',
    capacityKeys: ['CU', 'DISK'],
    faultZoneType: 'zone',
    maxPartitionsPerInstance: null,
    wagedResourceCount: 2,
    nonWagedResourceCount: 1,
    generatedAt: '2026-11-01T10:00:00Z',
    collectionStartedAt: '2026-11-01T09:59:59Z',
    collectionDurationMs: 40,
    source: 'EXTERNAL_VIEW',
    controllerName: 'ctl',
    paused: false,
    maintenance: false,
    maintenanceReason: null,
    warnings: [],
    instances: [
      node('n1', 'z1', 300),
      node('n2', 'z2', 900),
      node('n3', 'z3', 1200, { errorReplicas: 2 }),
      node('n4', 'z3', 0, {
        live: false,
        enabled: false,
        operation: 'DISABLE',
        capacityStatus: 'UNKNOWN',
        max: null,
        used: null,
        topStateUsed: null,
      }),
    ],
    summary: {
      totalInstances: 4,
      liveInstances: 3,
      enabledInstances: 3,
      instancesWithUnknownCapacity: 1,
      instancesOverCapacity: 1,
      unregisteredEvHosts: 0,
      totalErrorReplicas: 2,
    },
  };
}
