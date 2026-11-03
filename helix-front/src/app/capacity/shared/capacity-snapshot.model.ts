export type CapacityValues = Record<string, number> | null;

// Mirrors the JSON of GET /clusters/{cluster}/capacity/heatmap. Null max, used
// or topStateUsed means unknown, never zero.
export interface InstanceResourceUsage {
  resourceName: string;
  waged: boolean;
  replicas: number;
  topStateReplicas: number;
  errorReplicas: number;
  used: CapacityValues;
}

export interface InstanceCapacity {
  name: string;
  zone: string | null;
  registered: boolean;
  live: boolean;
  enabled: boolean;
  operation: string | null;
  capacityStatus: 'KNOWN' | 'UNKNOWN' | 'NOT_APPLICABLE';
  max: CapacityValues;
  used: CapacityValues;
  topStateUsed: CapacityValues;
  wagedPartitions: number;
  nonWagedPartitions: number;
  wagedTopStatePartitions: number;
  nonWagedTopStatePartitions: number;
  errorReplicas: number;
  warnings: string[];
  resources: InstanceResourceUsage[];
}

export interface CapacitySummary {
  totalInstances: number;
  liveInstances: number;
  enabledInstances: number;
  instancesWithUnknownCapacity: number;
  instancesOverCapacity: number;
  unregisteredEvHosts: number;
  totalErrorReplicas: number;
}

export interface CapacitySnapshot {
  cluster: string;
  capacityKeys: string[];
  faultZoneType: string | null;
  maxPartitionsPerInstance: number | null;
  wagedResourceCount: number;
  nonWagedResourceCount: number;
  generatedAt: string;
  collectionStartedAt: string;
  collectionDurationMs: number;
  source: string;
  controllerName: string | null;
  paused: boolean;
  maintenance: boolean;
  maintenanceReason: string | null;
  warnings: string[];
  instances: InstanceCapacity[];
  summary: CapacitySummary;
}
