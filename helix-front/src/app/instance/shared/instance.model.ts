export type InstanceOperationState =
  | 'ENABLE'
  | 'DISABLE'
  | 'EVACUATE'
  | 'SWAP_IN'
  | 'UNKNOWN';

export interface InstanceOperationEntry {
  operation: InstanceOperationState;
  source: string;
  reason: string;
  timestamp: number;
}

// One entry of GET /instances?command=getInstanceSummaries
export interface InstanceSummary {
  host: string;
  port: string;
  domain: string;
  tags: string[];
  capacity: Record<string, number>;
  instanceOperations: InstanceOperationEntry[];
  maintenanceMarkerExpiresAtMillis?: number;
}

// One entry of GET /instances?command=getInstanceErrorSummaries
export interface InstanceErrorSummary {
  errorCount: number;
  resources: Record<string, number>;
}

export class Instance {
  readonly name: string;
  readonly clusterName: string;
  readonly enabled: boolean;
  readonly liveInstance: boolean | string;
  readonly sessionId: string;
  readonly helixVersion: string;
  readonly operationState: InstanceOperationState;
  readonly summary?: InstanceSummary;

  get healthy(): boolean {
    return this.liveInstance && this.operationState === 'ENABLE';
  }

  get online(): boolean {
    return !!this.liveInstance;
  }

  constructor(
    name: string,
    clusterName: string,
    enabled: boolean,
    liveInstance: boolean | string,
    sessionId?: string,
    helixVersion?: string,
    operationState?: InstanceOperationState,
    summary?: InstanceSummary,
  ) {
    this.name = name;
    this.clusterName = clusterName;
    this.enabled = enabled;
    this.liveInstance = liveInstance;
    this.sessionId = sessionId;
    this.helixVersion = helixVersion;
    this.operationState = operationState || (enabled ? 'ENABLE' : 'DISABLE');
    this.summary = summary;
  }
}
