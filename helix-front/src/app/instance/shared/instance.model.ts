export type InstanceOperationState =
  | 'ENABLE'
  | 'DISABLE'
  | 'EVACUATE'
  | 'SWAP_IN'
  | 'UNKNOWN';

export class Instance {
  readonly name: string;
  readonly clusterName: string;
  readonly enabled: boolean;
  readonly liveInstance: boolean | string;
  readonly sessionId: string;
  readonly helixVersion: string;
  readonly operationState: InstanceOperationState;

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
    operationState?: InstanceOperationState
  ) {
    this.name = name;
    this.clusterName = clusterName;
    this.enabled = enabled;
    this.liveInstance = liveInstance;
    this.sessionId = sessionId;
    this.helixVersion = helixVersion;
    this.operationState = operationState || (enabled ? 'ENABLE' : 'DISABLE');
  }
}
