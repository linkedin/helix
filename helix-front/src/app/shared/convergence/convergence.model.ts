// Response of GET /clusters/{cluster}/convergence and
// GET /clusters/{cluster}/resources/{resource}/convergence.
export type ConvergenceStatusName =
  | 'CONVERGED'
  | 'IN_PROGRESS'
  | 'BLOCKED'
  | 'UNKNOWN'
  | 'PAUSED';

export interface ConvergenceReport {
  clusterId: string;
  resourceName?: string;
  status: ConvergenceStatusName;
  primaryReason: string;
  // FRESH, or why the report cannot be trusted; only FRESH carries the fields below
  reportState: string;
  generatedAt?: number;
  // when the controller last ran its pipeline; it moves even when nothing changed
  evaluatedAt?: number;
  publishedBy?: string;
  unconvergedSince?: number;
  targetFreshness?: string;
  optimizerStatus?: string;
  resourceCount?: number;
  partitionSummary?: {
    total: number;
    affected: number;
    statusCounts: Partial<Record<ConvergenceStatusName, number>>;
    reasonCounts: Record<string, number>;
  };
}

// most severe first, the order of the bar and the legend
export const STATUS_ORDER: ConvergenceStatusName[] = [
  'BLOCKED',
  'UNKNOWN',
  'IN_PROGRESS',
  'PAUSED',
  'CONVERGED',
];

export const STATUS_LABELS: Record<ConvergenceStatusName, string> = {
  CONVERGED: 'Converged',
  IN_PROGRESS: 'In progress',
  BLOCKED: 'Blocked',
  UNKNOWN: 'Unknown',
  PAUSED: 'Paused',
};

export const REASON_TEXT: Record<string, string> = {
  PENDING_TRANSITION:
    'A transition message is on the participant and has not finished.',
  CANCELLATION_PENDING: 'A transition is being cancelled.',
  RELAY_PENDING: 'A relay message has not finished.',
  TRANSITION_DISPATCHED: 'The controller sent a transition in its last run.',
  MESSAGE_THROTTLED: 'A transition is ready but held back by a throttle.',
  STATE_CONSTRAINT_WAIT:
    'A transition waits on a state constraint, such as one leader at a time.',
  MESSAGE_DISPATCH_FAILED: 'The controller could not send the message.',
  ERROR_STATE: 'A replica is in ERROR and needs a reset.',
  TARGET_INSTANCE_NOT_LIVE: 'The target instance has no live session.',
  WAITING_FOR_DELAY:
    'The target instance is down or disabled and the delayed rebalance window is still open.',
  NO_PROGRESS_PATH:
    'The current state differs from the target and no message would close the gap.',
  TARGET_ASSIGNMENT_MISSING: 'There is no target assignment to compare with.',
  UNDER_REPLICATED:
    'Fewer replicas are placed than the ideal state asks for, and none is on its way.',
  OUTSIDE_PREFERENCE_LIST:
    'A replica stays on an instance outside the preference list, for example because the new instance has no capacity.',
  INVALID_STATE: 'A replica reports a state its state model does not define.',
  MAINTENANCE_MODE:
    'The cluster is in maintenance mode, so nothing is compared.',
  MANAGEMENT_MODE: 'The cluster is in management mode, so nothing is compared.',
  WAGED_LAST_KNOWN_GOOD:
    'WAGED could not compute and serves its last good assignment.',
  WAGED_CAPACITY_DEFICIT:
    'WAGED cannot fit the replicas into the capacity of the instances.',
  WAGED_NO_CANDIDATE_NODE: 'WAGED found no instance that can take a replica.',
  WAGED_INVALID_CONFIGURATION:
    'WAGED rejected the cluster or resource configuration.',
  WAGED_INTERNAL_FAILURE: 'WAGED failed while computing the assignment.',
};

// why a report is not shown, by reportState
export const REPORT_STATE_TEXT: Record<string, string> = {
  MONITORING_DISABLED:
    'Convergence monitoring is off for this cluster. CONVERGENCE_MONITORING_ENABLED is false in the cluster config.',
  REPORT_MISSING:
    'The controller has not published a report yet. A controller with this feature writes one on its next pipeline run.',
  SCHEMA_UNSUPPORTED: 'The report is in a format this REST server cannot read.',
  CONTROLLER_LEADER_MISSING:
    'There is no controller leader, so no report can be trusted.',
  CONTROLLER_SESSION_MISMATCH:
    'The report is from an earlier leader and the current one has not confirmed it yet.',
  REPORT_STALE:
    'The controller has stopped updating its report. Its pipeline may be stuck, so the status below is not shown.',
  MANAGEMENT_MODE: REASON_TEXT.MANAGEMENT_MODE,
};

// PENDING_TRANSITION -> "Pending transition", WAGED_LAST_KNOWN_GOOD -> "WAGED last known good"
export function reasonLabel(reason: string): string {
  const text = reason.toLowerCase().replace(/_/g, ' ');
  return text.startsWith('waged ')
    ? 'WAGED' + text.slice(5)
    : text.charAt(0).toUpperCase() + text.slice(1);
}

// "45s", "4m 12s", "3h 5m", "2d 4h"
export function duration(ms: number): string {
  let rest = Math.max(0, Math.floor(ms / 1000));
  const parts: string[] = [];
  for (const [size, unit] of [
    [86400, 'd'],
    [3600, 'h'],
    [60, 'm'],
    [1, 's'],
  ] as [number, string][]) {
    if (parts.length || rest >= size || size === 1) {
      parts.push(`${Math.floor(rest / size)}${unit}`);
      rest %= size;
    }
    if (parts.length === 2) break;
  }
  return parts.join(' ');
}
