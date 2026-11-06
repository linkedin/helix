export interface GuardrailViolation {
  ruleId: string;
  resourceName?: string;
  partitionName?: string;
  message?: string;
  indeterminate?: boolean;
}

// Rules guarding capacity, placement and liveness, where an operator can
// knowingly override. Rules that reject invalid config are never forceable.
const FORCEABLE_RULES = new Set([
  'LIVE_INSTANCE_ON_INSTANCE_DROP',
  'RESOURCE_IN_USE_ON_RESOURCE_DROP',
  'MIN_ACTIVE_REPLICA_ON_INSTANCE_DISABLE',
  'INSTANCE_OPERATION_CAUSES_WAGED_UNPLACEABLE',
  'INSTANCE_TAG_REMOVAL_CAUSES_WAGED_UNPLACEABLE',
  'IDEAL_STATE_REBALANCE_FEASIBILITY',
  'INSTANCE_CAPACITY_BELOW_CLUSTER_DEMAND',
  'PARTITION_WEIGHT_EXCEEDS_INSTANCE_CAPACITY',
]);

export function isForceable(violations: GuardrailViolation[]): boolean {
  return violations.every((v) => FORCEABLE_RULES.has(v.ruleId));
}

// helix-rest answers a blocked mutation with 400 and the verdict as the body.
export function parseGuardrailVerdict(body: any): GuardrailViolation[] | null {
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (e) {
      return null;
    }
  }
  if (
    body &&
    typeof body === 'object' &&
    Array.isArray(body.violations) &&
    body.violations.length &&
    body.violations.every((v: any) => v && typeof v.ruleId === 'string')
  ) {
    return body.violations;
  }
  return null;
}
