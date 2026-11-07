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

const TITLES: Record<string, string> = {
  LIVE_INSTANCE_ON_INSTANCE_DROP: 'Instance is still live',
  RESOURCE_IN_USE_ON_RESOURCE_DROP: 'Resource is still in use',
  MIN_ACTIVE_REPLICA_ON_INSTANCE_DISABLE: 'Not enough active replicas left',
  INSTANCE_OPERATION_CAUSES_WAGED_UNPLACEABLE: 'Replicas could not be placed',
  INSTANCE_TAG_REMOVAL_CAUSES_WAGED_UNPLACEABLE: 'Replicas could not be placed',
  IDEAL_STATE_REBALANCE_FEASIBILITY: 'Rebalance would fail',
  INSTANCE_CAPACITY_BELOW_CLUSTER_DEMAND: 'Capacity below cluster demand',
  PARTITION_WEIGHT_EXCEEDS_INSTANCE_CAPACITY:
    'Partition weight exceeds capacity',
  STATE_MODEL_DEF_NOT_FOUND: 'State model not found',
  USER_DEFINED_REBALANCER_CLASS_INVALID: 'Invalid rebalancer class',
  MIN_ACTIVE_REPLICAS_EXCEEDS_REPLICAS: 'Min active replicas too high',
  WAGED_INSTANCE_CAPACITY_KEY_MISSING: 'Instance capacity key missing',
  RETIRED_CLUSTER_CONFIG: 'Retired cluster config',
};

export function ruleTitle(ruleId: string): string {
  return (
    TITLES[ruleId] ||
    ruleId
      .toLowerCase()
      .replace(/_/g, ' ')
      .replace(/^./, (c) => c.toUpperCase())
  );
}

// Splits a rule message into what happens and what to do about it. The
// trailing "or retry with force=true" hint is dropped, the dialog has a button.
export function splitMessage(message?: string): {
  cause: string;
  advice: string;
} {
  const text = (message || '')
    .replace(
      /,?\s*(or|and)?\s*(retry with|pass|use)\s+force=true[^.]*\.?/i,
      '.',
    )
    .replace(/\s+/g, ' ')
    .replace(/\.\./g, '.')
    .trim();
  const sentences = text.match(/[^.]+(?:\.|$)/g) || [];
  const isAdvice = (s: string) =>
    /^\s*(wait|add|stop|retry|disable|enable|fix|remove|set|use|check|lower|raise|reduce|increase|drop|ensure)\b/i.test(
      s,
    );
  const advice = sentences.filter(isAdvice).join('').trim();
  const cause = sentences
    .filter((s) => !isAdvice(s))
    .join('')
    .trim();
  return { cause: cause || advice, advice: cause ? advice : '' };
}
