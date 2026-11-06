import { describe, expect, it } from '@jest/globals';

import { isForceable, parseGuardrailVerdict } from './guardrail';

describe('guardrail verdict', () => {
  const v = (ruleId: string) => ({ ruleId, message: 'm' });

  it('parses a violations body, object or JSON string', () => {
    const body = { feasible: false, violations: [v('A')] };
    expect(parseGuardrailVerdict(body)).toEqual(body.violations);
    expect(parseGuardrailVerdict(JSON.stringify(body))).toEqual(
      body.violations,
    );
  });

  it('ignores other 400 bodies', () => {
    expect(parseGuardrailVerdict({ error: 'bad' })).toBeNull();
    expect(parseGuardrailVerdict('plain text')).toBeNull();
    expect(parseGuardrailVerdict({ violations: [] })).toBeNull();
  });

  it('allows force only when every rule is overridable', () => {
    expect(isForceable([v('LIVE_INSTANCE_ON_INSTANCE_DROP')])).toBe(true);
    expect(
      isForceable([
        v('LIVE_INSTANCE_ON_INSTANCE_DROP'),
        v('STATE_MODEL_DEF_NOT_FOUND'),
      ]),
    ).toBe(false);
    expect(isForceable([v('RETIRED_CLUSTER_CONFIG')])).toBe(false);
  });
});
