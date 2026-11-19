import { describe, expect, it, jest } from '@jest/globals';
import {
  HttpErrorResponse,
  HttpParams,
  HttpRequest,
  HttpResponse,
} from '@angular/common/http';
import { of, throwError } from 'rxjs';

import {
  GuardrailBlocked,
  isForceable,
  parseGuardrailVerdict,
  splitMessage,
} from './guardrail';
import { GuardrailInterceptor } from './guardrail.interceptor';
import { HelperService } from '../shared/helper.service';

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
    // helix-rest documents force=true as the override for these
    expect(isForceable([v('WAGED_INSTANCE_CAPACITY_KEY_MISSING')])).toBe(true);
    expect(isForceable([v('USER_DEFINED_REBALANCER_CLASS_INVALID')])).toBe(
      true,
    );
  });

  describe('interceptor', () => {
    const blocked = new HttpErrorResponse({
      status: 400,
      error: { violations: [v('LIVE_INSTANCE_ON_INSTANCE_DROP')] },
    });
    const req = new HttpRequest('PUT', '/x', {}, { params: new HttpParams() });
    const run = (force: boolean) => {
      const sent: HttpRequest<any>[] = [];
      const next: any = {
        handle: (r: HttpRequest<any>) => {
          sent.push(r);
          return sent.length === 1
            ? throwError(blocked)
            : of(new HttpResponse({ status: 200 }));
        },
      };
      const dialog: any = { open: () => ({ afterClosed: () => of(force) }) };
      const result = { value: null as any, error: null as any };
      new GuardrailInterceptor(dialog).intercept(req, next).subscribe(
        (e) => (result.value = e),
        (e) => (result.error = e),
      );
      return { sent, ...result };
    };

    it('gives the caller a GuardrailBlocked error when the dialog is closed', () => {
      const { sent, value, error } = run(false);
      expect(sent).toHaveLength(1);
      expect(value).toBeNull();
      expect(error).toBeInstanceOf(GuardrailBlocked);
      expect(error.violations[0].ruleId).toBe('LIVE_INSTANCE_ON_INSTANCE_DROP');
    });

    it('re-sends with force=true when the user forces', () => {
      const { sent, value, error } = run(true);
      expect(sent[1].params.get('force')).toBe('true');
      expect(value.status).toBe(200);
      expect(error).toBeNull();
    });
  });

  it('confirms a blocked write with a snackbar, not a second error dialog', () => {
    const snackBar: any = { open: jest.fn() };
    const dialog: any = { open: jest.fn() };
    new HelperService(snackBar, dialog).showError(
      new GuardrailBlocked([v('A')]),
    );
    expect(dialog.open).not.toHaveBeenCalled();
    expect(snackBar.open.mock.calls[0][0]).toBe(
      'Not applied: the change was blocked by a guardrail.',
    );
  });

  it('splits cause from advice and drops the force hint', () => {
    const r = splitMessage(
      'Resource R is in use. Disable it first, or retry with force=true to override.',
    );
    expect(r.cause).toBe('Resource R is in use.');
    expect(r.advice).toContain('Disable it first');
    expect(r.advice).not.toContain('force=true');
  });
});
