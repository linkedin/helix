import 'zone.js';
import 'zone.js/dist/zone-testing';

import { HttpErrorResponse, HttpHeaders } from '@angular/common/http';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';
import { NEVER, throwError } from 'rxjs';

import {
  ConvergenceService,
  ConvergenceTargetMissing,
  READ_TIMEOUT_MS,
} from './convergence.service';

describe('ConvergenceService', () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.useRealTimers());

  function read(response: any) {
    const http = { get: () => response } as any;
    const router = { url: '/helix/Demo' } as any;
    const errors: any[] = [];
    new ConvergenceService(router, http)
      .get('Demo', 'db')
      .subscribe({ error: (e) => errors.push(e) });
    return errors;
  }

  it('gives up on a read that never answers', () => {
    const errors = read(NEVER);
    jest.advanceTimersByTime(READ_TIMEOUT_MS - 1);
    expect(errors.length).toBe(0);
    jest.advanceTimersByTime(1);
    expect(errors.length).toBe(1);
  });

  it('keeps the message of a 404 from helix-rest', () => {
    const body = JSON.stringify({ error: 'Resource db does not exist' });
    const errors = read(
      throwError(new HttpErrorResponse({ status: 404, error: body })),
    );
    expect(errors[0]).toBeInstanceOf(ConvergenceTargetMissing);
    expect(errors[0].message).toBe('Resource db does not exist');
  });

  it('still reports a 404 without a message as not found', () => {
    const errors = read(
      throwError(new HttpErrorResponse({ status: 404, error: '<html>' })),
    );
    expect(errors).toEqual(['Not Found']);
  });

  it('does not take a 404 from the proxy for a missing target', () => {
    const errors = read(
      throwError(
        new HttpErrorResponse({
          status: 404,
          error: JSON.stringify({ error: 'no endpoint' }),
          headers: new HttpHeaders({
            'X-Helix-Front-Error': 'endpoint-not-configured',
          }),
        }),
      ),
    );
    expect(errors[0]).not.toBeInstanceOf(ConvergenceTargetMissing);
  });
});
