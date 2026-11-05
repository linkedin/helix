import { Injectable } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError as observableThrowError } from 'rxjs';
import { timeout } from 'rxjs/operators';

import { HelixService } from '../../core/helix.service';
import { ConvergenceReport } from './convergence.model';

export const READ_TIMEOUT_MS = 15000;

/** helix-rest answered 404 and said which cluster or resource it could not find. */
export class ConvergenceTargetMissing {
  constructor(public message: string) {}
}

@Injectable()
export class ConvergenceService extends HelixService {
  get(cluster: string, resource?: string): Observable<ConvergenceReport> {
    return this.request(
      resource
        ? `/clusters/${cluster}/resources/${resource}/convergence`
        : // the panel shows the summary only, so it skips the list of resources
          `/clusters/${cluster}/convergence?limit=0`,
    ).pipe(timeout(READ_TIMEOUT_MS));
  }

  // Runs unbound as a catchError callback, so it cannot use `this`.
  protected errorHandler(error: any) {
    const message = missingTargetMessage(error);
    return message
      ? observableThrowError(new ConvergenceTargetMissing(message))
      : super.errorHandler(error);
  }
}

function missingTargetMessage(error: any): string | null {
  if (
    !(error instanceof HttpErrorResponse) ||
    error.status !== 404 ||
    // a 404 from the front-end proxy is a missing endpoint, not a missing target
    error.headers?.get('X-Helix-Front-Error') === 'endpoint-not-configured'
  ) {
    return null;
  }
  let body = error.error;
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (e) {
      return null;
    }
  }
  return typeof body?.error === 'string' ? body.error : null;
}
