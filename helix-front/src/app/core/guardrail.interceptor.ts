import { Injectable } from '@angular/core';
import {
  HttpInterceptor,
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpErrorResponse,
} from '@angular/common/http';
import { Observable, EMPTY, throwError } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';
import { MatDialog } from '@angular/material/dialog';

import { isForceable, parseGuardrailVerdict } from './guardrail';
import { GuardrailDialogComponent } from '../shared/dialog/guardrail-dialog/guardrail-dialog.component';

// Shows the verdict when helix-rest blocks a mutation, and for overridable
// rules re-sends the same request with force=true if the user confirms.
@Injectable()
export class GuardrailInterceptor implements HttpInterceptor {
  constructor(private dialog: MatDialog) {}

  intercept(
    req: HttpRequest<any>,
    next: HttpHandler,
  ): Observable<HttpEvent<any>> {
    return next.handle(req).pipe(
      catchError((error: HttpErrorResponse) => {
        const violations =
          req.method !== 'GET' && error.status === 400
            ? parseGuardrailVerdict(error.error)
            : null;
        if (!violations) {
          return throwError(error);
        }
        return this.dialog
          .open(GuardrailDialogComponent, { data: { violations } })
          .afterClosed()
          .pipe(
            switchMap((force) =>
              force && isForceable(violations)
                ? next.handle(
                    req.clone({ params: req.params.set('force', 'true') }),
                  )
                : EMPTY,
            ),
          );
      }),
    );
  }
}
