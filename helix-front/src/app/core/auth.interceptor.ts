import { Injectable } from '@angular/core';
import {
  HttpInterceptor,
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpErrorResponse,
} from '@angular/common/http';
import { Observable, EMPTY } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { MatDialog } from '@angular/material/dialog';
import { AlertDialogComponent } from '../shared/dialog/alert-dialog/alert-dialog.component';

@Injectable()
export class AuthInterceptor implements HttpInterceptor {
  constructor(private dialog: MatDialog) {}

  intercept(
    req: HttpRequest<any>,
    next: HttpHandler
  ): Observable<HttpEvent<any>> {
    return next.handle(req).pipe(
      catchError((error: HttpErrorResponse) => {
        if (error.status === 401) {
          this.dialog
            .open(AlertDialogComponent, {
              data: {
                title: 'Session Expired',
                message:
                  'Your session has expired. Please sign in again to continue.',
              },
            })
            .afterClosed()
            .subscribe(() => {
              fetch('/api/user/logout', { method: 'POST' }).finally(() =>
                window.location.reload()
              );
            });
          return EMPTY;
        }
        return new Observable<HttpEvent<any>>((subscriber) =>
          subscriber.error(error)
        );
      })
    );
  }
}
