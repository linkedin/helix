import { throwError as observableThrowError, Observable } from 'rxjs';

import { catchError } from 'rxjs/operators';
import { Injectable } from '@angular/core';
import { HttpHeaders, HttpClient, HttpParams, HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';

import { Settings } from './settings';

@Injectable()
export class HelixService {
  constructor(protected router: Router, private http: HttpClient) {}

  public can(): Observable<any> {
    return this.http
      .get(`${Settings.userAPI}/can`, { headers: this.getHeaders() })
      .pipe(catchError(this.errorHandler));
  }

  protected request(path: string, helix?: string): Observable<any> {
    if (helix == null) {
      helix = this.getHelixKey();
    }

    return this.http
      .get(`${Settings.helixAPI}${helix}${path}`, {
        headers: this.getHeaders(),
      })
      .pipe(catchError(this.errorHandler));
  }

  protected post(path: string, data: any, params?: HttpParams): Observable<any> {
    return this.http
      .post(`${Settings.helixAPI}${this.getHelixKey()}${path}`, data, {
        headers: this.getHeaders(),
        params,
      })
      .pipe(catchError(this.errorHandler));
  }

  protected put(path: string, data: string): Observable<any> {
    return this.http
      .put(`${Settings.helixAPI}${this.getHelixKey()}${path}`, data, {
        headers: this.getHeaders(),
      })
      .pipe(catchError(this.errorHandler));
  }

  protected delete(path: string): Observable<any> {
    return this.http
      .delete(`${Settings.helixAPI}${this.getHelixKey()}${path}`, {
        headers: this.getHeaders(),
      })
      .pipe(catchError(this.errorHandler));
  }

  protected getHelixKey(): string {
    // fetch helix key from url
    return `/${this.router.url.split('/')[1]}`;
  }

  protected getHeaders() {
    const headers = new HttpHeaders();
    headers.append('Accept', 'application/json');
    headers.append('Content-Type', 'application/json');
    return headers;
  }

  protected errorHandler(error: any) {
    console.error(error);

    let message = error.message || 'Cannot reach Helix restful service.';

    // Angular's HttpClient surfaces failures as HttpErrorResponse (not HttpResponse),
    // so match that type to parse status and body correctly.
    if (error instanceof HttpErrorResponse) {
      if (error.status === 0) {
        message =
          'No response from helix-rest. Check connectivity and the configured endpoint.';
      } else if (
        error.status === 404 &&
        error.headers?.get('X-Helix-Front-Error') !== 'endpoint-not-configured'
      ) {
        // a 404 from helix-rest itself; callers may treat it as "not found".
        // A 404 from the front-end proxy (endpoint not configured) is a real
        // failure and falls through to be reported with its own message.
        message = 'Not Found';
      } else {
        // the error body may be an already-parsed object, a JSON string, or plain text
        const body = error.error;
        if (
          body &&
          typeof body === 'object' &&
          typeof body.error === 'string'
        ) {
          message = body.error;
        } else if (typeof body === 'string' && body.length) {
          try {
            message = JSON.parse(body).error || body;
          } catch (e) {
            message = body;
          }
        } else if (error.message) {
          message = error.message;
        }
      }
    }

    return observableThrowError(message);
  }
}
