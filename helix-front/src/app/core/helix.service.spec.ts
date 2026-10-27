import { TestBed, inject } from '@angular/core/testing';
import { HttpClientModule, HttpErrorResponse } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { HelixService } from './helix.service';

describe('HelixService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule, RouterTestingModule],
      providers: [HelixService],
    });
  });

  it('should be ready', inject([HelixService], (service: HelixService) => {
    expect(service).toBeTruthy();
  }));

  // the error handler runs as a bare function reference in catchError, so we can
  // exercise it directly through a subclass that exposes it
  class ProbeService extends HelixService {
    run(error: any): Promise<string> {
      return new Promise((resolve) => {
        (this as any).errorHandler(error).subscribe({
          error: (message: string) => resolve(message),
        });
      });
    }
  }

  it('maps a 404 to Not Found so callers can treat it as empty', async () => {
    const service = new ProbeService(null as any, null as any);
    const message = await service.run(
      new HttpErrorResponse({ status: 404, error: 'missing' }),
    );
    expect(message).toBe('Not Found');
  });

  it('explains a transport failure without claiming that an operation failed on the server', async () => {
    const service = new ProbeService(null, null);
    const message = await service.run(new HttpErrorResponse({ status: 0 }));
    expect(message).toBe(
      'No response from helix-rest. Check connectivity and the configured endpoint.',
    );
  });

  it('reads an error field from an already-parsed object body', async () => {
    const service = new ProbeService(null as any, null as any);
    const message = await service.run(
      new HttpErrorResponse({ status: 500, error: { error: 'boom happened' } }),
    );
    expect(message).toBe('boom happened');
  });

  it('reads an error field from a JSON string body', async () => {
    const service = new ProbeService(null as any, null as any);
    const message = await service.run(
      new HttpErrorResponse({
        status: 500,
        error: JSON.stringify({ error: 'stringified boom' }),
      }),
    );
    expect(message).toBe('stringified boom');
  });

  it('falls back to a plain text body when it is not JSON', async () => {
    const service = new ProbeService(null as any, null as any);
    const message = await service.run(
      new HttpErrorResponse({ status: 500, error: 'just text' }),
    );
    expect(message).toBe('just text');
  });
});
