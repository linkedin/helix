import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';
import { TestBed } from '@angular/core/testing';
import {
  HttpClientTestingModule,
  HttpTestingController,
} from '@angular/common/http/testing';
import { Router } from '@angular/router';
import {
  InstanceDiagnosticsService,
  recordedErrorMessages,
} from './instance-diagnostics.service';
import { record } from '../../shared/models/znrecord.fixture';

describe('Instance diagnostic API contracts', () => {
  let service: InstanceDiagnosticsService;
  let http: HttpTestingController;

  beforeEach(() => {
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        InstanceDiagnosticsService,
        { provide: Router, useValue: { url: '/helix.default/Demo' } },
      ],
    });
    service = TestBed.inject(InstanceDiagnosticsService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    http.verify();
    jest.restoreAllMocks();
  });

  it('decodes NEW and READ messages without inventing execution status', async () => {
    const result = service.messages('Demo', 'node').toPromise();
    const request = http.expectOne(
      '/api/helix/helix.default/clusters/Demo/instances/node/messages',
    );
    expect(request.request.method).toBe('GET');
    request.flush({ new_messages: ['a'], read_messages: ['b'] });
    expect(await result).toEqual([
      { id: 'a', state: 'NEW' },
      { id: 'b', state: 'READ' },
    ]);
  });

  it('decodes all recorded sessions, keeping old records distinguishable', async () => {
    const result = service.errors('Demo', 'node').toPromise();
    http
      .expectOne((request) => request.url.endsWith('/errors'))
      .flush({
        errors: {
          old: { Orders: ['Orders_0'] },
          current: { Orders: ['Orders_1'] },
        },
      });
    expect(await result).toEqual([
      { session: 'old', resource: 'Orders', partition: 'Orders_0' },
      { session: 'current', resource: 'Orders', partition: 'Orders_1' },
    ]);
  });

  it('treats a missing error subtree as empty but does not hide permission failures', async () => {
    const empty = service.errors('Demo', 'node').toPromise();
    http
      .expectOne((request) => request.url.endsWith('/errors'))
      .flush('absent', { status: 404, statusText: 'Not Found' });
    expect(await empty).toEqual([]);
    const denied = service.errors('Demo', 'node').toPromise();
    http
      .expectOne((request) => request.url.endsWith('/errors'))
      .flush({ error: 'denied' }, { status: 403, statusText: 'Forbidden' });
    await expect(denied).rejects.toBe('denied');
  });

  it('rejects malformed indexes rather than returning a healthy-looking zero', async () => {
    const result = service.messages('Demo', 'node').toPromise();
    http
      .expectOne((request) => request.url.endsWith('/messages'))
      .flush({ new_messages: 'invalid', read_messages: [] });
    await expect(result).rejects.toThrow('Invalid message index');
  });

  it('escapes every error path segment and returns the unmodified record', async () => {
    const entry = {
      session: 's+1',
      resource: 'Orders & More',
      partition: 'p#1',
    };
    const result = service.error('Demo space', 'node+1', entry).toPromise();
    const request = http.expectOne(
      '/api/helix/helix.default/clusters/Demo%20space/instances/node%2B1/errors/s%2B1/Orders%20%26%20More/p%231',
    );
    const evidence = record('p#1', {
      listFields: { errors: ['failure stack'] },
    });
    request.flush(evidence);
    expect(await result).toEqual(evidence);
  });

  it('surfaces a disappeared detail rather than turning it into an empty record', async () => {
    const result = service.message('Demo', 'node', 'a').toPromise();
    http
      .expectOne((request) => request.url.endsWith('/messages/a'))
      .flush('absent', { status: 404, statusText: 'Not Found' });
    await expect(result).rejects.toBe('Not Found');
  });

  it('highlights literal recorded causes without changing the full stack record', () => {
    const evidence = record('error', {
      mapFields: {
        first: {
          AdditionalInfo:
            'InvocationTargetException\nCaused by: IOException: disk unavailable\n at handler',
        },
        duplicate: {
          AdditionalInfo:
            'InvocationTargetException\nCaused by: IOException: disk unavailable\n at handler',
        },
        other: { AdditionalInfo: 'Transition timed out\n at handler' },
      },
    });
    expect(recordedErrorMessages(evidence)).toEqual([
      'Caused by: IOException: disk unavailable',
      'Transition timed out',
    ]);
    expect(evidence.mapFields.first.AdditionalInfo).toContain('\n at handler');
  });

  it('uses the deepest explicit recorded cause while preserving missing messages as missing', () => {
    const evidence = record('error', {
      mapFields: {
        nested: {
          AdditionalInfo:
            'Wrapper\nCaused by: IOException\n\tCaused by: FileNotFoundException: shard data',
        },
        absent: { AdditionalInfo: null },
        empty: { AdditionalInfo: '\n  \t\n' },
        other: { Class: 'Handler' },
        missing: null,
      },
    });
    expect(recordedErrorMessages(evidence)).toEqual([
      'Caused by: FileNotFoundException: shard data',
    ]);
    expect(recordedErrorMessages(record('empty'))).toEqual([]);
  });
});
