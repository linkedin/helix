import { TestBed, inject } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import {
  HttpClientTestingModule,
  HttpTestingController,
} from '@angular/common/http/testing';
import { Router } from '@angular/router';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { ResourceService } from './resource.service';

describe('ResourceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule],
      providers: [
        ResourceService,
        { provide: Router, useValue: { url: '/helix.default/Demo' } },
      ],
    });
  });

  it('should be ready', inject(
    [ResourceService],
    (service: ResourceService) => {
      expect(service).toBeTruthy();
    }
  ));

  it('reads the bulk summaries into sorted rows', async () => {
    const service = TestBed.inject(ResourceService);
    const http = TestBed.inject(HttpTestingController);
    const result = service.getSummaries('Demo').toPromise();
    const request = http.expectOne(
      '/api/helix/helix.default/clusters/Demo/resources?command=getResourceSummaries',
    );
    expect(request.request.method).toBe('GET');
    request.flush({
      id: 'Demo',
      resource_summaries: {
        b: { waged: true, hasExternalView: false, enabled: true },
        a: { waged: false, hasExternalView: true, enabled: false },
      },
    });
    const rows = await result;
    expect(rows.map((r) => [r.name, r.cluster, r.alive, r.waged])).toEqual([
      ['a', 'Demo', true, false],
      ['b', 'Demo', false, true],
    ]);
    http.verify();
  });

  it('rejects the plain list an older REST server returns', async () => {
    const service = TestBed.inject(ResourceService);
    const http = TestBed.inject(HttpTestingController);
    const result = service.getSummaries('Demo').toPromise();
    http
      .expectOne(
        '/api/helix/helix.default/clusters/Demo/resources?command=getResourceSummaries',
      )
      .flush({ id: 'Demo', idealStates: ['a'], externalViews: ['a'] });
    await expect(result).rejects.toMatch('does not support');
    http.verify();
  });
});
