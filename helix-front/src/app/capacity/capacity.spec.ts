import 'zone.js';
import 'zone.js/dist/zone-testing';

import { TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { RouterTestingModule } from '@angular/router/testing';
import {
  HttpClientTestingModule,
  HttpTestingController,
} from '@angular/common/http/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { of, throwError } from 'rxjs';

import { CapacityService, SNAPSHOT_TIMEOUT_MS } from './shared/capacity.service';
import { CapacityOverviewComponent } from './capacity-overview/capacity-overview.component';
import { HelperService } from '../shared/helper.service';
import { snapshotFixture } from './testing/snapshot.fixture';

describe('CapacityService', () => {
  it('requests the snapshot with the stale hint', async () => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule],
      providers: [
        CapacityService,
        { provide: Router, useValue: { url: '/helix.default/Demo' } },
      ],
    });
    const http = TestBed.inject(HttpTestingController);
    const result = TestBed.inject(CapacityService)
      .getSnapshot('Demo', 0)
      .toPromise();
    const request = http.expectOne(
      '/api/helix/helix.default/clusters/Demo/capacity/heatmap?maxStaleMs=0',
    );
    expect(request.request.method).toBe('GET');
    request.flush(snapshotFixture());
    expect((await result).cluster).toBe('Demo');
    http.verify();
  });

  it('waits past the old 45 second limit and gives up after the server budget', () => {
    jest.useFakeTimers();
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule],
      providers: [
        CapacityService,
        { provide: Router, useValue: { url: '/helix.default/Demo' } },
      ],
    });
    const http = TestBed.inject(HttpTestingController);
    const service = TestBed.inject(CapacityService);
    const url = '/api/helix/helix.default/clusters/Demo/capacity/heatmap';
    let cluster = '';
    let failure: any = null;

    service.getSnapshot('Demo').subscribe((s) => (cluster = s.cluster));
    jest.advanceTimersByTime(60000);
    http.expectOne(url).flush(snapshotFixture());
    expect(cluster).toBe('Demo');

    service.getSnapshot('Demo').subscribe({ error: (e) => (failure = e) });
    jest.advanceTimersByTime(SNAPSHOT_TIMEOUT_MS - 1);
    expect(failure).toBeNull();
    jest.advanceTimersByTime(1);
    expect(failure).not.toBeNull();
    jest.useRealTimers();
  });
});

describe('CapacityOverviewComponent', () => {
  const getSnapshot = jest.fn();
  const showError = jest.fn();

  function create() {
    TestBed.configureTestingModule({
      declarations: [CapacityOverviewComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { snapshot: { params: { name: 'Demo' } } } },
        },
        { provide: CapacityService, useValue: { getSnapshot } },
        { provide: HelperService, useValue: { showError } },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    });
    const fixture = TestBed.createComponent(CapacityOverviewComponent);
    fixture.detectChanges();
    return fixture;
  }

  beforeEach(() => {
    getSnapshot.mockReset();
    showError.mockReset();
  });

  it('sorts hottest first, unknown last, and never shows unknown as zero', () => {
    getSnapshot.mockReturnValue(of(snapshotFixture()));
    const fixture = create();
    const component = fixture.componentInstance;

    expect(component.key).toBe('CU');
    expect(component.rows.map((r) => r.instance.name)).toEqual([
      'n3',
      'n2',
      'n1',
      'n4',
    ]);
    expect(component.rows.map((r) => component.ratioText(r))).toEqual([
      '120%',
      '90%',
      '30%',
      'unknown',
    ]);
    expect(component.usedText(component.rows[3])).toBe('');
  });

  it('switches to top state usage and another capacity key', () => {
    getSnapshot.mockReturnValue(of(snapshotFixture()));
    const component = create().componentInstance;

    component.setTopStateOnly(true);
    expect(component.ratioText(component.rows[0])).toBe('60%');
    component.setKey('DISK');
    expect(component.usedText(component.rows[0])).toBe('1200 / 2000');
  });

  it('shows paused and maintenance banners and reports a load failure', () => {
    getSnapshot.mockReturnValue(
      of({ ...snapshotFixture(), paused: true, maintenance: true }),
    );
    const html: HTMLElement = create().nativeElement;
    expect(html.querySelector('[data-testid=paused]')).not.toBeNull();
    expect(html.querySelector('[data-testid=maintenance]')).not.toBeNull();

    TestBed.resetTestingModule();
    getSnapshot.mockReturnValue(throwError('boom'));
    const component = create().componentInstance;
    expect(component.error).toBe('boom');
    expect(showError).toHaveBeenCalledWith('boom');
  });
});
