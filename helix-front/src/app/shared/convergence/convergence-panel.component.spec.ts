import 'zone.js';
import 'zone.js/dist/zone-testing';

import { TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';
import { of, throwError } from 'rxjs';

import {
  ConvergencePanelComponent,
  POLL_MS,
} from './convergence-panel.component';
import { ConvergenceReport, duration, reasonLabel } from './convergence.model';
import {
  ConvergenceService,
  ConvergenceTargetMissing,
} from './convergence.service';

describe('ConvergencePanelComponent', () => {
  const get = jest.fn<any>();
  const fresh = (extra: Partial<ConvergenceReport>): ConvergenceReport => ({
    clusterId: 'Demo',
    status: 'CONVERGED',
    primaryReason: 'NONE',
    reportState: 'FRESH',
    generatedAt: 1700000000000,
    publishedBy: 'controller_1',
    unconvergedSince: 0,
    targetFreshness: 'CURRENT',
    optimizerStatus: 'HEALTHY',
    resourceCount: 2,
    partitionSummary: {
      total: 24,
      affected: 0,
      statusCounts: { CONVERGED: 24 },
      reasonCounts: {},
    },
    ...extra,
  });

  function create(resource?: string) {
    TestBed.configureTestingModule({
      declarations: [ConvergencePanelComponent],
      providers: [{ provide: ConvergenceService, useValue: { get } }],
      schemas: [NO_ERRORS_SCHEMA],
    });
    const fixture = TestBed.createComponent(ConvergencePanelComponent);
    fixture.componentInstance.cluster = 'Demo';
    fixture.componentInstance.resource = resource;
    fixture.componentInstance.ngOnChanges();
    fixture.detectChanges();
    const text = (id: string) =>
      fixture.nativeElement
        .querySelector(`[data-testid="${id}"]`)
        ?.textContent.replace(/\s+/g, ' ')
        .trim();
    return { fixture, text };
  }

  beforeEach(() => {
    jest.useFakeTimers();
    get.mockReset();
    TestBed.resetTestingModule();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('keeps reading a converged cluster, but not while the tab is hidden', () => {
    get.mockReturnValue(of(fresh({})));
    const { text } = create();

    expect(get).toHaveBeenCalledWith('Demo', undefined);
    expect(text('convergence-status')).toBe('Converged');
    expect(text('convergence-headline')).toBe(
      'All 24 partitions across 2 resources match the target assignment.',
    );
    jest.advanceTimersByTime(POLL_MS);
    expect(get).toHaveBeenCalledTimes(2);

    const hidden = jest.spyOn(document, 'hidden', 'get').mockReturnValue(true);
    jest.advanceTimersByTime(POLL_MS * 3);
    expect(get).toHaveBeenCalledTimes(2);
    hidden.mockRestore();
    jest.advanceTimersByTime(POLL_MS);
    expect(get).toHaveBeenCalledTimes(3);
  });

  it('puts the most urgent reason first even when another reason covers more partitions', () => {
    get.mockReturnValueOnce(
      of(
        fresh({
          status: 'BLOCKED',
          primaryReason: 'TARGET_INSTANCE_NOT_LIVE',
          unconvergedSince: Date.now() - 252000,
          resourceCount: undefined,
          partitionSummary: {
            total: 8,
            affected: 4,
            statusCounts: { CONVERGED: 4, BLOCKED: 3, UNKNOWN: 1 },
            reasonCounts: {
              PENDING_TRANSITION: 2,
              TARGET_INSTANCE_NOT_LIVE: 1,
            },
          },
        }),
      ),
    );
    get.mockReturnValueOnce(of(fresh({})));
    const { fixture, text } = create('db');

    expect(get).toHaveBeenCalledWith('Demo', 'db');
    expect(text('convergence-headline')).toBe(
      '3 of 8 partitions are not converged, 1 unknown. Most urgent: The target instance has no live session.',
    );
    expect(text('convergence-since')).toBe('for 4m 12s');
    expect(text('convergence-legend')).toBe('Blocked 3Unknown 1Converged 4');
    const reasons = fixture.nativeElement.querySelectorAll(
      '[data-testid="convergence-reason"] li',
    );
    expect(reasons[0].textContent).toContain('Target instance not live');
    expect(reasons[0].textContent).toContain('Most urgent');
    expect(reasons[1].textContent).toContain('Pending transition');
    expect(reasons[1].textContent).not.toContain('Most urgent');

    jest.advanceTimersByTime(POLL_MS);
    fixture.detectChanges();
    expect(text('convergence-status')).toBe('Converged');
  });

  it('does not count unknown partitions as not converged', () => {
    get.mockReturnValue(
      of(
        fresh({
          status: 'UNKNOWN',
          primaryReason: 'WAGED_INTERNAL_FAILURE',
          partitionSummary: {
            total: 10,
            affected: 10,
            statusCounts: { UNKNOWN: 10 },
            reasonCounts: { WAGED_INTERNAL_FAILURE: 10 },
          },
        }),
      ),
    );
    const { fixture, text } = create();
    expect(text('convergence-headline')).toContain(
      'Status unknown for 10 of 10 partitions',
    );
    fixture.destroy();
  });

  it('explains a report that cannot be trusted', () => {
    get.mockReturnValue(
      of({
        clusterId: 'Demo',
        status: 'UNKNOWN',
        primaryReason: 'CONTROLLER_LEADER_MISSING',
        reportState: 'CONTROLLER_LEADER_MISSING',
      }),
    );
    const { fixture, text } = create();

    expect(text('convergence-headline')).toBe(
      'There is no controller leader, so no report can be trusted.',
    );
    expect(text('convergence-legend')).toBeUndefined();
    fixture.destroy();
  });

  it('says when the report is stale and when it was last confirmed', () => {
    get.mockReturnValue(
      of({
        clusterId: 'Demo',
        status: 'UNKNOWN',
        primaryReason: 'NONE',
        reportState: 'REPORT_STALE',
        evaluatedAt: Date.now() - 6 * 60_000,
      }),
    );
    const { fixture, text } = create();
    expect(text('convergence-headline')).toContain('stopped updating');
    expect(text('convergence-headline')).toContain('Last confirmed');
    expect(fixture.componentInstance.tone).toBe('warning');
    fixture.destroy();
  });

  it('says when the REST server has no convergence endpoint', () => {
    get.mockReturnValue(throwError('Not Found'));
    const { fixture, text } = create();

    expect(text('convergence-headline')).toBe(
      'This REST server does not publish convergence status yet.',
    );
    expect(text('convergence-status')).toBeUndefined();
    jest.advanceTimersByTime(POLL_MS * 2);
    expect(get).toHaveBeenCalledTimes(1);
    expect(fixture.componentInstance.live).toBe(false);
  });

  it('keeps polling after a failed read and recovers', () => {
    get
      .mockReturnValueOnce(throwError('Service Unavailable'))
      .mockReturnValue(of(fresh({})));
    const { fixture, text } = create();

    expect(text('convergence-headline')).toContain(
      'Could not read convergence status',
    );
    expect(fixture.componentInstance.live).toBe(true);
    jest.advanceTimersByTime(POLL_MS);
    fixture.detectChanges();
    expect(get).toHaveBeenCalledTimes(2);
    expect(text('convergence-status')).toBe('Converged');
  });

  it('shows what helix-rest could not find and stops reading', () => {
    get.mockReturnValue(
      throwError(
        new ConvergenceTargetMissing(
          'Resource db does not exist in cluster Demo',
        ),
      ),
    );
    const { fixture, text } = create('db');

    expect(text('convergence-headline')).toBe(
      'Resource db does not exist in cluster Demo',
    );
    expect(fixture.componentInstance.live).toBe(false);
  });

  it('recovers from a read that never answers', () => {
    get
      .mockReturnValueOnce(throwError(new Error('timeout')))
      .mockReturnValue(of(fresh({})));
    const { fixture, text } = create();

    expect(text('convergence-headline')).toContain('request timed out');
    expect(fixture.componentInstance.loading).toBe(false);
    jest.advanceTimersByTime(POLL_MS);
    fixture.detectChanges();
    expect(text('convergence-status')).toBe('Converged');
  });

  it('formats durations and reasons', () => {
    expect(duration(45000)).toBe('45s');
    expect(duration(10805000)).toBe('3h 0m');
    expect(duration(-5)).toBe('0s');
    expect(reasonLabel('WAGED_LAST_KNOWN_GOOD')).toBe('WAGED last known good');
    expect(reasonLabel('PENDING_TRANSITION')).toBe('Pending transition');
  });
});
