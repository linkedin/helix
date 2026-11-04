import 'zone.js';
import 'zone.js/dist/zone-testing';

import { TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { of, Subject, throwError } from 'rxjs';

import { CapacityService } from '../../capacity/shared/capacity.service';
import { snapshotFixture } from '../../capacity/testing/snapshot.fixture';
import { ClusterDetailComponent } from '../../cluster/cluster-detail/cluster-detail.component';
import { Instance } from '../../instance/shared/instance.model';
import { InstanceService } from '../../instance/shared/instance.service';
import { ResourceService } from '../../resource/shared/resource.service';
import { ClusterOverviewComponent } from './cluster-overview.component';

describe('ClusterOverviewComponent', () => {
  const getSnapshot = jest.fn<any>();
  const getInstances = jest.fn<any>();
  const getResources = jest.fn<any>();
  const resource = (name: string, extra = {}) => ({
    name,
    rebalanceMode: 'FULL_AUTO',
    enabled: true,
    waged: true,
    hasExternalView: true,
    ...extra,
  });

  function create() {
    TestBed.configureTestingModule({
      declarations: [ClusterOverviewComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { snapshot: { params: { name: 'Demo' } } } },
        },
        { provide: CapacityService, useValue: { getSnapshot } },
        { provide: InstanceService, useValue: { getSummaries: getInstances } },
        { provide: ResourceService, useValue: { getSummaries: getResources } },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    });
    const fixture = TestBed.createComponent(ClusterOverviewComponent);
    fixture.detectChanges();
    return fixture;
  }

  const texts = (fixture, id: string): string[] =>
    Array.from(
      fixture.nativeElement.querySelectorAll(`[data-testid="${id}"]`),
      (e: HTMLElement) => e.textContent.replace(/\s+/g, ' ').trim(),
    );

  beforeEach(() => {
    getSnapshot.mockReset();
    getInstances.mockReset();
    getResources.mockReset();
    const healthy = snapshotFixture();
    getSnapshot.mockReturnValue(
      of({
        ...healthy,
        instances: [healthy.instances[0]],
        summary: {
          ...healthy.summary,
          instancesOverCapacity: 0,
          instancesWithUnknownCapacity: 0,
          totalErrorReplicas: 0,
        },
      }),
    );
    getInstances.mockReturnValue(of([new Instance('n1', 'Demo', true, true)]));
    getResources.mockReturnValue(of([resource('db')]));
  });

  it('is the first cluster tab', () => {
    const detail = new ClusterDetailComponent(
      null,
      null,
      null,
      null,
      null,
      null,
    );
    expect(detail.tabLinks[0]).toEqual({ label: 'Overview', link: 'overview' });
  });

  it('says healthy when Helix reports nothing to act on', () => {
    const fixture = create();

    expect(getSnapshot).toHaveBeenCalledWith('Demo');
    expect(texts(fixture, 'verdict')).toEqual([
      'Healthy: Helix reports nothing to act on.',
    ]);
    expect(fixture.componentInstance.tone).toBe('ok');
    expect(texts(fixture, 'issue')).toEqual([]);
    expect(texts(fixture, 'see-all')).toEqual([]);
  });

  it('shows the worst five issues, key numbers and read times', () => {
    getSnapshot.mockReturnValue(
      of({
        ...snapshotFixture(),
        maintenance: true,
        maintenanceReason: 'upgrade',
      }),
    );
    getInstances.mockReturnValue(
      of([
        new Instance('n4', 'Demo', true, false),
        new Instance('n5', 'Demo', false, true),
      ]),
    );
    getResources.mockReturnValue(
      of([
        resource('db'),
        resource('ra', { hasExternalView: false }),
        resource('rb', { hasExternalView: false, waged: false }),
        resource('rc', { enabled: false }),
      ]),
    );
    const fixture = create();
    const component = fixture.componentInstance;

    expect(texts(fixture, 'verdict')).toEqual([
      'Needs attention: 4 critical, 3 warnings.',
    ]);
    expect(component.tone).toBe('error');
    expect(
      Array.from(
        fixture.nativeElement.querySelectorAll('[data-testid="issue"]'),
        (tr: HTMLElement) =>
          Array.from(tr.querySelectorAll('.badge, .name'), (e) =>
            e.textContent.trim(),
          ).join(' | '),
      ),
    ).toEqual([
      'critical | n3 | 2 ERROR replicas',
      'critical | ra | No external view',
      'critical | rb | No external view',
      'critical | n4 | Instance not live',
      'warning | Demo | Cluster in maintenance mode',
    ]);
    expect(texts(fixture, 'see-all')).toEqual(['See all 9 in Diagnostics']);
    expect(texts(fixture, 'card')).toEqual([
      '1 / 2 instances live 1 with no live session.',
      '1 not enabled or under an operation 1 DISABLE',
      '4 resources 3 WAGED, 1 not WAGED. 1 disabled, 2 enabled with no external view.',
      '2 ERROR replicas On 1 instance.',
      '1 over WAGED capacity 1 with unknown capacity. Hottest: n3 at 120% of CU.',
      'ctl controller leader Holds leadership.',
      'maintenance cluster mode Reason: upgrade.',
    ]);
    expect(component.cards.map((c) => c.link)).toEqual([
      '../topology',
      '../instances',
      '../resources',
      '../diagnostics',
      '../capacity',
      '../controller',
      '../controller',
    ]);
    const [readTimes] = texts(fixture, 'read-times');
    expect(readTimes).toContain('sample taken at 2026-11-01 10:00:00 UTC');
    expect(readTimes).toContain('Instances and resources were read at');
    expect(component.listsReadAt).toBeGreaterThan(component.updated);
  });

  it('marks the picture incomplete and the cards unknown when a read fails', () => {
    getSnapshot.mockReturnValue(throwError('Service unavailable'));
    const fixture = create();
    const component = fixture.componentInstance;

    expect(texts(fixture, 'verdict')).toEqual([
      'Incomplete: could not read the capacity snapshot (Service unavailable), so this is not the full picture. Among what was read: nothing to act on.',
    ]);
    expect(component.tone).toBe('warning');
    expect(component.updated).toBeNull();
    expect(texts(fixture, 'card')).toEqual([
      '1 / 1 instances live All have a live session.',
      '0 not enabled or under an operation All ENABLE.',
      '1 resources 1 WAGED, 0 not WAGED. 0 disabled, 0 enabled with no external view.',
      'unknown ERROR replicas Could not read the capacity snapshot.',
      'unknown over WAGED capacity Could not read the capacity snapshot.',
      'unknown controller leader Could not read the capacity snapshot.',
      'unknown cluster mode Could not read the capacity snapshot.',
    ]);
    const [readTimes] = texts(fixture, 'read-times');
    expect(readTimes).not.toContain('sample taken at');
    expect(readTimes).toContain('Instances and resources were read at');
  });

  it('drops a superseded read on refresh and on destroy', () => {
    const slow = new Subject<any>();
    getSnapshot.mockReturnValueOnce(slow);
    const fixture = create();
    expect(slow.observers.length).toBe(1);

    fixture.componentInstance.load();
    expect(slow.observers.length).toBe(0);
    expect(fixture.componentInstance.isLoading).toBe(false);

    const pending = new Subject<any>();
    getSnapshot.mockReturnValueOnce(pending);
    fixture.componentInstance.load();
    fixture.destroy();
    expect(pending.observers.length).toBe(0);
  });
});
