import 'zone.js';
import 'zone.js/dist/zone-testing';

import { TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { of, throwError } from 'rxjs';

import { CapacityService } from '../../capacity/shared/capacity.service';
import { snapshotFixture } from '../../capacity/testing/snapshot.fixture';
import { Instance } from '../../instance/shared/instance.model';
import { InstanceService } from '../../instance/shared/instance.service';
import { ResourceService } from '../../resource/shared/resource.service';
import { ClusterDiagnosticsComponent } from './cluster-diagnostics.component';

describe('ClusterDiagnosticsComponent', () => {
  const getSnapshot = jest.fn<any>();
  const getInstances = jest.fn<any>();
  const getResources = jest.fn<any>();

  function create() {
    TestBed.configureTestingModule({
      declarations: [ClusterDiagnosticsComponent],
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
    const fixture = TestBed.createComponent(ClusterDiagnosticsComponent);
    fixture.detectChanges();
    return fixture;
  }

  const rows = (fixture): string[] =>
    Array.from(
      fixture.nativeElement.querySelectorAll('[data-testid="issue"]'),
    ).map((tr: HTMLElement) =>
      Array.from(tr.querySelectorAll('.badge, .name, .sub, a'))
        .map((e) => e.textContent.trim())
        .join(' | '),
    );

  beforeEach(() => {
    getSnapshot.mockReset();
    getInstances.mockReset();
    getResources.mockReset();
    getSnapshot.mockReturnValue(of(snapshotFixture()));
    getInstances.mockReturnValue(of([new Instance('n4', 'Demo', true, false)]));
    getResources.mockReturnValue(of([]));
  });

  it('loads the queue for the cluster and filters it', () => {
    const fixture = create();
    const component = fixture.componentInstance;

    expect(getSnapshot).toHaveBeenCalledWith('Demo');
    expect(getInstances).toHaveBeenCalledWith('Demo');
    expect(getResources).toHaveBeenCalledWith('Demo');
    expect(component.isLoading).toBe(false);
    expect(component.updated).toBe(Date.parse('2026-11-01T10:00:00Z'));
    expect(component.listsReadAt).toBeGreaterThan(component.updated);
    const text = fixture.nativeElement
      .querySelector('[data-testid="read-times"]')
      .textContent.replace(/\s+/g, ' ');
    expect(text).toContain('sample taken at 2026-11-01 10:00:00 UTC');
    expect(text).toContain('Instances and resources were read at');
    expect(rows(fixture)).toEqual([
      'critical | n3 | ERROR replicas | 2 ERROR replicas | In the published external view. | Errors',
      'critical | n4 | Instances | Instance not live | Enabled, but has no live ZooKeeper session. | Instance',
      'warning | n3 | Capacity | Over WAGED capacity | CU 1200 / 1000 (120%), DISK 2400 / 2000 (120%). Partition weights, not CPU or disk usage. | Capacity',
      'info | n4 | Capacity | Capacity unknown | Capacity is not fully configured for CU, DISK. | Capacity',
    ]);
    expect(component.link(component.issues[0])).toEqual([
      '../',
      'instances',
      'n3',
      'diagnostics',
    ]);
    expect(component.count('critical', null)).toBe(2);

    component.severity = 'critical';
    component.category = 'instance';
    fixture.detectChanges();
    expect(rows(fixture)).toEqual([expect.stringContaining('critical | n4')]);

    component.severity = 'info';
    fixture.detectChanges();
    expect(rows(fixture)).toEqual([]);
    expect(
      fixture.nativeElement.querySelector('[data-testid="filtered-empty"]'),
    ).not.toBeNull();
  });

  it('keeps the last queue when a refresh fails', () => {
    const fixture = create();
    const component = fixture.componentInstance;
    getSnapshot.mockReturnValue(throwError('Service unavailable'));

    component.load();
    fixture.detectChanges();

    expect(getSnapshot).toHaveBeenCalledTimes(2);
    expect(component.error).toBe('Service unavailable');
    expect(component.isLoading).toBe(false);
    expect(rows(fixture).length).toBe(4);
  });

  it('shows no table for a cluster with nothing to act on', () => {
    getSnapshot.mockReturnValue(
      of({ ...snapshotFixture(), instances: [snapshotFixture().instances[0]] }),
    );
    getInstances.mockReturnValue(of([]));
    const fixture = create();

    expect(fixture.componentInstance.issues).toEqual([]);
    expect(fixture.nativeElement.querySelector('table')).toBeNull();
  });
});
