import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { Observable, Subject, of, throwError } from 'rxjs';

import { ResourceListComponent } from './resource-list.component';
import { ResourceService } from '../shared/resource.service';
import { ResourceSummary } from '../shared/resource.model';
import { WorkflowService } from '../../workflow/shared/workflow.service';
import { HelperService } from '../../shared/helper.service';

function summary(name: string, extra: Partial<ResourceSummary> = {}) {
  return <ResourceSummary>{
    cluster: 'Demo',
    name,
    rebalanceMode: 'FULL_AUTO',
    stateModelDef: 'OnlineOffline',
    numPartitions: 4,
    replicas: '2',
    minActiveReplicas: 1,
    instanceGroupTag: null,
    enabled: true,
    waged: false,
    hasExternalView: true,
    alive: true,
    ...extra,
  };
}

describe('ResourceListComponent summaries', () => {
  let component: ResourceListComponent;
  let fixture: ComponentFixture<ResourceListComponent>;
  const showError = jest.fn();
  const getSummaries = jest.fn();

  function create(params: Observable<any> = of({ name: 'Demo' })) {
    TestBed.configureTestingModule({
      declarations: [ResourceListComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: { data: {} },
            parent: { snapshot: { params: {} }, params },
          },
        },
        { provide: Router, useValue: { navigate: () => {} } },
        { provide: ResourceService, useValue: { getSummaries } },
        { provide: HelperService, useValue: { showError } },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).overrideComponent(ResourceListComponent, {
      set: {
        providers: [
          { provide: WorkflowService, useValue: { getAll: () => of(['wf1']) } },
        ],
      },
    });
    fixture = TestBed.createComponent(ResourceListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }

  beforeEach(() => {
    showError.mockReset();
    getSummaries.mockReset();
  });

  it('drops workflow and job resources and counts each filter', () => {
    getSummaries.mockReturnValue(
      of([
        summary('Orders', { waged: true }),
        summary('Index'),
        summary('Stuck', { hasExternalView: false, alive: false }),
        summary('Old', { enabled: false }),
        summary('wf1'),
        summary('wf1_job1'),
      ]),
    );
    create();

    expect(component.allResources.map((r) => r.name)).toEqual([
      'Orders',
      'Index',
      'Stuck',
      'Old',
    ]);
    expect(component.count('all')).toBe(4);
    expect(component.count('waged')).toBe(1);
    expect(component.count('classic')).toBe(3);
    expect(component.count('disabled')).toBe(1);
    expect(component.count('no-view')).toBe(1);
    expect(component.updated).not.toBeNull();
  });

  it('filters the table rows', () => {
    getSummaries.mockReturnValue(
      of([summary('Orders', { waged: true }), summary('Index')]),
    );
    create();

    component.setFilter('waged');
    expect(component.resources.map((r) => r.name)).toEqual(['Orders']);
    component.setFilter('all');
    expect(component.resources.length).toBe(2);
  });

  it('shows the error and stops loading when the read fails', () => {
    getSummaries.mockReturnValue(throwError('boom'));
    create();

    expect(component.error).toBe('boom');
    expect(component.isLoading).toBe(false);
    expect(showError).toHaveBeenCalledWith('boom');
  });

  it('ignores a late response for the cluster that was left', () => {
    const reads: Record<string, Subject<ResourceSummary[]>> = {
      A: new Subject(),
      B: new Subject(),
    };
    getSummaries.mockImplementation((cluster: string) => reads[cluster]);
    const params = new Subject<any>();
    create(params);

    params.next({ name: 'A' });
    params.next({ name: 'B' });
    reads.B.next([summary('b-res')]);
    reads.A.next([summary('a-res')]);

    expect(component.allResources.map((r) => r.name)).toEqual(['b-res']);
    expect(component.isLoading).toBe(false);
    expect(reads.A.observers.length).toBe(0);
  });
});
