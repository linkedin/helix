import 'zone.js';
import 'zone.js/dist/zone-testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { beforeEach, describe, expect, it } from '@jest/globals';
import { convertToParamMap } from '@angular/router';
import { of, Subject, throwError } from 'rxjs';

import { TestingModule } from '../../../testing/testing.module';
import { PartitionListComponent } from './partition-list.component';
import { ResourceService } from '../shared/resource.service';
import { InstanceService } from '../../instance/shared/instance.service';
import { Instance } from '../../instance/shared/instance.model';
import {
  instances,
  makeResource,
  masterSlave,
} from '../shared/partition-diagnostics.fixture';

describe('PartitionListComponent', () => {
  let component: PartitionListComponent;
  let fixture: ComponentFixture<PartitionListComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      providers: [ResourceService, InstanceService],
      declarations: [PartitionListComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PartitionListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});

describe('PartitionListComponent behavior', () => {
  const resource = makeResource({
    p_1: { a: 'MASTER', b: 'SLAVE', c: 'SLAVE' },
    p_2: { a: 'SLAVE', b: 'SLAVE', c: 'SLAVE' },
    p_3: { a: 'MASTER', b: 'SLAVE', c: 'ERROR' },
  });

  // each load() takes the next instance summaries read
  function build(...summaries: any[]) {
    const route: any = {
      parent: {
        paramMap: of(
          convertToParamMap({ cluster_name: 'c', resource_name: 'db' }),
        ),
      },
    };
    const service: any = {
      get: () => of(resource),
      getStateModel: () => of(masterSlave),
    };
    const nodes: any = { getSummaries: () => summaries.shift() };
    const component = new PartitionListComponent(route, service, nodes);
    component.ngOnInit();
    return component;
  }

  const statuses = (component: PartitionListComponent) =>
    component.rows.map((row) => `${row.name}:${row.status}`);

  it('counts every filter against all partitions and filters the rows', () => {
    const component = build(of(instances));
    expect(component.counts).toEqual({
      all: 3,
      attention: 2,
      unserved: 0,
      'missing-top': 1,
      'below-target': 1,
      errors: 1,
      assignment: null,
    });
    component.setFilter('errors');
    expect(statuses(component)).toEqual(['p_3:ERROR']);
    component.setFilter('all', 'P_1');
    expect(statuses(component)).toEqual(['p_1:HEALTHY']);
  });

  it('shows partitions first and fills liveness when it arrives', () => {
    const first = new Subject<Instance[]>();
    const second = new Subject<Instance[]>();
    const component = build(first, second);
    expect(component.instancesPending).toBe(true);
    expect(statuses(component)).toEqual([
      'p_3:ERROR',
      'p_2:DEGRADED',
      'p_1:UNKNOWN',
    ]);

    component.load();
    first.next(instances);
    expect(component.instancesPending).toBe(true);

    second.next(instances);
    expect(component.instancesPending).toBe(false);
    expect(statuses(component)).toContain('p_1:HEALTHY');
  });

  it('treats liveness as unknown when instance status cannot be read', () => {
    const component = build(throwError('no response'));
    expect(component.instancesPending).toBe(false);
    expect(component.warnings[0]).toContain('could not be read (no response)');
    expect(component.counts.unserved).toBe(0);
    expect(component.rows.find((row) => row.name === 'p_1').explanation).toBe(
      'Cannot judge liveness, instance status is unknown',
    );
  });
});
