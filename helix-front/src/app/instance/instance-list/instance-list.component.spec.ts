import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { beforeEach, describe, expect, it } from '@jest/globals';
import { of } from 'rxjs';

import { InstanceListComponent } from './instance-list.component';
import { InstanceService } from '../shared/instance.service';
import { HelperService } from '../../shared/helper.service';
import { Instance } from '../shared/instance.model';

describe('InstanceListComponent', () => {
  let component: InstanceListComponent;
  let fixture: ComponentFixture<InstanceListComponent>;

  const mockInstances = [
    new Instance('hostA_1', 'TestCluster', true, true, undefined, undefined, 'ENABLE'),
    new Instance('hostB_2', 'TestCluster', false, false, undefined, undefined, 'DISABLE'),
    new Instance('hostC_3', 'TestCluster', false, false, undefined, undefined, 'EVACUATE'),
    new Instance('hostD_4', 'TestCluster', false, false, undefined, undefined, 'SWAP_IN'),
    new Instance('hostE_5', 'TestCluster', false, false, undefined, undefined, 'UNKNOWN'),
  ];

  const mockInstanceService = {
    getAll: () => of(mockInstances),
  };

  const mockHelperService = {
    showError: () => {},
  };

  const mockRoute = {
    parent: {
      snapshot: {
        params: { name: 'TestCluster' },
      },
    },
  };

  const mockRouter = { navigate: () => {} };

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [InstanceListComponent],
      providers: [
        { provide: ActivatedRoute, useValue: mockRoute },
        { provide: Router, useValue: mockRouter },
        { provide: InstanceService, useValue: mockInstanceService },
        { provide: HelperService, useValue: mockHelperService },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InstanceListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should populate instances with correct operation states after init', () => {
    component.ngOnInit();

    expect(component.instances).toHaveLength(5);
    expect(component.instances[0].operationState).toBe('ENABLE');
    expect(component.instances[0].healthy).toBe(true);
    expect(component.instances[1].operationState).toBe('DISABLE');
    expect(component.instances[2].operationState).toBe('EVACUATE');
    expect(component.instances[3].operationState).toBe('SWAP_IN');
    expect(component.instances[4].operationState).toBe('UNKNOWN');
  });

  it('should set isLoading to false after data loads', () => {
    component.ngOnInit();
    expect(component.isLoading).toBe(false);
  });

  it('should set clusterName from route params', () => {
    component.ngOnInit();
    expect(component.clusterName).toBe('TestCluster');
  });
});
