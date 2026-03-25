import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { MatMenuModule } from '@angular/material/menu';
import { afterEach, beforeEach, describe, expect, it } from '@jest/globals';
import { of } from 'rxjs';

import { InstanceDetailComponent } from './instance-detail.component';
import { InstanceService } from '../shared/instance.service';
import { HelperService } from '../../shared/helper.service';
import { Instance } from '../shared/instance.model';

describe('InstanceDetailComponent', () => {
  let component: InstanceDetailComponent;
  let fixture: ComponentFixture<InstanceDetailComponent>;

  const mockRoute = {
    snapshot: {
      params: {
        cluster_name: 'TestCluster',
        instance_name: 'host1_1234',
      },
    },
  };

  const mockRouter = { navigate: () => {} };

  const mockHelperService = {
    showError: () => {},
    showSnackBar: () => {},
    showConfirmation: () => Promise.resolve(false),
  };

  function setupWithInstance(instance: Instance) {
    const mockInstanceService = {
      get: () => of(instance),
      can: () => of(true),
      enable: () => of(null),
      disable: () => of(null),
    };

    TestBed.configureTestingModule({
      imports: [MatMenuModule],
      declarations: [InstanceDetailComponent],
      providers: [
        { provide: ActivatedRoute, useValue: mockRoute },
        { provide: Router, useValue: mockRouter },
        { provide: HelperService, useValue: mockHelperService },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    });

    TestBed.overrideComponent(InstanceDetailComponent, {
      set: {
        providers: [
          { provide: InstanceService, useValue: mockInstanceService },
        ],
      },
    });

    TestBed.compileComponents();

    fixture = TestBed.createComponent(InstanceDetailComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
  }

  afterEach(() => {
    TestBed.resetTestingModule();
  });

  it('should create and load an ENABLE instance', () => {
    const instance = new Instance(
      'host1_1234', 'TestCluster', true, true,
      'sess-1', '1.0.0', 'ENABLE'
    );
    setupWithInstance(instance);

    expect(component).toBeTruthy();
    expect(component.instance.operationState).toBe('ENABLE');
    expect(component.instance.healthy).toBe(true);
    expect(component.instance.online).toBe(true);
  });

  it('should load a DISABLE instance correctly', () => {
    const instance = new Instance(
      'host1_1234', 'TestCluster', false, false,
      undefined, undefined, 'DISABLE'
    );
    setupWithInstance(instance);

    expect(component.instance.operationState).toBe('DISABLE');
    expect(component.instance.healthy).toBeFalsy();
  });

  it('should load an EVACUATE instance correctly', () => {
    const instance = new Instance(
      'host1_1234', 'TestCluster', false, false,
      undefined, undefined, 'EVACUATE'
    );
    setupWithInstance(instance);

    expect(component.instance.operationState).toBe('EVACUATE');
    expect(component.instance.healthy).toBeFalsy();
  });

  it('should load an UNKNOWN instance correctly', () => {
    const instance = new Instance(
      'host1_1234', 'TestCluster', false, false,
      undefined, undefined, 'UNKNOWN'
    );
    setupWithInstance(instance);

    expect(component.instance.operationState).toBe('UNKNOWN');
  });

  it('should load a SWAP_IN instance correctly', () => {
    const instance = new Instance(
      'host1_1234', 'TestCluster', false, false,
      undefined, undefined, 'SWAP_IN'
    );
    setupWithInstance(instance);

    expect(component.instance.operationState).toBe('SWAP_IN');
  });

  it('should set can to true when service.can() returns true', () => {
    const instance = new Instance(
      'host1_1234', 'TestCluster', true, true,
      undefined, undefined, 'ENABLE'
    );
    setupWithInstance(instance);

    expect(component.can).toBe(true);
  });

  it('should set isLoading to false after instance loads', () => {
    const instance = new Instance(
      'host1_1234', 'TestCluster', true, true,
      undefined, undefined, 'ENABLE'
    );
    setupWithInstance(instance);

    expect(component.isLoading).toBe(false);
  });
});
