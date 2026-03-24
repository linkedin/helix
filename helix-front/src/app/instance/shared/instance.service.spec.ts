import { TestBed } from '@angular/core/testing';
import {
  HttpClientTestingModule,
  HttpTestingController,
} from '@angular/common/http/testing';
import { Router } from '@angular/router';
import { beforeEach, afterEach, describe, expect, it } from '@jest/globals';

import { InstanceService } from './instance.service';
import { Instance } from './instance.model';

describe('InstanceService', () => {
  let service: InstanceService;
  let httpMock: HttpTestingController;

  const mockRouter = { url: '/testHelix/clusters/TestCluster/instances' };

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        InstanceService,
        { provide: Router, useValue: mockRouter },
      ],
    });
    service = TestBed.inject(InstanceService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('getAll', () => {
    it('should map an enabled + online instance to ENABLE state', (done) => {
      service.getAll('TestCluster').subscribe((instances: Instance[]) => {
        expect(instances).toHaveLength(1);
        expect(instances[0].operationState).toBe('ENABLE');
        expect(instances[0].online).toBe(true);
        expect(instances[0].healthy).toBe(true);
        expect(instances[0].enabled).toBe(true);
        done();
      });

      const req = httpMock.expectOne(
        '/api/helix/testHelix/clusters/TestCluster/instances'
      );
      req.flush({
        instances: ['host1_1234'],
        online: ['host1_1234'],
        enabled: ['host1_1234'],
        disabled: [],
        evacuated: [],
        swap_in: [],
        unknown: [],
      });
    });

    it('should map a disabled instance to DISABLE state', (done) => {
      service.getAll('TestCluster').subscribe((instances: Instance[]) => {
        expect(instances).toHaveLength(1);
        expect(instances[0].operationState).toBe('DISABLE');
        expect(instances[0].enabled).toBe(false);
        expect(instances[0].healthy).toBeFalsy();
        done();
      });

      const req = httpMock.expectOne(
        '/api/helix/testHelix/clusters/TestCluster/instances'
      );
      req.flush({
        instances: ['host1_1234'],
        online: [],
        enabled: [],
        disabled: ['host1_1234'],
        evacuated: [],
        swap_in: [],
        unknown: [],
      });
    });

    it('should map an evacuated instance to EVACUATE state', (done) => {
      service.getAll('TestCluster').subscribe((instances: Instance[]) => {
        expect(instances).toHaveLength(1);
        expect(instances[0].operationState).toBe('EVACUATE');
        expect(instances[0].enabled).toBe(false);
        done();
      });

      const req = httpMock.expectOne(
        '/api/helix/testHelix/clusters/TestCluster/instances'
      );
      req.flush({
        instances: ['host1_1234'],
        online: [],
        enabled: [],
        disabled: [],
        evacuated: ['host1_1234'],
        swap_in: [],
        unknown: [],
      });
    });

    it('should map a swap_in instance to SWAP_IN state', (done) => {
      service.getAll('TestCluster').subscribe((instances: Instance[]) => {
        expect(instances).toHaveLength(1);
        expect(instances[0].operationState).toBe('SWAP_IN');
        done();
      });

      const req = httpMock.expectOne(
        '/api/helix/testHelix/clusters/TestCluster/instances'
      );
      req.flush({
        instances: ['host1_1234'],
        online: [],
        enabled: [],
        disabled: [],
        evacuated: [],
        swap_in: ['host1_1234'],
        unknown: [],
      });
    });

    it('should map an unknown instance to UNKNOWN state', (done) => {
      service.getAll('TestCluster').subscribe((instances: Instance[]) => {
        expect(instances).toHaveLength(1);
        expect(instances[0].operationState).toBe('UNKNOWN');
        done();
      });

      const req = httpMock.expectOne(
        '/api/helix/testHelix/clusters/TestCluster/instances'
      );
      req.flush({
        instances: ['host1_1234'],
        online: [],
        enabled: [],
        disabled: [],
        evacuated: [],
        swap_in: [],
        unknown: ['host1_1234'],
      });
    });

    it('should correctly assign mixed states across multiple instances', (done) => {
      service.getAll('TestCluster').subscribe((instances: Instance[]) => {
        expect(instances).toHaveLength(4);
        const byName = new Map(instances.map((i) => [i.name, i]));

        expect(byName.get('hostA_1')!.operationState).toBe('ENABLE');
        expect(byName.get('hostA_1')!.online).toBe(true);

        expect(byName.get('hostB_2')!.operationState).toBe('DISABLE');
        expect(byName.get('hostB_2')!.online).toBe(false);

        expect(byName.get('hostC_3')!.operationState).toBe('EVACUATE');

        expect(byName.get('hostD_4')!.operationState).toBe('UNKNOWN');
        done();
      });

      const req = httpMock.expectOne(
        '/api/helix/testHelix/clusters/TestCluster/instances'
      );
      req.flush({
        instances: ['hostA_1', 'hostB_2', 'hostC_3', 'hostD_4'],
        online: ['hostA_1'],
        enabled: ['hostA_1'],
        disabled: ['hostB_2'],
        evacuated: ['hostC_3'],
        swap_in: [],
        unknown: ['hostD_4'],
      });
    });

    it('should sort instances by name', (done) => {
      service.getAll('TestCluster').subscribe((instances: Instance[]) => {
        expect(instances.map((i) => i.name)).toEqual([
          'alpha_1',
          'beta_2',
          'gamma_3',
        ]);
        done();
      });

      const req = httpMock.expectOne(
        '/api/helix/testHelix/clusters/TestCluster/instances'
      );
      req.flush({
        instances: ['gamma_3', 'alpha_1', 'beta_2'],
        online: ['alpha_1', 'beta_2', 'gamma_3'],
        enabled: ['alpha_1', 'beta_2', 'gamma_3'],
        disabled: [],
        evacuated: [],
        swap_in: [],
        unknown: [],
      });
    });
  });

  describe('get', () => {
    const apiUrl =
      '/api/helix/testHelix/clusters/TestCluster/instances/host1_1234';

    it('should read INSTANCE_OPERATION_STATE from simpleFields for a live instance', (done) => {
      service
        .get('TestCluster', 'host1_1234')
        .subscribe((instance: Instance) => {
          expect(instance.operationState).toBe('ENABLE');
          expect(instance.online).toBe(true);
          expect(instance.healthy).toBe(true);
          expect(instance.sessionId).toBe('sess-123');
          expect(instance.helixVersion).toBe('1.4.0');
          done();
        });

      httpMock.expectOne(apiUrl).flush({
        id: 'host1_1234',
        liveInstance: {
          simpleFields: {
            LIVE_INSTANCE: 'true',
            SESSION_ID: 'sess-123',
            HELIX_VERSION: '1.4.0',
          },
        },
        config: {
          simpleFields: {
            HELIX_ENABLED: 'true',
            INSTANCE_OPERATION_STATE: 'ENABLE',
          },
        },
      });
    });

    it('should read EVACUATE state from simpleFields for an offline instance', (done) => {
      service
        .get('TestCluster', 'host1_1234')
        .subscribe((instance: Instance) => {
          expect(instance.operationState).toBe('EVACUATE');
          expect(instance.online).toBe(false);
          expect(instance.healthy).toBeFalsy();
          done();
        });

      httpMock.expectOne(apiUrl).flush({
        id: 'host1_1234',
        liveInstance: null,
        config: {
          simpleFields: {
            HELIX_ENABLED: 'true',
            INSTANCE_OPERATION_STATE: 'EVACUATE',
          },
        },
      });
    });

    it('should read UNKNOWN state from simpleFields', (done) => {
      service
        .get('TestCluster', 'host1_1234')
        .subscribe((instance: Instance) => {
          expect(instance.operationState).toBe('UNKNOWN');
          done();
        });

      httpMock.expectOne(apiUrl).flush({
        id: 'host1_1234',
        liveInstance: null,
        config: {
          simpleFields: {
            INSTANCE_OPERATION_STATE: 'UNKNOWN',
          },
        },
      });
    });

    it('should read SWAP_IN state from simpleFields', (done) => {
      service
        .get('TestCluster', 'host1_1234')
        .subscribe((instance: Instance) => {
          expect(instance.operationState).toBe('SWAP_IN');
          done();
        });

      httpMock.expectOne(apiUrl).flush({
        id: 'host1_1234',
        liveInstance: null,
        config: {
          simpleFields: {
            INSTANCE_OPERATION_STATE: 'SWAP_IN',
          },
        },
      });
    });

    it('should default to ENABLE when INSTANCE_OPERATION_STATE is missing and HELIX_ENABLED is true', (done) => {
      service
        .get('TestCluster', 'host1_1234')
        .subscribe((instance: Instance) => {
          expect(instance.operationState).toBe('ENABLE');
          done();
        });

      httpMock.expectOne(apiUrl).flush({
        id: 'host1_1234',
        liveInstance: null,
        config: {
          simpleFields: {
            HELIX_ENABLED: 'true',
          },
        },
      });
    });

    it('should default to DISABLE when INSTANCE_OPERATION_STATE is missing and HELIX_ENABLED is false', (done) => {
      service
        .get('TestCluster', 'host1_1234')
        .subscribe((instance: Instance) => {
          expect(instance.operationState).toBe('DISABLE');
          done();
        });

      httpMock.expectOne(apiUrl).flush({
        id: 'host1_1234',
        liveInstance: null,
        config: {
          simpleFields: {
            HELIX_ENABLED: 'false',
          },
        },
      });
    });

    it('should ignore invalid INSTANCE_OPERATION_STATE values and fallback', (done) => {
      service
        .get('TestCluster', 'host1_1234')
        .subscribe((instance: Instance) => {
          expect(instance.operationState).toBe('ENABLE');
          done();
        });

      httpMock.expectOne(apiUrl).flush({
        id: 'host1_1234',
        liveInstance: null,
        config: {
          simpleFields: {
            HELIX_ENABLED: 'true',
            INSTANCE_OPERATION_STATE: 'BOGUS_STATE',
          },
        },
      });
    });
  });
});
