import { describe, expect, it } from '@jest/globals';
import { Instance, InstanceOperationState } from './instance.model';

describe('Instance', () => {
  describe('constructor defaults', () => {
    it('should default operationState to ENABLE when enabled is true', () => {
      const inst = new Instance('host_1234', 'cluster1', true, true);
      expect(inst.operationState).toBe('ENABLE');
    });

    it('should default operationState to DISABLE when enabled is false', () => {
      const inst = new Instance('host_1234', 'cluster1', false, false);
      expect(inst.operationState).toBe('DISABLE');
    });
  });

  describe('explicit operationState', () => {
    const states: InstanceOperationState[] = [
      'ENABLE',
      'DISABLE',
      'EVACUATE',
      'SWAP_IN',
      'UNKNOWN',
    ];

    states.forEach((state) => {
      it(`should use explicit operationState ${state}`, () => {
        const inst = new Instance(
          'host_1234',
          'cluster1',
          false,
          false,
          undefined,
          undefined,
          state
        );
        expect(inst.operationState).toBe(state);
      });
    });
  });

  describe('healthy getter', () => {
    it('should return true when online and operationState is ENABLE', () => {
      const inst = new Instance(
        'host_1234',
        'cluster1',
        true,
        true,
        undefined,
        undefined,
        'ENABLE'
      );
      expect(inst.healthy).toBe(true);
    });

    it('should return false when online but operationState is EVACUATE', () => {
      const inst = new Instance(
        'host_1234',
        'cluster1',
        false,
        true,
        undefined,
        undefined,
        'EVACUATE'
      );
      expect(inst.healthy).toBeFalsy();
    });

    it('should return false when online but operationState is DISABLE', () => {
      const inst = new Instance(
        'host_1234',
        'cluster1',
        false,
        true,
        undefined,
        undefined,
        'DISABLE'
      );
      expect(inst.healthy).toBeFalsy();
    });

    it('should return false when online but operationState is UNKNOWN', () => {
      const inst = new Instance(
        'host_1234',
        'cluster1',
        false,
        true,
        undefined,
        undefined,
        'UNKNOWN'
      );
      expect(inst.healthy).toBeFalsy();
    });

    it('should return false when offline regardless of operationState', () => {
      const inst = new Instance(
        'host_1234',
        'cluster1',
        true,
        false,
        undefined,
        undefined,
        'ENABLE'
      );
      expect(inst.healthy).toBeFalsy();
    });
  });

  describe('online getter', () => {
    it('should return true when liveInstance is truthy', () => {
      const inst = new Instance('host_1234', 'cluster1', true, true);
      expect(inst.online).toBe(true);
    });

    it('should return true when liveInstance is a truthy string', () => {
      const inst = new Instance('host_1234', 'cluster1', true, 'LIVE');
      expect(inst.online).toBe(true);
    });

    it('should return false when liveInstance is null', () => {
      const inst = new Instance('host_1234', 'cluster1', true, null);
      expect(inst.online).toBe(false);
    });

    it('should return false when liveInstance is false', () => {
      const inst = new Instance('host_1234', 'cluster1', true, false);
      expect(inst.online).toBe(false);
    });
  });

  describe('basic properties', () => {
    it('should store name, clusterName, sessionId, helixVersion', () => {
      const inst = new Instance(
        'host_1234',
        'testCluster',
        true,
        true,
        'session-abc',
        '1.2.3',
        'ENABLE'
      );
      expect(inst.name).toBe('host_1234');
      expect(inst.clusterName).toBe('testCluster');
      expect(inst.sessionId).toBe('session-abc');
      expect(inst.helixVersion).toBe('1.2.3');
    });
  });
});
