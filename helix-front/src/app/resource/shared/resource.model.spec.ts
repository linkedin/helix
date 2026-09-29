import { describe, expect, it } from '@jest/globals';

import { Resource } from './resource.model';

describe('Resource rebalance mode', () => {
  it.each([undefined, 'AUTO', 'AUTO_REBALANCE', 'CUSTOMIZED'])(
    'uses modern metadata without exposing legacy mode %s',
    (legacyMode) => {
      const simpleFields: Record<string, string> = {
        REBALANCE_MODE: 'FULL_AUTO',
        STATE_MODEL_DEF_REF: 'OnlineOffline',
        NUM_PARTITIONS: '1',
        REPLICAS: '1',
      };
      if (legacyMode !== undefined) {
        simpleFields.IDEAL_STATE_MODE = legacyMode;
      }
      const idealState = {
        simpleFields,
        listFields: { resource_0: ['node'] },
        mapFields: { resource_0: { node: 'ONLINE' } },
      };
      const externalView = {
        mapFields: { resource_0: { node: 'ONLINE' } },
      };

      const resource = new Resource(
        'cluster',
        'resource',
        {},
        idealState,
        externalView,
      );

      expect(resource.rebalanceMode).toBe('FULL_AUTO');
      expect(resource).not.toHaveProperty('idealStateMode');
      expect(resource.partitions[0].replicas).toEqual([
        { instanceName: 'node', externalView: 'ONLINE', idealState: 'ONLINE' },
      ]);
      expect(resource.idealState).toBe(idealState);
      expect(simpleFields.IDEAL_STATE_MODE).toBe(legacyMode);
    },
  );
});
