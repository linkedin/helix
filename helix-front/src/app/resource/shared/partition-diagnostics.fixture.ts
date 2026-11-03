import { Instance } from '../../instance/shared/instance.model';
import { record } from '../../shared/models/znrecord.fixture';
import { ZNRecord } from '../../shared/models/znrecord.model';
import { Resource } from './resource.model';

export const masterSlave = record('MasterSlave', {
  simpleFields: { INITIAL_STATE: 'OFFLINE' },
  listFields: {
    STATE_PRIORITY_LIST: ['MASTER', 'SLAVE', 'OFFLINE', 'DROPPED', 'ERROR'],
  },
  mapFields: {
    'MASTER.meta': { count: '1' },
    'SLAVE.meta': { count: 'R' },
  },
});

export const makeResource = (
  externalView: Record<string, Record<string, string>>,
  simpleFields: Record<string, string> = {},
  idealState: Partial<ZNRecord> = {},
) =>
  new Resource(
    'c',
    'db',
    {},
    {
      ...record('db', idealState),
      simpleFields: {
        REBALANCE_MODE: 'FULL_AUTO',
        STATE_MODEL_DEF_REF: 'MasterSlave',
        REPLICAS: '3',
        ...simpleFields,
      },
    },
    { mapFields: externalView },
  );

// a, b and c are live; d is not
export const instances = [
  ...['a', 'b', 'c'].map((name) => new Instance(name, 'c', true, true)),
  new Instance('d', 'c', false, false, undefined, undefined, 'EVACUATE'),
];
