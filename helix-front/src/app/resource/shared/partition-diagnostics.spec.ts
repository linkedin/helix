import { describe, expect, it } from '@jest/globals';

import { record } from '../../shared/models/znrecord.fixture';
import { diagnosePartitions } from './partition-diagnostics';
import {
  instances,
  makeResource,
  masterSlave,
} from './partition-diagnostics.fixture';

const diagnose = (
  ev: Record<string, Record<string, string>>,
  nodes = instances,
) => diagnosePartitions(makeResource(ev), masterSlave, nodes);

describe('diagnosePartitions', () => {
  // d is not live, so its SLAVE does not serve
  it.each`
    states                                     | status        | explanation                  | issues
    ${{ c: 'SLAVE', a: 'MASTER', b: 'SLAVE' }} | ${'HEALTHY'}  | ${'3 of 3 replicas serving'} | ${[]}
    ${{ a: 'SLAVE', b: 'SLAVE', c: 'SLAVE' }}  | ${'DEGRADED'} | ${'MASTER is missing'}       | ${['missing-top']}
    ${{ a: 'MASTER', b: 'SLAVE', d: 'SLAVE' }} | ${'DEGRADED'} | ${'2 of 3 replicas serving'} | ${['below-target']}
    ${{ a: 'MASTER', b: 'SLAVE', c: 'ERROR' }} | ${'ERROR'}    | ${'1 replica in ERROR'}      | ${['errors', 'below-target']}
  `(
    'judges $states as $status',
    ({ states, status, explanation, issues }: any) => {
      const [row] = diagnose({ p: states });
      expect([row.status, row.explanation, row.issues]).toEqual([
        status,
        explanation,
        issues,
      ]);
    },
  );

  it('orders replicas by state priority and tags the non-live instance', () => {
    const [row] = diagnose({ p: { d: 'SLAVE', b: 'SLAVE', a: 'MASTER' } });
    expect(row.replicas.map((r) => r.instanceName)).toEqual(['a', 'b', 'd']);
    expect(row.replicas[2]).toMatchObject({
      live: false,
      operation: 'EVACUATE',
      serving: false,
    });
  });

  it('lists IdealState partitions without an ExternalView entry as unserved', () => {
    const rows = diagnosePartitions(
      makeResource(
        { p_1: { a: 'MASTER', b: 'SLAVE', c: 'SLAVE' } },
        {},
        { listFields: { p_1: [], p_2: [] } },
      ),
      masterSlave,
      instances,
    );
    expect(rows.map((row) => [row.name, row.status, row.explanation])).toEqual([
      ['p_2', 'UNSERVED', 'No live replica is in a serving state'],
      ['p_1', 'HEALTHY', '3 of 3 replicas serving'],
    ]);
  });

  it.each`
    mode            | differs
    ${'CUSTOMIZED'} | ${true}
    ${'SEMI_AUTO'}  | ${null}
    ${'FULL_AUTO'}  | ${null}
  `(
    'judges the assignment only where the IdealState map is the target ($mode)',
    ({ mode, differs }: any) => {
      const [row] = diagnosePartitions(
        makeResource(
          { p: { a: 'MASTER', b: 'SLAVE', c: 'SLAVE' } },
          { REBALANCE_MODE: mode },
          { mapFields: { p: { a: 'SLAVE', b: 'MASTER', c: 'SLAVE' } } },
        ),
        masterSlave,
        instances,
      );
      expect(row.assignmentDiffers).toBe(differs);
      expect(row.issues.includes('assignment')).toBe(!!differs);
    },
  );

  const task = record('Task', {
    simpleFields: { INITIAL_STATE: 'INIT' },
    listFields: { STATE_PRIORITY_LIST: ['INIT', 'RUNNING', 'COMPLETED'] },
  });
  it.each`
    stateModel       | model
    ${'Task'}        | ${task}
    ${'MasterSlave'} | ${null}
  `(
    'cannot judge serving for state model $stateModel',
    ({ stateModel, model }: any) => {
      const [row] = diagnosePartitions(
        makeResource(
          { p: { a: 'RUNNING' } },
          { STATE_MODEL_DEF_REF: stateModel },
        ),
        model,
        instances,
      );
      expect([row.status, row.issues, row.replicas[0].serving]).toEqual([
        'UNKNOWN',
        [],
        null,
      ]);
    },
  );

  it('never reports a gap from unknown liveness', () => {
    const [row] = diagnose(
      { p: { a: 'MASTER', b: 'SLAVE', c: 'SLAVE' } },
      null,
    );
    expect([row.status, row.explanation, row.issues]).toEqual([
      'UNKNOWN',
      'Cannot judge liveness, instance status is unknown',
      [],
    ]);
  });

  it('sorts the worst first, then by name in natural order', () => {
    const healthy = { a: 'MASTER', b: 'SLAVE', c: 'SLAVE' };
    const rows = diagnose({
      p10: healthy,
      p2: healthy,
      p1: { a: 'SLAVE', b: 'SLAVE', c: 'SLAVE' },
      p3: { a: 'OFFLINE' },
    });
    expect(rows.map((row) => row.name)).toEqual(['p3', 'p1', 'p2', 'p10']);
  });
});
