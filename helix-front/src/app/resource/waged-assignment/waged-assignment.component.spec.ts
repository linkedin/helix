import { describe, expect, it } from '@jest/globals';
import { of } from 'rxjs';

import { WagedAssignmentComponent } from './waged-assignment.component';

describe('WagedAssignmentComponent', () => {
  function create(config: any) {
    const service: any = {
      getWagedAssignment: () =>
        of({
          assignment: { db: { p_0: { a: 'MASTER', b: 'SLAVE' } } },
          metadata: { version: '7', writeTimeMs: 1 },
        }),
      get: () =>
        of({
          idealState: { mapFields: { p_0: { a: 'SLAVE', b: 'MASTER' } } },
          externalView: { mapFields: { p_0: { a: 'MASTER', b: 'SLAVE' } } },
        }),
      getClusterConfig: () => of(config),
    };
    const route: any = {
      snapshot: { data: { kind: 'bestPossible' } },
      parent: {
        snapshot: { params: { cluster_name: 'c', resource_name: 'db' } },
      },
    };
    const c = new WagedAssignmentComponent(route, service);
    c.ngOnInit();
    return c;
  }

  it('shows the saved placement alone until asked to compare', () => {
    const c = create({ simpleFields: {} });
    expect(c.compareWith).toBe('none');
    expect(c.rows[0].other).toEqual([]);
    expect(c.differing).toBe(0);

    c.compare('idealState');
    expect([c.rows[0].difference, c.differing]).toEqual(['2 states differ', 1]);
    c.compare('externalView');
    expect(c.differing).toBe(0);
  });

  it('reads which assignment the ideal state holds', () => {
    expect(create({ simpleFields: {} }).persist).toEqual({
      bestPossible: false,
      intermediate: false,
    });
    expect(
      create({
        simpleFields: {
          PERSIST_BEST_POSSIBLE_ASSIGNMENT: 'true',
          PERSIST_INTERMEDIATE_ASSIGNMENT: 'TRUE',
        },
      }).persist,
    ).toEqual({ bestPossible: true, intermediate: true });
    expect(create(null).persist).toBeNull();
  });
});
