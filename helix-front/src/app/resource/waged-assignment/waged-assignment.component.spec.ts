import { describe, expect, it } from '@jest/globals';
import { of, throwError } from 'rxjs';

import { WagedAssignmentComponent } from './waged-assignment.component';

describe('WagedAssignmentComponent', () => {
  function create(config: any, overrides: any = {}) {
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
      ...overrides,
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

  const persisted = (config: any) => {
    const c = create(config);
    c.compare('idealState');
    return c.persist;
  };

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

  it('reads the resource and config only when a comparison is picked', () => {
    let reads = 0;
    const counted = () => {
      reads++;
      return of({ externalView: { mapFields: {} } });
    };
    const c = create({ simpleFields: {} }, { get: counted });
    expect([reads, c.persist, c.rows.length]).toEqual([0, null, 1]);
    c.compare('externalView');
    c.compare('idealState');
    expect(reads).toBe(1);
  });

  it('keeps the default view when only the resource read fails', () => {
    const c = create(
      { simpleFields: {} },
      { get: () => throwError('Not Found') },
    );
    expect([c.notSaved, c.error, c.rows.length]).toEqual([false, '', 1]);
    c.compare('externalView');
    expect(c.notSaved).toBe(false);
    expect(c.compared).toBe(false);
    expect(c.compareError).toContain('Not Found');
    expect(c.rows[0].difference).toBeNull();
  });

  it('says not saved only when the assignment read is a 404', () => {
    const c = create(null, {
      getWagedAssignment: () => throwError('Not Found'),
    });
    expect([c.notSaved, c.error]).toEqual([true, '']);
  });

  it('reads which assignment the ideal state holds', () => {
    expect(persisted({ simpleFields: {} })).toEqual({
      bestPossible: false,
      intermediate: false,
    });
    expect(
      persisted({
        simpleFields: {
          PERSIST_BEST_POSSIBLE_ASSIGNMENT: 'true',
          PERSIST_INTERMEDIATE_ASSIGNMENT: 'TRUE',
        },
      }),
    ).toEqual({ bestPossible: true, intermediate: true });
    expect(persisted(null)).toBeNull();
  });
});
