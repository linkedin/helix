/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { describe, expect, it, jest } from '@jest/globals';
import { defer, of, throwError } from 'rxjs';

import { InstanceListComponent } from './instance-list.component';
import { Instance, InstanceOperationState } from '../shared/instance.model';

const operations: InstanceOperationState[] = [
  'ENABLE',
  'DISABLE',
  'EVACUATE',
  'SWAP_IN',
  'UNKNOWN',
];

function allOperations(): Instance[] {
  return operations.flatMap((operation) =>
    [true, false].map(
      (live) =>
        new Instance(
          `${operation}-${live ? 'live' : 'offline'}`,
          'Demo',
          operation === 'ENABLE',
          live,
          's',
          'v',
          operation,
        ),
    ),
  );
}

function sample(): Instance[] {
  return [
    new Instance('live-a', 'Demo', true, true, 's', 'v', 'ENABLE'),
    new Instance('offline-b', 'Demo', true, false, 's', 'v', 'ENABLE'),
    new Instance('disabled-c', 'Demo', false, true, 's', 'v', 'DISABLE'),
  ];
}

function build(data: any) {
  const route: any = { parent: { snapshot: { params: { name: 'Demo' } } } };
  const router: any = { navigate: jest.fn() };
  const service: any = { getAll: jest.fn(() => data) };
  const helper: any = { showError: jest.fn() };
  const component = new InstanceListComponent(route, router, service, helper);
  component.ngOnInit();
  return component;
}

describe('InstanceListComponent live vs operation filtering', () => {
  it('counts each filter against the whole set', () => {
    const component = build(of(sample()));
    expect(component.count('all')).toBe(3);
    expect(component.count('membership')).toBe(2);
    expect(component.count('offline')).toBe(1);
    expect(component.count('disabled')).toBe(1);
  });

  it('provides a filter for every operation and counts the whole set', () => {
    const component = build(of(allOperations()));
    expect(
      component.operationFilters.map((operation) => operation.state),
    ).toEqual(operations);
    expect(component.count('all')).toBe(10);
    expect(component.count('membership')).toBe(9);
    expect(component.count('offline')).toBe(5);

    for (const operation of component.operationFilters) {
      expect(component.count(operation.filter)).toBe(2);
    }
  });

  it('filters each operation independently of liveness', () => {
    const component = build(of(allOperations()));

    for (const operation of component.operationFilters) {
      component.setFilter(operation.filter);
      expect(component.instances.map((instance) => instance.name)).toEqual([
        `${operation.state}-live`,
        `${operation.state}-offline`,
      ]);
      expect(component.count('all')).toBe(10);
      for (const counted of component.operationFilters) {
        expect(component.count(counted.filter)).toBe(2);
      }
    }
  });

  it('keeps the selected operation when refreshed data changes its members', () => {
    let data = allOperations();
    const component = build(defer(() => of(data)));
    component.setFilter('swap-in');

    data = [
      new Instance('replacement', 'Demo', false, true, 's', 'v', 'SWAP_IN'),
      new Instance('enabled', 'Demo', true, true, 's', 'v', 'ENABLE'),
    ];
    component.load();

    expect(component.filter).toBe('swap-in');
    expect(component.instances.map((instance) => instance.name)).toEqual([
      'replacement',
    ]);
    expect(component.count('swap-in')).toBe(1);
    expect(component.count('all')).toBe(2);
    expect(component.count('evacuate')).toBe(0);
  });

  it('clears the spinner and records an error when the read fails', () => {
    const component = build(throwError('cannot reach helix-rest'));
    expect(component.isLoading).toBe(false);
    expect(component.error).toBe('cannot reach helix-rest');
  });
});
