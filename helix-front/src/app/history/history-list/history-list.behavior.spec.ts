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
import { of, throwError } from 'rxjs';

import { HistoryListComponent } from './history-list.component';

function build(data: any, instance?: string) {
  const route: any = {
    parent: {
      snapshot: {
        params: { cluster_name: 'Demo', instance_name: instance },
      },
    },
  };
  const service: any = {
    getControllerHistory: jest.fn(() => data),
    getInstanceHistory: jest.fn(() => data),
  };
  const component = new HistoryListComponent(route, service);
  component.ngOnInit();
  return component;
}

describe('HistoryListComponent error and empty states', () => {
  it('shows rows and a sample time on success', () => {
    const component = build(of([{ date: 'd', time: 1, controller: 'c' }]));
    expect(component.rows.length).toBe(1);
    expect(component.error).toBe('');
    expect(component.isLoading).toBe(false);
    expect(component.updated).not.toBeNull();
  });

  it('treats Not Found as an empty history for an instance', () => {
    const component = build(throwError('Not Found'), 'host_1');
    expect(component.rows).toEqual([]);
    expect(component.error).toBe('');
    expect(component.isLoading).toBe(false);
  });

  it('reports Not Found on the controller history as an error', () => {
    const component = build(throwError('Not Found'));
    expect(component.error).toBe('Not Found');
    expect(component.rows).toBeUndefined();
    expect(component.updated).toBeNull();
  });

  it('reports a proxy configuration failure instead of an empty history', () => {
    const message =
      'The Helix endpoint is not configured for this cluster key.';
    const component = build(throwError(message), 'host_1');
    expect(component.error).toBe(message);
    expect(component.rows).toBeUndefined();
    expect(component.updated).toBeNull();
  });

  it('surfaces a real failure instead of a silent empty list', () => {
    const component = build(throwError('HTTP 500'));
    expect(component.error).toBe('HTTP 500');
    expect(component.isLoading).toBe(false);
    expect(component.rows).toBeUndefined();
  });
});
