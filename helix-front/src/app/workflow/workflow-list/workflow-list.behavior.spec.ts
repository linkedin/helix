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

import { WorkflowListComponent } from './workflow-list.component';

function build(data: any) {
  const route: any = { parent: { snapshot: { params: { name: 'Demo' } } } };
  const router: any = { navigate: jest.fn() };
  const service: any = { getAll: jest.fn(() => data) };
  const component = new WorkflowListComponent(router, route, service);
  component.ngOnInit();
  return component;
}

describe('WorkflowListComponent error and empty states', () => {
  it('maps workflow names to rows on success', () => {
    const component = build(of(['flow-a', 'flow-b']));
    expect(component.workflowRows.length).toBe(2);
    expect(component.workflowRows[0].name).toBe('flow-a');
    expect(component.error).toBe('');
    expect(component.isLoading).toBe(false);
  });

  it('reports a 404 as an error, not an empty list', () => {
    const component = build(throwError('Not Found'));
    expect(component.error).toBe('Not Found');
    expect(component.updated).toBeNull();
    expect(component.isLoading).toBe(false);
  });

  it('surfaces a real failure instead of only logging it', () => {
    const component = build(throwError('HTTP 503'));
    expect(component.error).toBe('HTTP 503');
    expect(component.isLoading).toBe(false);
  });
});
