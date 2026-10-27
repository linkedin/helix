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

import { describe, expect, it } from '@jest/globals';

import { ListStatusComponent } from './list-status.component';

describe('ListStatusComponent', () => {
  function make(): ListStatusComponent {
    return new ListStatusComponent();
  }

  it('shows the empty state only after a completed load returned zero rows', () => {
    const c = make();
    c.loading = false;
    c.error = '';
    c.count = 0;
    expect(c.showEmpty).toBe(true);
  });

  it('does not show the empty state while loading', () => {
    const c = make();
    c.loading = true;
    c.count = 0;
    expect(c.showEmpty).toBe(false);
  });

  it('does not show the empty state when a load errored', () => {
    const c = make();
    c.loading = false;
    c.error = 'Cannot reach helix-rest.';
    c.count = 0;
    // an error is not an empty success; the two states are mutually exclusive
    expect(c.showEmpty).toBe(false);
  });

  it('does not show the empty state before the first load resolves', () => {
    const c = make();
    c.loading = false;
    c.count = null;
    expect(c.showEmpty).toBe(false);
  });

  it('formats a sampled timestamp in UTC only when there is one', () => {
    const c = make();
    expect(c.sampledAt).toBe('');
    c.updated = Date.UTC(2026, 8, 8, 20, 52, 20);
    expect(c.sampledAt).toBe('2026-09-08 20:52:20 UTC');
  });

  it('emits refresh when asked', () => {
    const c = make();
    let fired = 0;
    c.refresh.subscribe(() => (fired += 1));
    c.refresh.emit();
    expect(fired).toBe(1);
  });
});
