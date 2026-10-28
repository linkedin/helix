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

import { Component, EventEmitter, Input, Output } from '@angular/core';

/**
 * Compact status bar for read-only list/detail views. It renders three
 * mutually exclusive states (loading, error, empty) plus a "sampled at"
 * timestamp for a successful read, and exposes a single Refresh action.
 *
 * It exists so the legacy list screens report freshness and failure the
 * same way instead of each silently swallowing errors.
 */
@Component({
  selector: 'hi-list-status',
  templateUrl: './list-status.component.html',
  styleUrls: ['./list-status.component.scss'],
})
export class ListStatusComponent {
  @Input() loading = false;
  @Input() error = '';
  // number of rows currently loaded; null means "not loaded yet"
  @Input() count: number | null = null;
  // epoch millis of the last successful sample; null means "never"
  @Input() updated: number | null = null;
  @Input() label = 'items';
  @Input() emptyText = '';

  @Output() refresh = new EventEmitter<void>();

  get showEmpty(): boolean {
    return !this.loading && !this.error && this.count === 0;
  }

  get sampledAt(): string {
    if (!this.updated) return '';
    // render in UTC so list pages never contradict the UTC-based heat map
    const iso = new Date(this.updated).toISOString();
    return iso.slice(0, 19).replace('T', ' ') + ' UTC';
  }
}
