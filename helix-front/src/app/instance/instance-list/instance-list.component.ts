import { Component, OnDestroy, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { Subscription, of } from 'rxjs';
import { catchError } from 'rxjs/operators';

import { Settings } from '../../core/settings';
import {
  Instance,
  InstanceErrorSummary,
  InstanceOperationState,
} from '../shared/instance.model';
import { InstanceService } from '../shared/instance.service';
import {
  OperationRow,
  formatUtc,
  operationDetail,
  operationRows,
} from '../shared/instance-operations';
import { HelperService } from '../../shared/helper.service';

type InstanceFilter =
  | 'all'
  | 'membership'
  | 'offline'
  | 'enabled'
  | 'disabled'
  | 'evacuate'
  | 'swap-in'
  | 'unknown';

@Component({
  selector: 'hi-instance-list',
  templateUrl: './instance-list.component.html',
  styleUrls: ['./instance-list.component.scss'],
})
export class InstanceListComponent implements OnInit, OnDestroy {
  isLoading = true;
  error = '';
  updated: number | null = null;
  clusterName: string;
  allInstances: Instance[] = [];
  instances: Instance[] = [];
  filter: InstanceFilter = 'all';
  // null when the error summaries could not be read
  errors: Record<string, InstanceErrorSummary> | null = {};
  // the error scan reads ZooKeeper per instance, so it fills in after the table
  errorsPending = false;
  private summaryRead: Subscription;
  private errorRead: Subscription;
  readonly operationFilters: ReadonlyArray<{
    filter: InstanceFilter;
    state: InstanceOperationState;
    label: string;
  }> = [
    { filter: 'enabled', state: 'ENABLE', label: 'Enabled' },
    { filter: 'disabled', state: 'DISABLE', label: 'Disabled' },
    { filter: 'evacuate', state: 'EVACUATE', label: 'Evacuating' },
    { filter: 'swap-in', state: 'SWAP_IN', label: 'Swap in' },
    { filter: 'unknown', state: 'UNKNOWN', label: 'Unknown' },
  ];
  rowHeight = Settings.tableRowHeight;
  headerHeight = Settings.tableHeaderHeight;
  sorts = [
    { prop: 'online', dir: 'asc' },
    { prop: 'name', dir: 'asc' },
  ];

  constructor(
    protected route: ActivatedRoute,
    protected router: Router,
    protected service: InstanceService,
    protected helper: HelperService
  ) {}

  ngOnInit() {
    if (this.route.parent) {
      this.clusterName = this.route.parent.snapshot.params['name'];
      this.load();
    }
  }

  ngOnDestroy() {
    this.summaryRead?.unsubscribe();
    this.errorRead?.unsubscribe();
  }

  load() {
    this.isLoading = true;
    this.error = '';
    this.summaryRead?.unsubscribe();
    this.errorRead?.unsubscribe();
    this.summaryRead = this.service.getSummaries(this.clusterName).subscribe(
      (data) => {
        this.allInstances = data;
        this.applyFilter();
        this.updated = Date.now();
        this.isLoading = false;
        this.readErrors();
      },
      (error) => {
        // finish the loading flag on failure instead of leaving a spinner
        this.error =
          typeof error === 'string' ? error : 'Cannot load instances.';
        this.isLoading = false;
        this.helper.showError(error);
      },
    );
  }

  private readErrors() {
    this.errors = {};
    this.errorsPending = true;
    this.errorRead = this.service
      .getErrorSummaries(this.clusterName)
      .pipe(catchError(() => of(null)))
      .subscribe((errors) => {
        this.errors = errors;
        this.errorsPending = false;
      });
  }

  errorCount(instance: Instance): number | null {
    return this.errors ? this.errors[instance.name]?.errorCount || 0 : null;
  }

  errorTitle(instance: Instance): string {
    const resources = this.errors?.[instance.name]?.resources || {};
    return Object.entries(resources)
      .map(([resource, count]) => `${resource}: ${count}`)
      .join(', ');
  }

  operations(instance: Instance): OperationRow[] {
    return operationRows(instance.summary, instance.operationState);
  }

  operationDetail(row: OperationRow): string {
    return operationDetail(row);
  }

  operationTitle(row: OperationRow): string {
    return `${row.entry.operation} from ${row.entry.source || 'unknown source'}${row.active ? ' (active)' : ''}: ${operationDetail(row)}`;
  }

  markerUntil(instance: Instance): string {
    const until = instance.summary?.maintenanceMarkerExpiresAtMillis;
    return until ? formatUtc(until) : '';
  }

  capacityText(instance: Instance): string {
    return Object.entries(instance.summary?.capacity || {})
      .map(([key, value]) => `${key} ${value}`)
      .join(', ');
  }

  setFilter(filter: InstanceFilter) {
    this.filter = filter;
    this.applyFilter();
  }

  count(filter: InstanceFilter): number {
    return this.allInstances.filter((instance) =>
      this.matches(instance, filter)
    ).length;
  }

  private matches(instance: Instance, filter: InstanceFilter): boolean {
    switch (filter) {
      case 'all':
        return true;
      case 'membership':
        // membership/operation problems only: offline, disabled, or otherwise
        // not enabled. This does not assess resource ERROR or under-replication.
        return !instance.healthy;
      case 'offline':
        return !instance.online;
      default:
        return this.operationFilters.some(
          (operation) =>
            operation.filter === filter &&
            instance.operationState === operation.state,
        );
    }
  }

  private applyFilter() {
    this.instances = this.allInstances.filter((instance) =>
      this.matches(instance, this.filter),
    );
  }

  onSelect({ selected }) {
    const row = selected[0];
    this.router.navigate([row.name], { relativeTo: this.route });
  }
}
