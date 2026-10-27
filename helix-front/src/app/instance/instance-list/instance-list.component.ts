import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { Settings } from '../../core/settings';
import { Instance, InstanceOperationState } from '../shared/instance.model';
import { InstanceService } from '../shared/instance.service';
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
export class InstanceListComponent implements OnInit {
  isLoading = true;
  error = '';
  updated: number | null = null;
  clusterName: string;
  allInstances: Instance[] = [];
  instances: Instance[] = [];
  filter: InstanceFilter = 'all';
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
    { prop: 'liveInstance', dir: 'asc' },
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

  load() {
    this.isLoading = true;
    this.error = '';
    this.service.getAll(this.clusterName).subscribe(
      (data) => {
        this.allInstances = data;
        this.applyFilter();
        this.updated = Date.now();
      },
      (error) => {
        // finish the loading flag on failure instead of leaving a spinner
        this.error =
          typeof error === 'string' ? error : 'Cannot load instances.';
        this.isLoading = false;
        this.helper.showError(error);
      },
      () => (this.isLoading = false),
    );
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
