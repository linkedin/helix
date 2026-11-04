import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { of, Subscription } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { Settings } from '../../core/settings';
import { Instance } from '../../instance/shared/instance.model';
import { InstanceService } from '../../instance/shared/instance.service';
import { ZNRecord } from '../../shared/models/znrecord.model';
import { Resource } from '../shared/resource.model';
import { ResourceService } from '../shared/resource.service';
import { DatatableComponent } from '@swimlane/ngx-datatable';
import {
  diagnosePartitions,
  matchesFilter,
  PARTITION_FILTERS,
  PartitionFilter,
  PartitionRow,
} from '../shared/partition-diagnostics';

@Component({
  selector: 'hi-partition-list',
  templateUrl: './partition-list.component.html',
  styleUrls: ['./partition-list.component.scss'],
})
export class PartitionListComponent implements OnInit, OnDestroy {
  @ViewChild('partitionsTable', { static: false })
  table: DatatableComponent;

  isLoading = true;
  clusterName: string;
  resource: Resource;
  readonly filters = PARTITION_FILTERS;
  resourceName: string;
  // null until the first successful read
  partitions: PartitionRow[] | null = null;
  rows: PartitionRow[] = [];
  // null count means the filter does not apply to this resource
  counts: Partial<Record<PartitionFilter, number | null>> = {};
  filter: PartitionFilter = 'all';
  search = '';
  offset = 0;
  // instance liveness is a second read that fills in after the partitions
  instancesPending = false;
  error = '';
  warnings: string[] = [];
  updated: number | null = null;
  headerHeight = Settings.tableHeaderHeight;
  rowHeight = Settings.tableRowHeight;
  private model: ZNRecord | null = null;
  private instances: Instance[] | null = null;
  private routeRead: Subscription;
  private reads: Subscription;

  constructor(
    protected route: ActivatedRoute,
    protected service: ResourceService,
    protected instanceService: InstanceService,
  ) {}

  ngOnInit() {
    this.routeRead = this.route.parent?.paramMap.subscribe((params) => {
      this.clusterName = params.get('cluster_name');
      this.resourceName = params.get('resource_name');
      this.resource = this.partitions = null;
      this.rows = [];
      this.load();
    });
  }

  ngOnDestroy() {
    this.routeRead?.unsubscribe();
    this.reads?.unsubscribe();
  }

  load() {
    const cluster = this.clusterName;
    // superseded reads must never overwrite newer data
    this.reads?.unsubscribe();
    this.reads = new Subscription();
    this.isLoading = this.instancesPending = true;
    this.error = '';
    this.warnings = [];
    this.instances = null;
    this.reads.add(
      this.service
        .get(cluster, this.resourceName)
        .pipe(
          switchMap((resource) =>
            this.service.getStateModel(cluster, resource.stateModel).pipe(
              catchError((error) => {
                this.warnings.push(
                  `State model ${resource.stateModel} could not be read (${error}), so serving is not judged.`,
                );
                return of(null);
              }),
              map((model) => ({ resource, model })),
            ),
          ),
        )
        .subscribe(
          ({ resource, model }) => {
            this.resource = resource;
            this.model = model;
            this.updated = Date.now();
            this.isLoading = false;
            this.diagnose();
          },
          (error) => {
            this.error =
              typeof error === 'string' ? error : 'Cannot load the resource.';
            this.isLoading = false;
          },
        ),
    );
    this.reads.add(
      this.instanceService
        .getSummaries(cluster)
        .pipe(
          catchError((error) => {
            this.warnings.push(
              `Instance status could not be read (${error}). Liveness is unknown, so no replica is reported as not live.`,
            );
            return of(null);
          }),
        )
        .subscribe((instances) => {
          this.instances = instances;
          this.instancesPending = false;
          this.diagnose();
        }),
    );
  }

  setFilter(filter: PartitionFilter, search = this.search) {
    this.filter = filter;
    this.search = search;
    this.applyFilter();
  }

  onSelect({ selected }) {
    const row = selected[0];

    this.table.rowDetail.toggleExpandRow(row);
  }

  private diagnose() {
    if (!this.resource) return;
    const rows = diagnosePartitions(this.resource, this.model, this.instances);
    this.partitions = rows;
    for (const { value } of this.filters) {
      this.counts[value] = rows.filter((row) =>
        matchesFilter(row, value),
      ).length;
    }
    // FULL_AUTO and WAGED keep no per-partition target to compare against
    if (rows.every((row) => row.assignmentDiffers === null)) {
      this.counts.assignment = null;
    }
    this.applyFilter();
  }

  private applyFilter() {
    const search = this.search.trim().toLowerCase();
    this.rows = (this.partitions || []).filter(
      (row) =>
        matchesFilter(row, this.filter) &&
        row.name.toLowerCase().includes(search),
    );
    this.offset = 0;
  }
}
