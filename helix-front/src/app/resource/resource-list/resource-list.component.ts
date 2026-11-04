import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { Observable, Subscription } from 'rxjs';
import { map, switchMap } from 'rxjs/operators';

import * as _ from 'lodash';

import { Settings } from '../../core/settings';
import { Resource, ResourceSummary } from '../shared/resource.model';
import { ResourceService } from '../shared/resource.service';
import { WorkflowService } from '../../workflow/shared/workflow.service';
import { HelperService } from '../../shared/helper.service';
import { DatatableComponent } from '@swimlane/ngx-datatable';

type ResourceFilter = 'all' | 'waged' | 'classic' | 'disabled' | 'no-view';

@Component({
  selector: 'hi-resource-list',
  templateUrl: './resource-list.component.html',
  styleUrls: ['./resource-list.component.scss'],
  providers: [WorkflowService],
})
export class ResourceListComponent implements OnInit, OnDestroy {
  @ViewChild('resourcesTable', { static: false })
  table: DatatableComponent;

  isForInstance = false;
  headerHeight = Settings.tableHeaderHeight;
  rowHeight = Settings.tableRowHeight;
  resources: Array<Resource | ResourceSummary>;
  allResources: ResourceSummary[] = [];
  filter: ResourceFilter = 'all';
  readonly filters: ReadonlyArray<{ filter: ResourceFilter; label: string }> = [
    { filter: 'all', label: 'All' },
    { filter: 'waged', label: 'WAGED' },
    { filter: 'classic', label: 'Not WAGED' },
    { filter: 'disabled', label: 'Disabled' },
    { filter: 'no-view', label: 'No external view' },
  ];
  error = '';
  updated: number | null = null;
  isLoading = true;
  clusterName: string;
  instanceName: string;
  // the read in flight, replaced when a new cluster or a refresh starts one
  private read: Subscription;
  sorts = [
    { prop: 'alive', dir: 'asc' },
    { prop: 'name', dir: 'asc' },
  ];

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private service: ResourceService,
    private workflowService: WorkflowService,
    protected helper: HelperService
  ) {}

  ngOnInit() {
    if (this.route.parent) {
      if (this.route.snapshot.data.forInstance) {
        this.isForInstance = true;
        this.isLoading = true;
        this.clusterName = this.route.parent.snapshot.params.cluster_name;
        this.instanceName = this.route.parent.snapshot.params.instance_name;

        this.service
          .getAllOnInstance(this.clusterName, this.instanceName)
          .subscribe(
            (resources) => (this.resources = resources),
            (error) => console.log(error),
            () => (this.isLoading = false)
          );
      } else {
        this.route.parent.params.pipe(map((p) => p.name)).subscribe((name) => {
          if (name !== this.clusterName) {
            // do not leave the previous cluster's rows under the new name
            this.allResources = [];
            this.resources = [];
            this.updated = null;
          }
          this.clusterName = name;
          this.fetchResources();
        });
      }
    }
  }

  ngOnDestroy() {
    this.read?.unsubscribe();
  }

  // since resource list contains also workflows and jobs
  // need to subtract them from original resource list
  // to obtain all jobs list, need to go through every workflow
  // and perform get request for each.
  // However, it has huge performance issue when there are thousands of
  // workflows. We are using a smart way here: to remove resources whose
  // prefix is a workflow name
  protected fetchResources() {
    this.isLoading = true;
    this.error = '';
    this.read?.unsubscribe();

    const cluster = this.clusterName;
    this.read = this.workflowService
      .getAll(cluster)
      .pipe(
        switchMap((workflows) =>
          this.service
            .getSummaries(cluster)
            .pipe(
              map((result) =>
                _.differenceWith(
                  result,
                  workflows,
                  (resource: ResourceSummary, prefix: string) =>
                    _.startsWith(resource.name, prefix),
                ),
              ),
            ),
        ),
      )
      .subscribe(
        (resources) => {
          this.allResources = resources;
          this.applyFilter();
          this.updated = Date.now();
          this.isLoading = false;
        },
        (error) => this.fail(error),
      );
  }

  reload() {
    this.fetchResources();
  }

  setFilter(filter: ResourceFilter) {
    this.filter = filter;
    this.applyFilter();
  }

  count(filter: ResourceFilter): number {
    return this.allResources.filter((resource) => this.matches(resource, filter))
      .length;
  }

  private matches(resource: ResourceSummary, filter: ResourceFilter): boolean {
    switch (filter) {
      case 'waged':
        return resource.waged;
      case 'classic':
        return !resource.waged;
      case 'disabled':
        return !resource.enabled;
      case 'no-view':
        return !resource.hasExternalView;
      default:
        return true;
    }
  }

  private applyFilter() {
    this.resources = this.allResources.filter((resource) =>
      this.matches(resource, this.filter),
    );
  }

  private fail(error: any) {
    this.error =
      typeof error === 'string' ? error : 'Cannot load resources.';
    this.isLoading = false;
    this.helper.showError(error);
  }

  onSelect({ selected }) {
    const row = selected[0];

    if (this.isForInstance) {
      this.table.rowDetail.toggleExpandRow(row);
    } else {
      this.router.navigate([row.name], { relativeTo: this.route });
    }
  }
}
