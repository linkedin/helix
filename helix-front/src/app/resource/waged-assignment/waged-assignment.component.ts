import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, of, Subscription } from 'rxjs';
import { catchError } from 'rxjs/operators';

import { Resource } from '../shared/resource.model';
import { ResourceService } from '../shared/resource.service';
import {
  AssignmentKind,
  AssignmentRow,
  compareAssignment,
  PartitionMap,
} from './waged-assignment.model';

export type CompareWith = 'none' | 'idealState' | 'externalView';

const ABOUT: Record<AssignmentKind, { title: string; text: string }> = {
  baseline: {
    title: 'Baseline',
    text: 'The steady placement WAGED computes while ignoring short term conditions such as an instance being down. It changes only on a global rebalance.',
  },
  bestPossible: {
    title: 'Best possible',
    text: 'The placement WAGED computed in its latest rebalance, before delayed rebalance and preference list adjustments. The external view is what replicas actually hold.',
  },
};

const OTHER_NAMES: Record<Exclude<CompareWith, 'none'>, string> = {
  idealState: 'ideal state',
  externalView: 'external view',
};

@Component({
  selector: 'hi-waged-assignment',
  templateUrl: './waged-assignment.component.html',
  styleUrls: ['./waged-assignment.component.scss'],
})
export class WagedAssignmentComponent implements OnInit, OnDestroy {
  kind: AssignmentKind = 'bestPossible';
  clusterName = '';
  resourceName = '';
  isLoading = true;
  error = '';
  notSaved = false;
  compareWith: CompareWith = 'none';
  rows: AssignmentRow[] = [];
  differing = 0;
  onlyDiffering = false;
  version = '';
  written: number | null = null;
  readAt: number | null = null;
  // the cluster's PERSIST_* flags, null when the config could not be read
  persist: { bestPossible: boolean; intermediate: boolean } | null = null;
  private saved: PartitionMap = {};
  private resource: Resource | null = null;
  private sub: Subscription;

  get about() {
    return ABOUT[this.kind];
  }

  get otherName(): string {
    return this.compareWith === 'none' ? '' : OTHER_NAMES[this.compareWith];
  }

  get shown(): AssignmentRow[] {
    return this.onlyDiffering
      ? this.rows.filter((r) => r.difference)
      : this.rows;
  }

  constructor(
    private route: ActivatedRoute,
    private service: ResourceService,
  ) {}

  ngOnInit() {
    this.kind = this.route.snapshot.data.kind || 'bestPossible';
    this.clusterName = this.route.parent.snapshot.params.cluster_name;
    this.resourceName = this.route.parent.snapshot.params.resource_name;
    this.load();
  }

  ngOnDestroy() {
    this.sub?.unsubscribe();
  }

  load() {
    this.isLoading = true;
    this.error = '';
    this.notSaved = false;
    this.sub?.unsubscribe();
    this.sub = forkJoin([
      this.service.getWagedAssignment(
        this.clusterName,
        this.resourceName,
        this.kind,
      ),
      this.service.get(this.clusterName, this.resourceName),
      // only needed to explain the ideal state comparison
      this.service
        .getClusterConfig(this.clusterName)
        .pipe(catchError(() => of(null))),
    ]).subscribe(
      ([saved, resource, config]) => {
        this.saved = saved.assignment?.[this.resourceName] || {};
        this.resource = resource;
        const flag = (key: string) =>
          String(config?.simpleFields?.[key]).toLowerCase() === 'true';
        this.persist = config
          ? {
              bestPossible: flag('PERSIST_BEST_POSSIBLE_ASSIGNMENT'),
              intermediate: flag('PERSIST_INTERMEDIATE_ASSIGNMENT'),
            }
          : null;
        this.version = saved.metadata?.version || '';
        this.written = saved.metadata?.writeTimeMs || null;
        this.readAt = Date.now();
        this.compare(this.compareWith);
        this.isLoading = false;
      },
      (error) => {
        // helix-rest answers 404 until the controller saves an assignment
        this.notSaved = /not found/i.test(String(error));
        this.error = this.notSaved ? '' : String(error);
        this.rows = [];
        this.isLoading = false;
      },
    );
  }

  compare(compareWith: CompareWith) {
    this.compareWith = compareWith;
    const other =
      compareWith === 'none'
        ? null
        : this.resource?.[compareWith]?.mapFields || {};
    this.rows = compareAssignment(this.saved, other, this.otherName);
    this.differing = this.rows.filter((r) => r.difference).length;
    this.onlyDiffering = this.onlyDiffering && this.differing > 0;
  }

  utc(time: number): string {
    return new Date(time).toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
  }
}
