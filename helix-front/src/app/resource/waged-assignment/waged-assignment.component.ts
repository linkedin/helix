import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, Subscription } from 'rxjs';

import { ResourceService } from '../shared/resource.service';
import {
  AssignmentKind,
  AssignmentRow,
  compareAssignment,
} from './waged-assignment.model';

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
  rows: AssignmentRow[] = [];
  differing = 0;
  onlyDiffering = false;
  version = '';
  written: number | null = null;
  readAt: number | null = null;
  private sub: Subscription;

  get about() {
    return ABOUT[this.kind];
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
    ]).subscribe(
      ([saved, resource]) => {
        this.rows = compareAssignment(
          saved.assignment?.[this.resourceName] || {},
          resource.externalView?.mapFields || {},
        );
        this.differing = this.rows.filter((r) => r.difference).length;
        this.version = saved.metadata?.version || '';
        this.written = saved.metadata?.writeTimeMs || null;
        this.readAt = Date.now();
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

  utc(time: number): string {
    return new Date(time).toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
  }
}
