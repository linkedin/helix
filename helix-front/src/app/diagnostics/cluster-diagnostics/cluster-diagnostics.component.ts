import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, Subscription } from 'rxjs';

import { CapacityService } from '../../capacity/shared/capacity.service';
import { InstanceService } from '../../instance/shared/instance.service';
import { ResourceService } from '../../resource/shared/resource.service';
import {
  CATEGORIES,
  Category,
  DiagnosticIssue,
  diagnosticIssues,
  SEVERITIES,
  Severity,
} from '../shared/diagnostic-rules';

@Component({
  selector: 'hi-cluster-diagnostics',
  templateUrl: './cluster-diagnostics.component.html',
  styleUrls: ['./cluster-diagnostics.component.scss'],
})
export class ClusterDiagnosticsComponent implements OnInit, OnDestroy {
  readonly severities = SEVERITIES;
  readonly categories = CATEGORIES;
  clusterName: string;
  // null until the first successful read
  issues: DiagnosticIssue[] | null = null;
  severity: Severity | null = null;
  category: Category | null = null;
  isLoading = true;
  error = '';
  updated: number | null = null;
  listsReadAt: number | null = null;

  private sub: Subscription;

  constructor(
    private route: ActivatedRoute,
    private capacityService: CapacityService,
    private instanceService: InstanceService,
    private resourceService: ResourceService,
  ) {}

  ngOnInit() {
    if (this.route.parent) {
      this.clusterName = this.route.parent.snapshot.params['name'];
      this.load();
    }
  }

  ngOnDestroy() {
    this.sub?.unsubscribe();
  }

  load() {
    this.isLoading = true;
    this.error = '';
    this.sub?.unsubscribe();
    this.sub = forkJoin([
      this.capacityService.getSnapshot(this.clusterName),
      this.instanceService.getSummaries(this.clusterName),
      this.resourceService.getSummaries(this.clusterName),
    ]).subscribe(
      ([snapshot, instances, resources]) => {
        this.issues = diagnosticIssues(snapshot, instances, resources);
        this.updated = Date.parse(snapshot.generatedAt) || Date.now();
        this.listsReadAt = Date.now();
        this.isLoading = false;
      },
      (error) => {
        this.error =
          typeof error === 'string' ? error : 'Cannot load diagnostics.';
        this.isLoading = false;
      },
    );
  }

  get visible(): DiagnosticIssue[] {
    return (this.issues || []).filter(
      (issue) =>
        (!this.severity || issue.severity === this.severity) &&
        (!this.category || issue.category === this.category),
    );
  }

  count(severity: Severity | null, category: Category | null): number {
    return (this.issues || []).filter(
      (issue) =>
        (!severity || issue.severity === severity) &&
        (!category || issue.category === category),
    ).length;
  }

  utc(time: number | null): string {
    return time === null
      ? ''
      : new Date(time).toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
  }

  categoryLabel(category: Category): string {
    return this.categories.find((c) => c.value === category)?.label || '';
  }

  link(issue: DiagnosticIssue): string[] {
    return ['../', ...issue.link];
  }
}
