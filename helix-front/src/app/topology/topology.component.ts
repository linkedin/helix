import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, Subscription } from 'rxjs';

import { HelperService } from '../shared/helper.service';
import { CapacityService } from '../capacity/shared/capacity.service';
import { CapacitySnapshot } from '../capacity/shared/capacity-snapshot.model';
import { InstanceService } from '../instance/shared/instance.service';
import {
  average,
  buildTopology,
  groupIds,
  KeyUsage,
  TopologyNode,
  TopologyRow,
  visibleRows,
} from './topology.model';

@Component({
  selector: 'hi-topology',
  templateUrl: './topology.component.html',
  styleUrls: [
    '../capacity/capacity-overview/capacity-overview.component.scss',
    './topology.component.scss',
  ],
})
export class TopologyComponent implements OnInit, OnDestroy {
  clusterName: string;
  snapshot: CapacitySnapshot | null = null;
  tree: TopologyNode[] = [];
  rows: TopologyRow[] = [];
  expanded = new Set<string>();
  key = '';
  query = '';
  isLoading = true;
  error = '';
  updated: number | null = null;
  domainsReadAt: number | null = null;

  private sub: Subscription;

  constructor(
    private route: ActivatedRoute,
    private capacity: CapacityService,
    private instances: InstanceService,
    private helper: HelperService,
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
    // The snapshot has state, capacity and ERROR replicas as of one sample and
    // can come from the server cache. The summaries are read now, only for the
    // full domain, so the two times are kept apart and shown.
    this.sub = forkJoin([
      this.capacity.getSnapshot(this.clusterName),
      this.instances.getSummaries(this.clusterName),
    ]).subscribe(
      ([snapshot, summaries]) => {
        const first = !this.snapshot;
        this.snapshot = snapshot;
        if (!snapshot.capacityKeys.includes(this.key)) {
          this.key = snapshot.capacityKeys[0] || '';
        }
        this.tree = buildTopology(
          snapshot.instances,
          new Map(summaries.map((i) => [i.name, i.summary?.domain])),
          snapshot.capacityKeys,
        );
        if (first) {
          this.expanded = new Set(this.tree.map((n) => n.id));
        }
        this.updated = Date.parse(snapshot.generatedAt) || Date.now();
        this.domainsReadAt = Date.now();
        this.render();
        this.isLoading = false;
      },
      (error) => {
        this.error =
          typeof error === 'string' ? error : 'Cannot load the topology.';
        this.isLoading = false;
        this.helper.showError(error);
      },
    );
  }

  setQuery(query: string) {
    this.query = query;
    this.render();
  }

  toggle(node: TopologyNode) {
    if (!this.expanded.delete(node.id)) {
      this.expanded.add(node.id);
    }
    this.render();
  }

  expandAll(open: boolean) {
    this.expanded = new Set(open ? groupIds(this.tree) : []);
    this.render();
  }

  usage(node: TopologyNode): KeyUsage {
    return node.stats.usage[this.key];
  }

  averageText(node: TopologyNode): string {
    return this.pct(average(this.usage(node)));
  }

  utc(time: number | null): string {
    return time === null
      ? ''
      : new Date(time).toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
  }

  pct(ratio: number | null): string {
    return ratio === null ? 'unknown' : `${Math.round(ratio * 100)}%`;
  }

  min(a: number, b: number): number {
    return Math.min(a, b);
  }

  trackRow(_index: number, row: TopologyRow): string {
    return row.node.id;
  }

  private render() {
    this.rows = visibleRows(this.tree, this.expanded, this.query);
  }
}
