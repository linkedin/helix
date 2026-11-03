import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';

import { HelperService } from '../../shared/helper.service';
import { CapacityService } from '../shared/capacity.service';
import {
  CapacitySnapshot,
  InstanceCapacity,
} from '../shared/capacity-snapshot.model';

export interface CapacityRow {
  instance: InstanceCapacity;
  // null when used or max is unknown for the selected key
  ratio: number | null;
}

@Component({
  selector: 'hi-capacity-overview',
  templateUrl: './capacity-overview.component.html',
  styleUrls: ['./capacity-overview.component.scss'],
})
export class CapacityOverviewComponent implements OnInit, OnDestroy {
  clusterName: string;
  snapshot: CapacitySnapshot | null = null;
  rows: CapacityRow[] = [];
  key = '';
  topStateOnly = false;
  isLoading = true;
  error = '';
  updated: number | null = null;

  private sub: Subscription;

  constructor(
    private route: ActivatedRoute,
    private service: CapacityService,
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
    this.sub = this.service.getSnapshot(this.clusterName).subscribe(
      (snapshot) => {
        this.snapshot = snapshot;
        if (!snapshot.capacityKeys.includes(this.key)) {
          this.key = snapshot.capacityKeys[0] || '';
        }
        this.updated = Date.parse(snapshot.generatedAt) || Date.now();
        this.buildRows();
        this.isLoading = false;
      },
      (error) => {
        this.error =
          typeof error === 'string' ? error : 'Cannot load the capacity snapshot.';
        this.isLoading = false;
        this.helper.showError(error);
      },
    );
  }

  setKey(key: string) {
    this.key = key;
    this.buildRows();
  }

  setTopStateOnly(topStateOnly: boolean) {
    this.topStateOnly = topStateOnly;
    this.buildRows();
  }

  min(a: number, b: number): number {
    return Math.min(a, b);
  }

  ratioText(row: CapacityRow): string {
    return row.ratio === null ? 'unknown' : `${Math.round(row.ratio * 100)}%`;
  }

  usedText(row: CapacityRow): string {
    const used = this.usedMap(row.instance)?.[this.key];
    const max = row.instance.max?.[this.key];
    return used == null || max == null ? '' : `${used} / ${max}`;
  }

  private usedMap(instance: InstanceCapacity) {
    return this.topStateOnly ? instance.topStateUsed : instance.used;
  }

  private buildRows() {
    const rows = (this.snapshot?.instances || []).map((instance) => {
      const used = this.usedMap(instance)?.[this.key];
      const max = instance.max?.[this.key];
      return <CapacityRow>{
        instance,
        ratio: used == null || !max || max <= 0 ? null : used / max,
      };
    });
    // hottest first, unknown last
    this.rows = rows.sort(
      (a, b) =>
        (b.ratio ?? -1) - (a.ratio ?? -1) ||
        a.instance.name.localeCompare(b.instance.name),
    );
  }
}
