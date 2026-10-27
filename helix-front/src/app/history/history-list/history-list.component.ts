import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';

import { Settings } from '../../core/settings';
import { HistoryService } from '../shared/history.service';
import { History } from '../shared/history.model';
import { HISTORY_ZONES, HistoryZone, formatTime } from '../shared/history-time';

const ZONE_KEY = 'helix.history.zone';

@Component({
  selector: 'hi-history-list',
  templateUrl: './history-list.component.html',
  styleUrls: ['./history-list.component.scss'],
  providers: [HistoryService],
  // FIXME: have to turn off shadow dom or .current-controller won't work
  encapsulation: ViewEncapsulation.None,
})
export class HistoryListComponent implements OnInit {
  rows: History[];
  rowHeight = Settings.tableRowHeight;
  headerHeight = Settings.tableHeaderHeight;
  isController: boolean;
  isLoading = true;
  error = '';
  updated: number | null = null;
  sorts = [{ prop: 'time', dir: 'desc' }];
  zones = HISTORY_ZONES;
  zone: HistoryZone = HISTORY_ZONES[0];
  currentSession = '';

  private clusterName: string;
  private instanceName: string;

  // to let ngx-datatable helper funcs have 'this' context
  bindFunc = _.bind;

  constructor(
    private route: ActivatedRoute,
    private service: HistoryService,
  ) {}

  ngOnInit() {
    let saved: string | null = null;
    try {
      saved = localStorage.getItem(ZONE_KEY);
    } catch (e) {
      // storage can be blocked, the default zone is used instead
    }
    this.zone = this.zones.find((z) => z.key === saved) || this.zones[0];
    if (this.route.parent) {
      this.clusterName = this.route.parent.snapshot.params['cluster_name'];
      this.instanceName = this.route.parent.snapshot.params['instance_name'];
      this.isController = !this.instanceName;
      this.load();
    }
  }

  load() {
    this.isLoading = true;
    this.error = '';
    const observable = this.instanceName
      ? this.service.getInstanceHistory(this.clusterName, this.instanceName)
      : this.service.getControllerHistory(this.clusterName);

    observable.subscribe(
      (histories) => {
        this.rows = histories;
        this.currentSession = this.latestSession(histories);
        this.updated = Date.now();
      },
      (error) => {
        // only an instance with no history node reads as Not Found. The
        // controller history returns an empty list when absent, so a 404 there
        // is a real failure, as is any network or server error.
        if (this.instanceName && error === 'Not Found') {
          this.rows = [];
          this.updated = Date.now();
        } else {
          this.error =
            typeof error === 'string' ? error : 'Cannot load history.';
        }
        this.isLoading = false;
      },
      () => (this.isLoading = false),
    );
  }

  setZone(key: string) {
    this.zone = this.zones.find((z) => z.key === key) || this.zones[0];
    try {
      localStorage.setItem(ZONE_KEY, this.zone.key);
    } catch (e) {
      // remembering the zone is best effort, it stays in memory
    }
  }

  format(ms: number) {
    return formatTime(ms, this.zone.tz);
  }

  private latestSession(rows: History[]) {
    const online = rows.filter((r) => r.event === 'Online');
    return online.length
      ? online.reduce((a, b) => (b.time > a.time ? b : a)).session
      : '';
  }

  getControllerCellClass({ value }): any {
    return {
      current: value == this.rows[this.rows.length - 1].controller,
    };
  }

  getEventCellClass({ value }): any {
    return { offline: value === 'Offline' };
  }

  getSessionCellClass({ value }): any {
    return {
      current: value && value == this.currentSession,
    };
  }
}
