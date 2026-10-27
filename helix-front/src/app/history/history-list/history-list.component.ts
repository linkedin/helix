import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import * as _ from 'lodash';

import { Settings } from '../../core/settings';
import { HistoryService } from '../shared/history.service';
import { History } from '../shared/history.model';

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
  sorts = [{ prop: 'date', dir: 'desc' }];

  private clusterName: string;
  private instanceName: string;

  // to let ngx-datatable helper funcs have 'this' context
  bindFunc = _.bind;

  constructor(private route: ActivatedRoute, private service: HistoryService) {}

  ngOnInit() {
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
        this.updated = Date.now();
      },
      (error) => {
        // an absent history node reads as Not Found; treat that as empty,
        // but surface real network or server failures instead of a blank list
        if (error === 'Not Found') {
          this.rows = [];
          this.updated = Date.now();
        } else {
          this.error =
            typeof error === 'string' ? error : 'Cannot load history.';
        }
        this.isLoading = false;
      },
      () => (this.isLoading = false)
    );
  }

  getControllerCellClass({ value }): any {
    return {
      current: value == this.rows[this.rows.length - 1].controller,
    };
  }

  getSessionCellClass({ value }): any {
    return {
      current: value == this.rows[this.rows.length - 1].session,
    };
  }
}
