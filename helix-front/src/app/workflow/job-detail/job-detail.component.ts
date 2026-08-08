import { Component, OnInit, Input } from '@angular/core';

import { Job } from '../shared/workflow.model';
import { JobService } from '../shared/job.service';
import { HelperService } from '../../shared/helper.service';

@Component({
  selector: 'hi-job-detail',
  templateUrl: './job-detail.component.html',
  styleUrls: ['./job-detail.component.scss'],
})
export class JobDetailComponent implements OnInit {
  @Input()
  job: Job;

  isLoading = true;

  constructor(protected service: JobService, protected helper: HelperService) {}

  ngOnInit() {
    this.service.get(this.job).subscribe(
      (data) => (this.isLoading = false),
      (error) => this.helper.showError(error),
      () => (this.isLoading = false)
    );
  }

  // Aggregated per-task status summary. It is computed on demand from the per-partition task
  // states carried in the JobContext, so it is refreshed every time this page is opened (ngOnInit
  // re-fetches the JobContext) rather than in the background. This surfaces partial failures even
  // when the job's own state flag is COMPLETED. If the per-partition states are not available (for
  // example the context was trimmed), it falls back to the TASK_STATUS_SUMMARY snapshot that the
  // controller materializes into the JobContext when the job reaches a terminal state.
  get taskSummary(): any {
    const live = this.computeSummaryFromStates();
    if (live) {
      return live;
    }
    const raw =
      this.job &&
      this.job.context &&
      this.job.context.simpleFields &&
      this.job.context.simpleFields.TASK_STATUS_SUMMARY;
    if (!raw) {
      return null;
    }
    try {
      return typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch (e) {
      return null;
    }
  }

  // Task partition states, mirrored from helix-core TaskPartitionState, that count as a terminal
  // failure (given up / errored / timed out).
  private static readonly FAILED_STATES = ['TASK_ERROR', 'TASK_ABORTED', 'TIMED_OUT', 'ERROR'];

  // Aggregates the raw per-partition task states (JobContext mapFields) into the same shape the
  // controller writes, so the running-job view stays accurate on each page open without any
  // background refresh. Returns null when no per-partition states are present.
  private computeSummaryFromStates(): any {
    const mapFields =
      this.job && this.job.context && this.job.context.mapFields;
    if (!mapFields || typeof mapFields !== 'object') {
      return null;
    }
    const partitions = Object.keys(mapFields).filter((k) => !isNaN(Number(k)));
    if (partitions.length === 0) {
      return null;
    }

    const byState: { [state: string]: number } = {};
    const failedTasks: number[] = [];
    const timedOutTasks: number[] = [];
    const inProgressTasks: number[] = [];
    const pendingTasks: number[] = [];
    let completed = 0;
    let failed = 0;
    let timedOut = 0;
    let inProgress = 0;
    let pending = 0;

    for (const key of partitions) {
      const p = Number(key);
      const state = (mapFields[key] && mapFields[key].STATE) || 'UNSCHEDULED';
      byState[state] = (byState[state] || 0) + 1;
      if (state === 'COMPLETED') {
        completed++;
      } else if (JobDetailComponent.FAILED_STATES.indexOf(state) !== -1) {
        failed++;
        failedTasks.push(p);
        if (state === 'TIMED_OUT') {
          timedOut++;
          timedOutTasks.push(p);
        }
      } else if (state === 'RUNNING') {
        inProgress++;
        inProgressTasks.push(p);
      } else if (state === 'INIT') {
        pending++;
        pendingTasks.push(p);
      }
    }

    const total = partitions.length;
    const numericAsc = (a: number, b: number) => a - b;
    return {
      total,
      completed,
      failed,
      timedOut,
      inProgress,
      pending,
      other: total - completed - failed - inProgress - pending,
      byState,
      failedTasks: failedTasks.sort(numericAsc),
      timedOutTasks: timedOutTasks.sort(numericAsc),
      inProgressTasks: inProgressTasks.sort(numericAsc),
      pendingTasks: pendingTasks.sort(numericAsc),
    };
  }

  get hasTaskFailures(): boolean {
    const summary = this.taskSummary;
    return !!summary && summary.failed > 0;
  }

  get hasTimedOut(): boolean {
    const summary = this.taskSummary;
    return !!summary && summary.timedOut > 0;
  }

  get hasInProgress(): boolean {
    const summary = this.taskSummary;
    return !!summary && summary.inProgress > 0;
  }

  get hasPending(): boolean {
    const summary = this.taskSummary;
    return !!summary && summary.pending > 0;
  }
}
