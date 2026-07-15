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

  // Aggregated per-task status summary. Helix writes it as a JSON string into the
  // JobContext simple field TASK_STATUS_SUMMARY when the job reaches a terminal state. Surfaces
  // partial failures even when the job's own state flag is COMPLETED.
  get taskSummary(): any {
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

  get hasTaskFailures(): boolean {
    const summary = this.taskSummary;
    return !!summary && summary.failed > 0;
  }
}
