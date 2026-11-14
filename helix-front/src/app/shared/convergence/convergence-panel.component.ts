import { Component, Input, OnChanges, OnDestroy } from '@angular/core';
import { Subscription, timer } from 'rxjs';

import {
  ConvergenceReport,
  ConvergenceStatusName,
  duration,
  REASON_TEXT,
  reasonLabel,
  REPORT_STATE_TEXT,
  STATUS_LABELS,
  STATUS_ORDER,
} from './convergence.model';
import {
  ConvergenceService,
  ConvergenceTargetMissing,
} from './convergence.service';

export const POLL_MS = 10000;

const TONES: Record<ConvergenceStatusName, string> = {
  CONVERGED: 'ok',
  IN_PROGRESS: 'info',
  BLOCKED: 'error',
  UNKNOWN: 'warning',
  PAUSED: 'neutral',
};

// a report in these states will not change by reading again
const SETTLED_STATES = ['MONITORING_DISABLED', 'SCHEMA_UNSUPPORTED'];

interface Segment {
  status: ConvergenceStatusName;
  label: string;
  count: number;
}

interface ReasonRow {
  primary: boolean;
  label: string;
  text: string;
  count: number;
}

/**
 * Whether the current assignment of a cluster, or of one resource, has reached
 * the target assignment, as published by the leader controller. Reads again
 * every 10 s while the tab is visible, so a cluster that drifts shows up.
 */
@Component({
  selector: 'hi-convergence-panel',
  templateUrl: './convergence-panel.component.html',
  styleUrls: ['./convergence-panel.component.scss'],
})
export class ConvergencePanelComponent implements OnChanges, OnDestroy {
  @Input() cluster: string;
  @Input() resource?: string;

  report: ConvergenceReport | null = null;
  error = '';
  loading = false;
  live = false;
  readAt: number | null = null;

  tone = '';
  statusLabel = '';
  headline = '';
  confirmed = '';
  since = '';
  segments: Segment[] = [];
  barLabel = '';
  reasons: ReasonRow[] = [];
  facts: { label: string; warn: boolean }[] = [];

  private sub?: Subscription;
  private next?: Subscription;

  constructor(private service: ConvergenceService) {}

  ngOnChanges() {
    this.report = null;
    this.error = '';
    this.load();
  }

  ngOnDestroy() {
    this.sub?.unsubscribe();
    this.next?.unsubscribe();
  }

  load() {
    this.sub?.unsubscribe();
    this.next?.unsubscribe();
    if (!this.cluster) return;
    this.loading = true;
    this.sub = this.service.get(this.cluster, this.resource).subscribe(
      (report) => this.show(report, ''),
      (error) => {
        // A 404 means the REST server never will, or the target is gone. Any other failure may
        // pass, so keep polling.
        if (error instanceof ConvergenceTargetMissing) {
          this.show(null, error.message);
        } else if (error === 'Not Found') {
          this.show(
            null,
            'This REST server does not publish convergence status yet.',
          );
        } else {
          const why = typeof error === 'string' ? error : 'request timed out';
          this.show(null, `Could not read convergence status: ${why}`, true);
        }
      },
    );
  }

  private show(report: ConvergenceReport | null, error: string, retry = false) {
    this.report = report;
    this.error = error;
    this.loading = false;
    this.readAt = Date.now();
    this.describe();
    this.live =
      retry || (!!report && !SETTLED_STATES.includes(report.reportState));
    if (this.live) this.schedule();
  }

  private schedule() {
    this.next = timer(POLL_MS).subscribe(() =>
      document.hidden ? this.schedule() : this.load(),
    );
  }

  private describe() {
    const r = this.report;
    this.segments = [];
    this.reasons = [];
    this.facts = [];
    this.since = '';
    this.confirmed = '';
    if (!r) {
      this.tone = 'warning';
      this.statusLabel = '';
      this.headline = this.error;
      return;
    }
    this.statusLabel = STATUS_LABELS[r.status] || r.status;
    if (r.reportState !== 'FRESH') {
      this.tone = r.reportState === 'MANAGEMENT_MODE' ? 'neutral' : 'warning';
      this.headline = REPORT_STATE_TEXT[r.reportState] || r.reportState;
      if (r.reportState === 'REPORT_STALE' && r.evaluatedAt > 0) {
        this.headline += ` Last confirmed ${duration(this.readAt - r.evaluatedAt)} ago.`;
      }
      return;
    }
    this.tone = TONES[r.status] || 'warning';
    if (r.evaluatedAt > 0) {
      this.confirmed = duration(this.readAt - r.evaluatedAt);
    }
    const summary = r.partitionSummary;
    const total = summary?.total ?? 0;
    const scope =
      `${total} partition${total === 1 ? '' : 's'}` +
      (r.resourceCount != null
        ? ` across ${r.resourceCount} resource${r.resourceCount === 1 ? '' : 's'}`
        : '');
    const why = REASON_TEXT[r.primaryReason] || reasonLabel(r.primaryReason);
    if (r.status === 'CONVERGED') {
      this.headline = `All ${scope} match the target assignment.`;
    } else if (r.status === 'PAUSED') {
      this.headline = why;
    } else {
      const unknown = summary?.statusCounts?.UNKNOWN ?? 0;
      const stuck = (summary?.affected ?? 0) - unknown;
      const counts = stuck
        ? `${stuck} of ${scope} are not converged${unknown ? `, ${unknown} unknown` : ''}`
        : `Status unknown for ${unknown} of ${scope}`;
      this.headline = `${counts}. Most urgent: ${why}`;
    }
    if (r.unconvergedSince > 0) {
      this.since = duration(this.readAt - r.unconvergedSince);
    }
    if (summary) {
      this.segments = STATUS_ORDER.filter(
        (s) => summary.statusCounts[s] > 0,
      ).map((status) => ({
        status,
        label: STATUS_LABELS[status],
        count: summary.statusCounts[status],
      }));
      this.barLabel =
        'Partitions by status: ' +
        this.segments.map((s) => `${s.label} ${s.count}`).join(', ');
      // every partition carries one reason; the controller picks the most
      // urgent one of the worst status as primary, which need not be the largest
      this.reasons = Object.entries(summary.reasonCounts || {})
        .map(([reason, count]) => ({
          primary: reason === r.primaryReason,
          label: reasonLabel(reason),
          text: REASON_TEXT[reason] || '',
          count,
        }))
        .sort((a, b) => +b.primary - +a.primary || b.count - a.count);
    }
    if (r.status === 'PAUSED') return;
    const value = (v?: string) =>
      (v || 'UNKNOWN').toLowerCase().replace(/_/g, ' ');
    this.facts = [
      {
        label: `Target ${value(r.targetFreshness)}`,
        warn: r.targetFreshness !== 'CURRENT',
      },
      {
        label: `Optimizer ${value(r.optimizerStatus)}`,
        warn: r.optimizerStatus === 'BASELINE_FAILED',
      },
    ];
  }
}
