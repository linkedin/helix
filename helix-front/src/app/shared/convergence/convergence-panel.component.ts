import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnDestroy,
  Output,
} from '@angular/core';
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
// counts read as 12,802 everywhere in the panel, headline and legend alike
const num = (n: number) => n.toLocaleString('en-US');

const SETTLED_STATES = ['MONITORING_DISABLED', 'SCHEMA_UNSUPPORTED'];

interface Segment {
  status: ConvergenceStatusName;
  label: string;
  count: number;
}

interface ResourceRow {
  name: string;
  status: ConvergenceStatusName;
  text: string;
}

const TARGET_TEXT: Record<string, string> = {
  CURRENT: 'up to date',
  LAST_KNOWN_GOOD: 'last known good',
};

const REBALANCER_TEXT: Record<string, string> = {
  HEALTHY: 'healthy',
  BASELINE_FAILED: 'baseline failed',
};

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
  // every read, so a page can fold the status into its own verdict
  @Output() reportChange = new EventEmitter<ConvergenceReport | null>();
  // a read that failed and will be retried; the last report emitted is still the best known
  @Output() readFailed = new EventEmitter<void>();

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
  resourceRows: ResourceRow[] = [];
  moreResources = 0;

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
    if (retry) {
      this.readFailed.emit();
    } else {
      this.reportChange.emit(report);
    }
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
    this.resourceRows = [];
    this.moreResources = 0;
    this.since = '';
    this.confirmed = '';
    if (!r) {
      this.tone = 'warning';
      this.statusLabel = '';
      this.headline = this.error;
      return;
    }
    this.statusLabel = STATUS_LABELS[r.status] || r.status;
    // Off by default, so on most clusters this is the normal state, not a problem.
    if (r.reportState === 'MONITORING_DISABLED') {
      this.tone = 'neutral';
      this.statusLabel = 'Off';
      this.headline = REPORT_STATE_TEXT[r.reportState];
      return;
    }
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
      `${num(total)} partition${total === 1 ? '' : 's'}` +
      (r.resourceCount != null
        ? ` across ${num(r.resourceCount)} resource${r.resourceCount === 1 ? '' : 's'}`
        : '');
    const why = REASON_TEXT[r.primaryReason] || reasonLabel(r.primaryReason);
    if (r.status === 'CONVERGED') {
      this.headline = `All ${scope} match the target assignment.`;
    } else if (r.status === 'PAUSED') {
      this.headline = why;
    } else {
      const unknown = summary?.statusCounts?.UNKNOWN ?? 0;
      const stuck = (summary?.notConverged ?? 0) - unknown;
      const counts = stuck
        ? `${num(stuck)} of ${scope} are not converged${unknown ? `, ${num(unknown)} unknown` : ''}`
        : `${num(unknown)} of ${scope} have an unknown status`;
      this.headline = `${counts}.`;
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
        this.segments.map((s) => `${s.label} ${num(s.count)}`).join(', ');
      // every partition carries one reason; the controller picks the most
      // urgent one of the worst status as primary, which need not be the largest
      // NONE is the converged partitions, which the bar already counts
      this.reasons = Object.entries(summary.reasonCounts || {})
        .filter(([reason]) => reason !== 'NONE')
        .map(([reason, count]) => ({
          primary: reason === r.primaryReason,
          label: reasonLabel(reason),
          text: REASON_TEXT[reason] || '',
          count,
        }))
        .sort((a, b) => +b.primary - +a.primary || b.count - a.count);
    }
    this.resourceRows = (r.listedResources || []).map((row) => ({
      name: row.resourceName,
      status: row.status,
      text: [
        STATUS_LABELS[row.status] || row.status,
        reasonLabel(row.primaryReason),
        `${num(row.notConvergedPartitions)} partition${row.notConvergedPartitions === 1 ? '' : 's'}`,
      ].join(' · '),
    }));
    this.moreResources =
      (r.listedResourceCount ?? 0) -
      this.resourceRows.length +
      (r.unlistedResourceCount ?? 0);
    if (r.status === 'PAUSED') return;
    this.facts = [
      {
        label: `Target: ${TARGET_TEXT[r.targetFreshness] || 'unknown'}`,
        warn: r.targetFreshness !== 'CURRENT',
      },
    ];
    // the rebalancer fact is about WAGED, so it is left out where WAGED is not used
    if (r.optimizerStatus !== 'NOT_APPLICABLE') {
      this.facts.push({
        label: `Rebalancer: ${REBALANCER_TEXT[r.optimizerStatus] || 'unknown'}`,
        warn: r.optimizerStatus === 'BASELINE_FAILED',
      });
    }
  }
}
