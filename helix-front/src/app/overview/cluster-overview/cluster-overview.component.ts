import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, Observable, of, Subscription } from 'rxjs';
import { catchError } from 'rxjs/operators';
import * as _ from 'lodash';

import { CapacityService } from '../../capacity/shared/capacity.service';
import { CapacitySnapshot } from '../../capacity/shared/capacity-snapshot.model';
import { percent } from '../../capacity/heatmap/heatmap.model';
import { singular } from '../../shared/list-status/list-status.component';
import { Instance } from '../../instance/shared/instance.model';
import { InstanceService } from '../../instance/shared/instance.service';
import { ResourceSummary } from '../../resource/shared/resource.model';
import { ResourceService } from '../../resource/shared/resource.service';
import {
  ConvergenceReport,
  convergenceConcern,
} from '../../shared/convergence/convergence.model';
import { ConvergencePanelComponent } from '../../shared/convergence/convergence-panel.component';
import {
  DiagnosticIssue,
  diagnosticIssues,
  isTaskResource,
} from '../../diagnostics/shared/diagnostic-rules';

type Source = 'snapshot' | 'instances' | 'resources';

// instance operations as words, e.g. "2 evacuating"
const OPERATION_WORDS: Record<string, string> = {
  DISABLE: 'disabled',
  EVACUATE: 'evacuating',
  SWAP_IN: 'swapping in',
  UNKNOWN: 'unknown',
};

const SOURCE_NAMES: Record<Source, string> = {
  snapshot: 'the capacity snapshot',
  instances: 'the instance list',
  resources: 'the resource list',
};

export interface OverviewCard {
  label: string;
  value: string | number;
  detail: string;
  // sibling tab of this one, e.g. '../capacity'
  link: string;
  tone?: 'bad' | 'warn';
}

@Component({
  selector: 'hi-cluster-overview',
  templateUrl: './cluster-overview.component.html',
  styleUrls: ['./cluster-overview.component.scss'],
})
export class ClusterOverviewComponent implements OnInit, OnDestroy {
  // when each part of the page was read, behind the info icon by the sample time
  get readTimes(): string {
    const utc = (time: number) =>
      new Date(time).toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
    return [
      this.snapshot && this.updated
        ? `ERROR replicas, capacity, controller and cluster mode are from the sample taken at ${utc(this.updated)}, which may be served from cache.`
        : '',
      (this.instances || this.resources) && this.listsReadAt
        ? `Instances and resources were read at ${utc(this.listsReadAt)}.`
        : '',
    ]
      .filter(Boolean)
      .join(' ');
  }

  // lets a host name wrap after its dots instead of mid-word
  breakable(value: unknown): string[] {
    return String(value).split(/(?<=[._])/);
  }

  // labels are plural nouns, so a count of one drops the trailing s
  cardLabel(card: OverviewCard): string {
    return card.value === 1 ? singular(card.label) : card.label;
  }

  clusterName: string;
  snapshot: CapacitySnapshot | null = null;
  instances: Instance[] | null = null;
  resources: ResourceSummary[] | null = null;
  // null until the first read completes
  issues: DiagnosticIssue[] | null = null;
  cards: OverviewCard[] = [];
  verdict = '';
  tone = '';
  isLoading = true;
  updated: number | null = null;
  listsReadAt: number | null = null;
  convergence: ConvergenceReport | null = null;
  convergenceReadFailed = false;
  private failed: string[] = [];

  private sub: Subscription;
  // one refresh for the whole tab, so the panel and the cards are read together
  @ViewChild(ConvergencePanelComponent)
  private panel?: ConvergencePanelComponent;

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
    this.panel?.load();
    this.sub?.unsubscribe();
    const failed: string[] = [];
    // a failed read becomes null so the other two still render
    const read = <T>(source: Source, request: Observable<T>) =>
      request.pipe(
        catchError((error) => {
          const reason =
            typeof error === 'string' ? error : error?.message || 'no response';
          failed.push(`${SOURCE_NAMES[source]} (${reason})`);
          return of(null as T);
        }),
      );
    this.sub = forkJoin([
      read('snapshot', this.capacityService.getSnapshot(this.clusterName)),
      read('instances', this.instanceService.getSummaries(this.clusterName)),
      read('resources', this.resourceService.getSummaries(this.clusterName)),
    ]).subscribe(([snapshot, instances, resources]) => {
      this.snapshot = snapshot;
      this.instances = instances;
      this.resources = resources?.filter((r) => !isTaskResource(r)) ?? null;
      this.issues = diagnosticIssues(
        snapshot,
        instances || [],
        resources || [],
      );
      this.updated = snapshot
        ? Date.parse(snapshot.generatedAt) || Date.now()
        : null;
      this.listsReadAt = Date.now();
      this.failed = failed;
      this.setVerdict(failed);
      this.cards = this.buildCards();
      this.isLoading = false;
    });
  }

  // the convergence panel reads on its own schedule; fold each read into the verdict
  onConvergence(report: ConvergenceReport | null) {
    this.convergence = report;
    this.convergenceReadFailed = false;
    if (this.issues) this.setVerdict(this.failed);
  }

  // keep the last report, so a blocked cluster stays blocked while a read fails
  onConvergenceReadFailed() {
    this.convergenceReadFailed = true;
    if (this.issues) this.setVerdict(this.failed);
  }

  // a count such as 7 or 3 / 4, as opposed to a word such as Normal
  isWord(value: string | number): boolean {
    return !/^[\d\s/]+$/.test(String(value));
  }

  link(issue: DiagnosticIssue): string[] {
    return ['../', ...issue.link];
  }

  private setVerdict(failed: string[]) {
    const count = (severity: string) =>
      this.issues.filter((issue) => issue.severity === severity).length;
    const [critical, warnings] = [count('critical'), count('warning')];
    const convergence =
      convergenceConcern(this.convergence) ??
      (this.convergenceReadFailed
        ? { tone: 'warning', text: 'convergence status could not be read' }
        : null);
    const found =
      [
        critical
          ? `${critical} critical issue${critical === 1 ? '' : 's'}`
          : '',
        warnings ? `${warnings} warning${warnings === 1 ? '' : 's'}` : '',
        convergence?.text ?? '',
      ]
        .filter(Boolean)
        .join(', ') || 'nothing to act on';
    // with convergence off we cannot tell whether replicas match the target, so do not say healthy
    const unmonitored = this.convergence?.reportState === 'MONITORING_DISABLED';
    this.tone =
      critical || convergence?.tone === 'error'
        ? 'error'
        : warnings || failed.length || convergence
          ? 'warning'
          : unmonitored
            ? ''
            : 'ok';
    this.verdict = failed.length
      ? `Incomplete: could not read ${failed.join(', ')}, so this is not the full picture. Among what was read: ${found}.`
      : critical || warnings || convergence
        ? `Needs attention: ${found}`
        : unmonitored
          ? 'Convergence is not monitored, so this does not show whether replicas match the target.'
          : 'Healthy: nothing to act on';
  }

  private buildCards(): OverviewCard[] {
    const { snapshot, instances, resources } = this;
    return [
      card(instances, 'instances', 'instances live', '../topology', (list) => {
        const down = list.filter((i) => !i.online).length;
        return {
          value: `${list.length - down} / ${list.length}`,
          detail: down
            ? `${down} without a live session`
            : 'All have a live session',
          tone: down ? 'warn' : undefined,
        };
      }),
      card(
        instances,
        'instances',
        'disabled or in an operation',
        '../instances',
        (list) => {
          const ops = _.countBy(
            list.filter((i) => i.operationState !== 'ENABLE'),
            'operationState',
          );
          const total = _.sum(Object.values(ops));
          return {
            value: total,
            detail:
              Object.entries(ops)
                .map(
                  ([op, n]) =>
                    `${n} ${OPERATION_WORDS[op] || op.toLowerCase().replace(/_/g, ' ')}`,
                )
                .join(', ') || 'All enabled',
            tone: total ? 'warn' : undefined,
          };
        },
      ),
      card(resources, 'resources', 'resources', '../resources', (list) => {
        const waged = list.filter((r) => r.waged).length;
        const disabled = list.filter((r) => !r.enabled).length;
        const noView = list.filter(
          (r) => r.enabled && !r.hasExternalView,
        ).length;
        return {
          label: list.length === 1 ? 'resource' : 'resources',
          value: list.length,
          // the split always shows; the two problems only when there are any
          detail: [
            `${waged} WAGED, ${list.length - waged} non-WAGED`,
            disabled ? `${disabled} disabled` : '',
            noView ? `${noView} missing an external view` : '',
          ]
            .filter(Boolean)
            .join(', '),
          tone: noView ? 'bad' : undefined,
        };
      }),
      card(snapshot, 'snapshot', 'ERROR replicas', '../diagnostics', (s) => {
        const hosts = s.instances.filter((row) => row.errorReplicas).length;
        return {
          value: s.summary.totalErrorReplicas,
          detail: hosts
            ? `On ${hosts} instance${hosts === 1 ? '' : 's'}`
            : 'None in the external view',
          tone: s.summary.totalErrorReplicas ? 'bad' : undefined,
        };
      }),
      card(snapshot, 'snapshot', 'over WAGED capacity', '../capacity', (s) =>
        s.capacityKeys.length
          ? {
              value: s.summary.instancesOverCapacity,
              detail: [
                hottest(s),
                s.summary.instancesWithUnknownCapacity
                  ? `${s.summary.instancesWithUnknownCapacity} with unknown capacity`
                  : '',
              ]
                .filter(Boolean)
                .join(', '),
              tone: s.summary.instancesOverCapacity ? 'warn' : undefined,
            }
          : { value: 'n/a', detail: 'No WAGED capacity keys defined' },
      ),
      card(snapshot, 'snapshot', 'controller leader', '../controller', (s) => ({
        value: s.controllerName || 'No leader',
        detail: s.controllerName
          ? 'Holds leadership'
          : 'Nothing is rebalanced or recovered',
        tone: s.controllerName ? undefined : 'bad',
      })),
      card(snapshot, 'snapshot', 'cluster mode', '../controller', (s) => ({
        value:
          [s.paused && 'Paused', s.maintenance && 'Maintenance']
            .filter(Boolean)
            .join(', ') || 'Normal',
        detail: s.maintenance
          ? `Reason: ${s.maintenanceReason || 'none recorded'}`
          : s.paused
            ? 'Assignments are not changed'
            : 'Not paused or in maintenance',
        tone: s.paused || s.maintenance ? 'warn' : undefined,
      })),
    ];
  }
}

// a card whose source could not be read says unknown, never 0
function card<T>(
  data: T | null,
  source: Source,
  label: string,
  link: string,
  fill: (
    data: T,
  ) => Pick<OverviewCard, 'value' | 'detail' | 'tone'> &
    Partial<Pick<OverviewCard, 'label'>>,
): OverviewCard {
  return {
    label,
    link,
    ...(data
      ? fill(data)
      : {
          value: 'Unknown',
          detail: `Could not read ${SOURCE_NAMES[source]}`,
        }),
  };
}

function hottest(snapshot: CapacitySnapshot): string {
  const hot = _.maxBy(
    _.flatMap(
      snapshot.instances.filter((row) => row.capacityStatus === 'KNOWN'),
      (row) =>
        snapshot.capacityKeys
          .filter((key) => row.max?.[key] > 0 && row.used?.[key] != null)
          .map((key) => ({
            name: row.name,
            key,
            ratio: row.used[key] / row.max[key],
          })),
    ),
    'ratio',
  );
  if (!hot) {
    return 'No instances report WAGED usage';
  }
  return hot.ratio > 0
    ? `Hottest: ${hot.name} at ${percent(hot.ratio)} of ${hot.key}`
    : 'No WAGED usage on any instance';
}
