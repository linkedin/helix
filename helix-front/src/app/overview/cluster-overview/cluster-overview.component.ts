import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, Observable, of, Subscription } from 'rxjs';
import { catchError } from 'rxjs/operators';
import * as _ from 'lodash';

import { CapacityService } from '../../capacity/shared/capacity.service';
import { CapacitySnapshot } from '../../capacity/shared/capacity-snapshot.model';
import { Instance } from '../../instance/shared/instance.model';
import { InstanceService } from '../../instance/shared/instance.service';
import { ResourceSummary } from '../../resource/shared/resource.model';
import { ResourceService } from '../../resource/shared/resource.service';
import {
  DiagnosticIssue,
  diagnosticIssues,
} from '../../diagnostics/shared/diagnostic-rules';

type Source = 'snapshot' | 'instances' | 'resources';

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
      this.resources = resources;
      this.issues = diagnosticIssues(
        snapshot,
        instances || [],
        resources || [],
      );
      this.updated = snapshot
        ? Date.parse(snapshot.generatedAt) || Date.now()
        : null;
      this.listsReadAt = Date.now();
      this.setVerdict(failed);
      this.cards = this.buildCards();
      this.isLoading = false;
    });
  }

  link(issue: DiagnosticIssue): string[] {
    return ['../', ...issue.link];
  }

  private setVerdict(failed: string[]) {
    const count = (severity: string) =>
      this.issues.filter((issue) => issue.severity === severity).length;
    const [critical, warnings] = [count('critical'), count('warning')];
    const found =
      [
        critical ? `${critical} critical` : '',
        warnings ? `${warnings} warning${warnings === 1 ? '' : 's'}` : '',
      ]
        .filter(Boolean)
        .join(', ') || 'nothing to act on';
    this.tone = critical
      ? 'error'
      : warnings || failed.length
        ? 'warning'
        : 'ok';
    this.verdict = failed.length
      ? `Incomplete: could not read ${failed.join(', ')}, so this is not the full picture. Among what was read: ${found}.`
      : critical || warnings
        ? `Needs attention: ${found}.`
        : 'Healthy: Helix reports nothing to act on.';
  }

  private buildCards(): OverviewCard[] {
    const { snapshot, instances, resources } = this;
    return [
      card(instances, 'instances', 'instances live', '../topology', (list) => {
        const down = list.filter((i) => !i.online).length;
        return {
          value: `${list.length - down} / ${list.length}`,
          detail: down
            ? `${down} with no live session.`
            : 'All have a live session.',
          tone: down ? 'warn' : undefined,
        };
      }),
      card(
        instances,
        'instances',
        'not enabled or under an operation',
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
                .map(([op, n]) => `${n} ${op}`)
                .join(', ') || 'All ENABLE.',
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
          value: list.length,
          detail: `${waged} WAGED, ${list.length - waged} not WAGED. ${disabled} disabled, ${noView} enabled with no external view.`,
          tone: noView ? 'bad' : undefined,
        };
      }),
      card(snapshot, 'snapshot', 'ERROR replicas', '../diagnostics', (s) => {
        const hosts = s.instances.filter((row) => row.errorReplicas).length;
        return {
          value: s.summary.totalErrorReplicas,
          detail: hosts
            ? `On ${hosts} instance${hosts === 1 ? '' : 's'}.`
            : 'None in the external view.',
          tone: s.summary.totalErrorReplicas ? 'bad' : undefined,
        };
      }),
      card(snapshot, 'snapshot', 'over WAGED capacity', '../capacity', (s) => ({
        value: s.summary.instancesOverCapacity,
        detail: `${s.summary.instancesWithUnknownCapacity} with unknown capacity. ${hottest(s)}`,
        tone: s.summary.instancesOverCapacity ? 'warn' : undefined,
      })),
      card(snapshot, 'snapshot', 'controller leader', '../controller', (s) => ({
        value: s.controllerName || 'no leader',
        detail: s.controllerName
          ? 'Holds leadership.'
          : 'Nothing is rebalanced or recovered.',
        tone: s.controllerName ? undefined : 'bad',
      })),
      card(snapshot, 'snapshot', 'cluster mode', '../controller', (s) => ({
        value:
          [s.paused && 'paused', s.maintenance && 'maintenance']
            .filter(Boolean)
            .join(', ') || 'normal',
        detail: s.maintenance
          ? `Reason: ${s.maintenanceReason || 'none recorded'}.`
          : s.paused
            ? 'Assignments are not changed.'
            : 'Not paused, not in maintenance.',
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
  fill: (data: T) => Pick<OverviewCard, 'value' | 'detail' | 'tone'>,
): OverviewCard {
  return {
    label,
    link,
    ...(data
      ? fill(data)
      : {
          value: 'unknown',
          detail: `Could not read ${SOURCE_NAMES[source]}.`,
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
  return hot
    ? `Hottest: ${hot.name} at ${Math.round(hot.ratio * 100)}% of ${hot.key}.`
    : 'No instance has a known WAGED usage.';
}
