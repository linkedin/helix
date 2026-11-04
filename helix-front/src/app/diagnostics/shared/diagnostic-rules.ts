import {
  CapacitySnapshot,
  InstanceCapacity,
} from '../../capacity/shared/capacity-snapshot.model';
import { Instance } from '../../instance/shared/instance.model';
import {
  formatUtc,
  operationRows,
} from '../../instance/shared/instance-operations';
import { ResourceSummary } from '../../resource/shared/resource.model';

export type Severity = 'critical' | 'warning' | 'info';
export type Category =
  | 'cluster'
  | 'errors'
  | 'resource'
  | 'instance'
  | 'capacity';

export interface DiagnosticIssue {
  severity: Severity;
  category: Category;
  subject: string;
  title: string;
  detail: string;
  // route below the cluster page, e.g. ['instances', 'h1']
  link: string[];
  linkLabel: string;
}

export const SEVERITIES: Severity[] = ['critical', 'warning', 'info'];

export const CATEGORIES: { value: Category; label: string }[] = [
  { value: 'cluster', label: 'Cluster' },
  { value: 'errors', label: 'ERROR replicas' },
  { value: 'resource', label: 'Resources' },
  { value: 'instance', label: 'Instances' },
  { value: 'capacity', label: 'Capacity' },
];

const OPERATION_TITLES: Record<string, string> = {
  DISABLE: 'Instance disabled',
  EVACUATE: 'Instance evacuating',
  SWAP_IN: 'Instance swapping in',
  UNKNOWN: 'Instance operation UNKNOWN',
};

// Issues come from published Helix state only (capacity snapshot, instance
// and resource summaries), never from application health. A null snapshot
// (the read failed) skips the cluster, ERROR and capacity rules.
export function diagnosticIssues(
  snapshot: CapacitySnapshot | null,
  instances: Instance[],
  resources: ResourceSummary[],
): DiagnosticIssue[] {
  const rows = snapshot?.instances || [];
  const issues = [
    ...(snapshot ? clusterIssues(snapshot) : []),
    ...resources.map(resourceIssue),
    ...instances.map(instanceIssue),
    ...rows.map(errorIssue),
    ...rows.map((row) => capacityIssue(row, snapshot)),
  ].filter((issue): issue is DiagnosticIssue => !!issue);
  const rank = (issue: DiagnosticIssue) =>
    SEVERITIES.indexOf(issue.severity) * 10 +
    CATEGORIES.findIndex((c) => c.value === issue.category);
  return issues.sort(
    (a, b) =>
      rank(a) - rank(b) ||
      a.subject.localeCompare(b.subject, undefined, { numeric: true }),
  );
}

function make(
  category: Category,
  subject: string,
  link: string[],
  linkLabel: string,
) {
  return (
    severity: Severity,
    title: string,
    detail: string,
  ): DiagnosticIssue => ({
    severity,
    category,
    subject,
    title,
    detail,
    link,
    linkLabel,
  });
}

function clusterIssues(snapshot: CapacitySnapshot): DiagnosticIssue[] {
  const issue = make('cluster', snapshot.cluster, ['controller'], 'Controller');
  const issues: DiagnosticIssue[] = [];
  if (!snapshot.controllerName) {
    issues.push(
      issue(
        'critical',
        'No controller leader',
        'No controller holds leadership, so nothing is rebalanced or recovered.',
      ),
    );
  }
  if (snapshot.paused) {
    issues.push(
      issue(
        'warning',
        'Cluster paused',
        'The controller is paused and does not change any assignment.',
      ),
    );
  }
  if (snapshot.maintenance) {
    issues.push(
      issue(
        'warning',
        'Cluster in maintenance mode',
        snapshot.maintenanceReason
          ? `Reason: ${snapshot.maintenanceReason}`
          : 'No reason recorded.',
      ),
    );
  }
  return issues;
}

function resourceIssue(resource: ResourceSummary): DiagnosticIssue | null {
  const issue = make(
    'resource',
    resource.name,
    ['resources', resource.name],
    'Resource',
  );
  if (!resource.enabled) {
    return issue(
      'info',
      'Resource disabled',
      'Disabled in its ideal state, so its replicas are taken offline.',
    );
  }
  if (!resource.hasExternalView) {
    return issue(
      'critical',
      'No external view',
      `Enabled ${resource.waged ? 'WAGED' : resource.rebalanceMode} resource with no published external view, so no replica is confirmed serving.`,
    );
  }
  return null;
}

function instanceIssue(instance: Instance): DiagnosticIssue | null {
  const operation = instance.operationState;
  const enabled = operation === 'ENABLE';
  if (instance.online && enabled) {
    return null;
  }
  const issue = make(
    'instance',
    instance.name,
    ['instances', instance.name],
    'Instance',
  );
  if (instance.online) {
    return issue(
      'warning',
      OPERATION_TITLES[operation] || `Instance operation ${operation}`,
      operationDetail(instance),
    );
  }
  return enabled
    ? issue(
        'critical',
        'Instance not live',
        'Enabled, but has no live ZooKeeper session.',
      )
    : issue(
        'warning',
        'Instance not live',
        `No live ZooKeeper session. Operation ${operation}. ${operationDetail(instance)}`,
      );
}

function operationDetail(instance: Instance): string {
  // same rule as the Instance list, so a legacy disable is not shown with the
  // source of an older stored operation
  const active = operationRows(instance.summary, instance.operationState).find(
    (row) => row.active,
  );
  if (!active) {
    return 'No source or reason recorded.';
  }
  if (active.entry.source === 'LEGACY') {
    return 'No stored operation matches, so this is the legacy HELIX_ENABLED flag. No source or reason recorded.';
  }
  const since =
    active.atMillis === null
      ? ', time not recorded'
      : ` since ${formatUtc(active.atMillis)}${active.fromConfigCreation ? ' (instance config created)' : ''}`;
  return `Set by ${active.entry.source || 'an unknown source'}${since}${active.entry.reason ? ': ' + active.entry.reason : '.'}`;
}

function errorIssue(row: InstanceCapacity): DiagnosticIssue | null {
  if (!row.errorReplicas) {
    return null;
  }
  const byResource = row.resources
    .filter((r) => r.errorReplicas > 0)
    .map((r) => `${r.resourceName} (${r.errorReplicas})`);
  return make(
    'errors',
    row.name,
    ['instances', row.name, 'diagnostics'],
    'Errors',
  )(
    'critical',
    `${row.errorReplicas} ERROR replica${row.errorReplicas === 1 ? '' : 's'}`,
    byResource.length
      ? `In the external view of ${byResource.join(', ')}.`
      : 'In the published external view.',
  );
}

function capacityIssue(
  row: InstanceCapacity,
  snapshot: CapacitySnapshot,
): DiagnosticIssue | null {
  const issue = make('capacity', row.name, ['capacity'], 'Capacity');
  if (row.capacityStatus === 'UNKNOWN') {
    return issue(
      'info',
      'Capacity unknown',
      !row.registered
        ? 'Hosts replicas in an external view but has no instance config.'
        : !row.max
          ? `Capacity is not fully configured for ${snapshot.capacityKeys.join(', ')}.`
          : 'A hosted WAGED partition weight cannot be resolved, so usage is unknown.',
    );
  }
  if (!row.max || !row.used) {
    return null;
  }
  const over = snapshot.capacityKeys
    .filter((key) => row.used[key] > row.max[key])
    .map((key) => {
      const [used, max] = [row.used[key], row.max[key]];
      return `${key} ${used} / ${max}${max > 0 ? ` (${Math.round((used / max) * 100)}%)` : ''}`;
    });
  return over.length
    ? issue(
        'warning',
        'Over WAGED capacity',
        `${over.join(', ')}. Partition weights, not CPU or disk usage.`,
      )
    : null;
}
