import { Instance } from '../../instance/shared/instance.model';
import { ZNRecord } from '../../shared/models/znrecord.model';
import { IReplica, Resource } from './resource.model';

// worst first
const STATUS_ORDER = [
  'UNSERVED',
  'ERROR',
  'DEGRADED',
  'UNKNOWN',
  'DISABLED',
  'HEALTHY',
] as const;
export type PartitionStatus = (typeof STATUS_ORDER)[number];

export const PARTITION_FILTERS = [
  { value: 'all', label: 'All' },
  { value: 'attention', label: 'Needs attention' },
  { value: 'unserved', label: 'No live serving replica' },
  { value: 'missing-top', label: 'Missing top state' },
  { value: 'below-target', label: 'Below replica target' },
  { value: 'errors', label: 'ERROR replicas' },
  { value: 'assignment', label: 'Assignment differs' },
] as const;
export type PartitionFilter = (typeof PARTITION_FILTERS)[number]['value'];
export type PartitionIssue = Exclude<PartitionFilter, 'all' | 'attention'>;

export interface ReplicaRow extends IReplica {
  // null when the instance status is not known
  live: boolean | null;
  operation: string | null;
  // null when it depends on unknown liveness or an unknown state model
  serving: boolean | null;
}

export interface PartitionRow {
  name: string;
  status: PartitionStatus;
  explanation: string;
  issues: PartitionIssue[];
  // null when the IdealState holds no target state for this partition
  assignmentDiffers: boolean | null;
  replicas: ReplicaRow[];
}

export function diagnosePartitions(
  resource: Resource,
  model: ZNRecord | null,
  instances: Instance[] | null,
): PartitionRow[] {
  const is = resource.idealState || {};
  const ev = resource.externalView || {};
  const byName = new Map((instances || []).map((node) => [node.name, node]));
  const priority = model?.listFields?.STATE_PRIORITY_LIST || [];
  const initial = model?.simpleFields?.INITIAL_STATE;
  const bound = (state: string) => model?.mapFields?.[`${state}.meta`]?.count;
  const serves = (state: string) =>
    priority.includes(state) &&
    ![initial, 'DROPPED', 'ERROR'].includes(state) &&
    !(Number(bound(state)) <= 0);
  const judged =
    resource.stateModel !== 'Task' && !!initial && priority.some(serves);
  const top = /^[1-9]\d*$/.test(bound(priority[0]) || '') ? priority[0] : null;
  const expected = isFinite(resource.replicaCount)
    ? resource.replicaCount
    : null;
  // FULL_AUTO (including WAGED) keeps no per-partition target in the IdealState
  const assigned = resource.rebalanceMode !== 'FULL_AUTO';
  const rank = (state: string) =>
    priority.includes(state) ? priority.indexOf(state) : priority.length;
  const count = (n: number) =>
    `${n}${expected === null ? '' : ` of ${expected}`} replica${
      (expected ?? n) === 1 ? '' : 's'
    } serving`;

  const diagnose = (name: string): PartitionRow => {
    const evMap = ev.mapFields?.[name] || {};
    const isMap = is.mapFields?.[name] || {};
    const hosts = new Set(Object.keys(evMap));
    if (assigned) {
      [...(is.listFields?.[name] || []), ...Object.keys(isMap)].forEach(
        (host) => hosts.add(host),
      );
    }
    hosts.delete('ANY_LIVEINSTANCE');
    const replicas = [...hosts]
      .map((instanceName): ReplicaRow => {
        const node = byName.get(instanceName);
        const live = node ? node.online : null;
        const state = evMap[instanceName];
        const operation = node?.operationState;
        return {
          instanceName,
          externalView: state,
          idealState: isMap[instanceName],
          live,
          operation: operation && operation !== 'ENABLE' ? operation : null,
          serving: !judged ? null : serves(state) ? live : false,
        };
      })
      .sort(
        (a, b) =>
          rank(a.externalView) - rank(b.externalView) ||
          a.instanceName.localeCompare(b.instanceName),
      );

    // a replica of unknown liveness may be serving, so it never proves a gap
    const maybe = replicas.filter((r) => r.serving !== false);
    const sure = replicas.filter((r) => r.serving === true).length;
    const errors = replicas.filter((r) => r.externalView === 'ERROR').length;
    // only a CUSTOMIZED map is the target, in SEMI_AUTO the controller picks states
    const differs =
      resource.rebalanceMode === 'CUSTOMIZED' && Object.keys(isMap).length
        ? replicas.some(
            (r) => (r.externalView ?? initial) !== (r.idealState ?? initial),
          )
        : null;
    const issues: PartitionIssue[] = [];
    if (errors) issues.push('errors');
    if (resource.enabled && judged) {
      if (!maybe.length) issues.push('unserved');
      if (top && !maybe.some((r) => r.externalView === top))
        issues.push('missing-top');
      if (expected !== null && maybe.length < expected)
        issues.push('below-target');
    }
    if (resource.enabled && differs) issues.push('assignment');
    const unknownLive = judged && sure !== maybe.length;

    const verdict = (): [PartitionStatus, string] => {
      if (issues.includes('unserved'))
        return ['UNSERVED', 'No live replica is in a serving state'];
      if (errors)
        return ['ERROR', `${errors} replica${errors > 1 ? 's' : ''} in ERROR`];
      if (!resource.enabled) return ['DISABLED', 'The resource is disabled'];
      if (!judged)
        return ['UNKNOWN', `Cannot judge serving for ${resource.stateModel}`];
      if (issues.includes('missing-top'))
        return ['DEGRADED', `${top} is missing`];
      if (issues.includes('below-target'))
        return [
          'DEGRADED',
          (unknownLive ? 'At most ' : '') + count(maybe.length),
        ];
      if (differs) return ['DEGRADED', 'State differs from the IdealState map'];
      if (unknownLive)
        return ['UNKNOWN', 'Cannot judge liveness, instance status is unknown'];
      return ['HEALTHY', count(sure)];
    };
    const [status, explanation] = verdict();
    return {
      name,
      status,
      explanation,
      issues,
      assignmentDiffers: differs,
      replicas,
    };
  };

  const names = new Set([
    ...Object.keys(is.listFields || {}),
    ...Object.keys(is.mapFields || {}),
    ...Object.keys(ev.mapFields || {}),
  ]);
  return [...names]
    .map(diagnose)
    .sort(
      (a, b) =>
        STATUS_ORDER.indexOf(a.status) - STATUS_ORDER.indexOf(b.status) ||
        a.name.localeCompare(b.name, undefined, { numeric: true }),
    );
}

export function matchesFilter(
  row: PartitionRow,
  filter: PartitionFilter,
): boolean {
  return (
    filter === 'all' ||
    (filter === 'attention'
      ? row.issues.length > 0
      : row.issues.includes(filter))
  );
}
