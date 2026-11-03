import { InstanceCapacity } from '../capacity/shared/capacity-snapshot.model';

export interface KeyUsage {
  // highest used / max of one instance, null when no instance has both known
  worst: number | null;
  // totals over the instances counted in worst
  used: number;
  max: number;
  // instances whose used or max is unknown, never counted as zero
  unknown: number;
}

export interface TopologyStats {
  instances: number;
  live: number;
  enabled: number;
  disabled: number;
  evacuating: number;
  errorReplicas: number;
  usage: Record<string, KeyUsage>;
}

export interface TopologyNode {
  id: string;
  // domain key of a group, such as 'zone'; null for an instance and the no domain group
  key: string | null;
  label: string;
  instance: InstanceCapacity | null;
  children: TopologyNode[];
  stats: TopologyStats;
}

export interface TopologyRow {
  node: TopologyNode;
  depth: number;
  open: boolean;
}

export const NO_DOMAIN_ID = 'none';

const names = new Intl.Collator(undefined, { numeric: true });

// Same rules as InstanceConfig#getDomainAsMap, but keeps the written order.
// Null when the domain is unset or malformed.
export function parseDomain(
  domain: string | null | undefined,
): [string, string][] | null {
  if (!domain || !domain.trim()) {
    return null;
  }
  const pairs = domain
    .trim()
    .split(',')
    .map((pair) => pair.split('=').map((part) => part.trim()));
  return pairs.every((pair) => pair.length === 2 && pair[0] && pair[1])
    ? (pairs as [string, string][])
    : null;
}

export function average(usage: KeyUsage): number | null {
  return usage.max > 0 ? usage.used / usage.max : null;
}

// Groups instances by their domain pairs, outermost first. A trailing pair
// whose value is the instance name only names the instance, so it is no group.
export function buildTopology(
  instances: InstanceCapacity[],
  domains: Map<string, string | undefined>,
  keys: string[],
): TopologyNode[] {
  const root = node('', null, '', null, keys);
  const groups = new Map<string, TopologyNode>();
  const child = (
    parent: TopologyNode,
    id: string,
    key: string | null,
    label: string,
  ) => {
    let group = groups.get(id);
    if (!group) {
      group = node(id, key, label, null, keys);
      groups.set(id, group);
      parent.children.push(group);
    }
    return group;
  };

  for (const instance of instances) {
    const pairs = parseDomain(domains.get(instance.name));
    let parent = pairs ? root : child(root, NO_DOMAIN_ID, null, 'no domain');
    if (pairs && pairs[pairs.length - 1][1] === instance.name) {
      pairs.pop();
    }
    for (const [key, value] of pairs || []) {
      parent = child(parent, `${parent.id}/${key}=${value}`, key, value);
    }
    parent.children.push(
      node(`instance:${instance.name}`, null, instance.name, instance, keys),
    );
  }
  return finish(root.children);
}

export function groupIds(nodes: TopologyNode[]): string[] {
  return nodes
    .filter((n) => !n.instance)
    .reduce<string[]>((ids, n) => ids.concat(n.id, groupIds(n.children)), []);
}

// Rows to render, depth first. A search keeps every match with its ancestors
// and opens them all.
export function visibleRows(
  nodes: TopologyNode[],
  expanded: Set<string>,
  query: string,
): TopologyRow[] {
  const q = query.trim().toLowerCase();
  const rows: TopologyRow[] = [];
  const walk = (list: TopologyNode[], depth: number) => {
    for (const n of list) {
      const open = !n.instance && (!!q || expanded.has(n.id));
      rows.push({ node: n, depth, open });
      if (open) {
        walk(n.children, depth + 1);
      }
    }
  };
  walk(q ? search(nodes, q) : nodes, 0);
  return rows;
}

function search(nodes: TopologyNode[], q: string): TopologyNode[] {
  const found: TopologyNode[] = [];
  for (const n of nodes) {
    const text = n.key ? `${n.key}=${n.label}` : n.label;
    if (text.toLowerCase().includes(q)) {
      found.push(n);
    } else {
      const children = search(n.children, q);
      if (children.length) {
        found.push({ ...n, children });
      }
    }
  }
  return found;
}

function node(
  id: string,
  key: string | null,
  label: string,
  instance: InstanceCapacity | null,
  keys: string[],
): TopologyNode {
  const stats: TopologyStats = {
    instances: 0,
    live: 0,
    enabled: 0,
    disabled: 0,
    evacuating: 0,
    errorReplicas: 0,
    usage: {},
  };
  for (const k of keys) {
    const used = instance?.used?.[k];
    const max = instance?.max?.[k];
    stats.usage[k] =
      used == null || !(max > 0)
        ? { worst: null, used: 0, max: 0, unknown: instance ? 1 : 0 }
        : { worst: used / max, used, max, unknown: 0 };
  }
  if (instance) {
    stats.instances = 1;
    stats.live = instance.live ? 1 : 0;
    stats.enabled = instance.operation === 'ENABLE' ? 1 : 0;
    stats.disabled = instance.operation === 'DISABLE' ? 1 : 0;
    stats.evacuating = instance.operation === 'EVACUATE' ? 1 : 0;
    stats.errorReplicas = instance.errorReplicas;
  }
  return { id, key, label, instance, children: [], stats };
}

// Rolls child stats up into each group and sorts groups first, then
// instances, with the no domain group last.
function finish(nodes: TopologyNode[]): TopologyNode[] {
  for (const group of nodes.filter((n) => !n.instance)) {
    const total = group.stats;
    for (const { stats } of finish(group.children)) {
      total.instances += stats.instances;
      total.live += stats.live;
      total.enabled += stats.enabled;
      total.disabled += stats.disabled;
      total.evacuating += stats.evacuating;
      total.errorReplicas += stats.errorReplicas;
      for (const k of Object.keys(total.usage)) {
        const sum = total.usage[k];
        const part = stats.usage[k];
        if (part.worst !== null) {
          sum.worst = Math.max(sum.worst ?? part.worst, part.worst);
        }
        sum.used += part.used;
        sum.max += part.max;
        sum.unknown += part.unknown;
      }
    }
  }
  const rank = (n: TopologyNode) =>
    n.id === NO_DOMAIN_ID ? 2 : n.instance ? 1 : 0;
  return nodes.sort(
    (a, b) => rank(a) - rank(b) || names.compare(a.label, b.label),
  );
}
