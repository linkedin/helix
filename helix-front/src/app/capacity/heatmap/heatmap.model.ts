import {
  capacityRatio,
  CapacitySnapshot,
  InstanceCapacity,
  InstanceResourceUsage,
} from '../shared/capacity-snapshot.model';

// Coolest to hottest. Unknown has its own grey band and never counts as low.
// Each band's swatch is a step from the middle of its range.
export const BANDS = [
  { id: 'unknown', label: 'Unknown', shade: 'unknown' },
  { id: 'low', label: 'Under 50%', shade: 'l5' },
  { id: 'mid', label: '50 to 75%', shade: 'l12' },
  { id: 'high', label: '75 to 90%', shade: 'l16' },
  { id: 'hot', label: '90 to 100%', shade: 'l19' },
  { id: 'over', label: 'Over 100%', shade: 'over' },
];

export type ColorMode = 'load' | 'median';

export interface HeatTile {
  instance: InstanceCapacity;
  ratio: number | null;
  band: string;
  // colour step: l0 to l20 in 5% steps, over or unknown; m5 to p5 in 10%
  // steps around the median
  shade: string;
  // any operation other than ENABLE
  operation: string | null;
  text: string;
  label: string;
  short: string;
}

export interface HeatZone {
  name: string;
  label: string;
  tiles: HeatTile[];
  mean: number | null;
  max: number | null;
  shade: string;
}

export interface Heatmap {
  zones: HeatZone[];
  // what Distance from median compares with: the median, or the mean when
  // the median is 0, as it is when most instances carry nothing
  reference: number | null;
  basis: 'median' | 'mean';
  counts: Record<string, number>;
  hottest: HeatTile[];
  // cells per zone row, the same for every zone so zones compare by shape
  columns: number;
}

const names = new Intl.Collator(undefined, { numeric: true });
const HOTTEST = 10;

export function bandOf(ratio: number | null): string {
  if (ratio === null) {
    return 'unknown';
  }
  const steps = [0.5, 0.75, 0.9].filter((step) => ratio >= step).length;
  return ratio > 1 ? 'over' : ['low', 'mid', 'high', 'hot'][steps];
}

// a small but nonzero load reads as <1%, so it is not mistaken for idle
export function percent(ratio: number | null): string {
  if (ratio === null) {
    return 'unknown';
  }
  return ratio > 0 && ratio < 0.005 ? '<1%' : `${Math.round(ratio * 100)}%`;
}

export function usageText(
  instance: InstanceCapacity,
  key: string,
  topStateOnly: boolean,
): string {
  const ratio = capacityRatio(instance, key, topStateOnly);
  const used = (topStateOnly ? instance.topStateUsed : instance.used)?.[key];
  return ratio === null
    ? 'unknown'
    : `${used} / ${instance.max[key]} (${percent(ratio)})`;
}

// In median mode the reference is what each ratio is compared with; when it
// is 0 anything above it is as far above as the scale goes.
export function shadeOf(
  ratio: number | null,
  mode: ColorMode,
  reference: number | null,
): string {
  if (ratio === null || (mode === 'median' && reference === null)) {
    return 'unknown';
  }
  if (mode === 'load') {
    return ratio > 1 ? 'over' : `l${Math.floor(ratio * 20)}`;
  }
  if (!reference) {
    return ratio > 0 ? 'p5' : 'p0';
  }
  const step = Math.round((ratio / reference - 1) * 10);
  const clamped = Math.max(-5, Math.min(5, step));
  return clamped < 0 ? `m${-clamped}` : `p${clamped}`;
}

// Drops the prefix and suffix every name shares, cut where letters meet digits
// or punctuation, so host-1234.example.com_11932 becomes 1234. Names short
// enough to read whole are kept.
export function shortNames(list: string[]): (name: string) => string {
  if (list.length < 2 || list.every((name) => name.length <= 16)) {
    return (name) => name;
  }
  const kind = (c = '') => (/\d/.test(c) ? 'd' : /[a-z]/i.test(c) ? 'a' : c);
  const [first] = list;
  let pre = first.length;
  let suf = first.length;
  for (const name of list) {
    let i = 0;
    while (i < pre && name[i] === first[i]) i++;
    pre = i;
    let j = 0;
    while (j < suf && name[name.length - 1 - j] === first[first.length - 1 - j])
      j++;
    suf = j;
  }
  // the cut must fall on a boundary in every name, not just the first
  const shortest = Math.min(...list.map((name) => name.length));
  suf = Math.min(suf, shortest - pre);
  const splits = (at: (name: string) => number) =>
    list.some((name) => {
      const i = at(name);
      return kind(name[i - 1]) === kind(name[i]);
    });
  while (pre > 0 && splits(() => pre)) pre--;
  while (suf > 0 && splits((name) => name.length - suf)) suf--;
  return (name) => name.slice(pre, name.length - suf) || name;
}

function median(values: number[]): number | null {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return !sorted.length
    ? null
    : sorted.length % 2
      ? sorted[mid]
      : (sorted[mid - 1] + sorted[mid]) / 2;
}

// Zones by name; inside a zone hottest first, unknown last, then by name.
// Without zones there is one block of every instance, hottest first.
export function buildHeatmap(
  snapshot: CapacitySnapshot | null,
  key: string,
  topStateOnly: boolean,
  mode: ColorMode = 'load',
  byZone = true,
): Heatmap {
  const instances = snapshot?.instances || [];
  const measure = topStateOnly ? `${key} top state` : key;
  const short = shortNames(instances.map((i) => i.name));
  const ratios = instances.map((i) => capacityRatio(i, key, topStateOnly));
  const known = ratios.filter((r) => r !== null);
  const mid = median(known);
  const basis = mid === 0 && known.some((r) => r > 0) ? 'mean' : 'median';
  const reference =
    basis === 'mean' ? known.reduce((a, b) => a + b, 0) / known.length : mid;
  const zones = new Map<string, HeatZone>();
  const counts: Record<string, number> = {};
  const tiles: HeatTile[] = [];
  instances.forEach((instance, n) => {
    const own = instance.zone || 'No fault zone';
    // ungrouped, every instance lands in one block and is ranked against all
    const name = byZone ? instance.zone || '' : '*';
    const zone: HeatZone = zones.get(name) || {
      name,
      label: byZone ? own : 'All instances',
      tiles: [],
      mean: null,
      max: null,
      shade: 'unknown',
    };
    zones.set(name, zone);
    const ratio = ratios[n];
    const operation =
      instance.operation && instance.operation !== 'ENABLE'
        ? instance.operation
        : null;
    const errors = instance.errorReplicas;
    const tile: HeatTile = {
      instance,
      ratio,
      band: bandOf(ratio),
      shade: shadeOf(ratio, mode, reference),
      operation,
      text: percent(ratio),
      short: short(instance.name),
      label: [
        instance.name,
        own,
        `${measure} ${usageText(instance, key, topStateOnly)}`,
        !instance.live && 'not live',
        operation && `operation ${operation}`,
        errors && `${errors} ERROR replica${errors > 1 ? 's' : ''}`,
      ]
        .filter(Boolean)
        .join(', '),
    };
    counts[tile.band] = (counts[tile.band] || 0) + 1;
    zone.tiles.push(tile);
    tiles.push(tile);
  });
  const hotFirst = (a: HeatTile, b: HeatTile) =>
    (b.ratio ?? -1) - (a.ratio ?? -1) ||
    names.compare(a.instance.name, b.instance.name);
  const list = Array.from(zones.values());
  for (const zone of list) {
    zone.tiles.sort(hotFirst);
    const known = zone.tiles.filter((t) => t.ratio !== null);
    zone.max = known.length ? known[0].ratio : null;
    zone.mean = known.length
      ? known.reduce((sum, t) => sum + t.ratio, 0) / known.length
      : null;
    zone.shade = shadeOf(zone.mean, mode, reference);
  }
  const largest = Math.max(0, ...list.map((z) => z.tiles.length));
  return {
    zones: list.sort(
      (a, b) => +!a.name - +!b.name || names.compare(a.name, b.name),
    ),
    reference,
    basis,
    counts,
    hottest: tiles
      .filter((t) => t.ratio !== null)
      .sort(hotFirst)
      .slice(0, HOTTEST),
    columns: Math.max(4, Math.min(12, Math.ceil(Math.sqrt(largest)))),
  };
}

// Usage for every key and the resources using the most of the selected key.
// Resources carry no top state weights, so their usage is for all replicas.
export function instanceDetail(
  instance: InstanceCapacity,
  keys: string[],
  key: string,
  topStateOnly: boolean,
) {
  const max = instance.max?.[key];
  const usedOf = (r: InstanceResourceUsage) => r.used?.[key] ?? null;
  return {
    keys: keys.map((k) => {
      const ratio = capacityRatio(instance, k, topStateOnly);
      const text = usageText(instance, k, topStateOnly);
      return { key: k, ratio, band: bandOf(ratio), text };
    }),
    resources: [...instance.resources]
      .sort(
        (a, b) =>
          (usedOf(b) ?? -1) - (usedOf(a) ?? -1) ||
          b.replicas - a.replicas ||
          names.compare(a.resourceName, b.resourceName),
      )
      .slice(0, 8)
      .map((r) => {
        const used = usedOf(r);
        const share =
          max > 0 && used !== null ? ` (${percent(used / max)})` : '';
        return {
          name: r.resourceName,
          used:
            used !== null
              ? `${used} ${key}${share}`
              : r.waged
                ? 'unknown'
                : 'not WAGED',
          replicas: `${r.replicas} replicas, ${r.topStateReplicas} top state${
            r.errorReplicas ? `, ${r.errorReplicas} ERROR` : ''
          }`,
        };
      }),
  };
}

export type InstanceDetail = ReturnType<typeof instanceDetail>;
