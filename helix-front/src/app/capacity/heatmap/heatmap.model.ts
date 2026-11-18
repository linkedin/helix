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

export interface HeatTile {
  instance: InstanceCapacity;
  ratio: number | null;
  band: string;
  // colour step: l0 to l20 in 5% steps, over or unknown
  shade: string;
  // any operation other than ENABLE
  operation: string | null;
  text: string;
  label: string;
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
  counts: Record<string, number>;
  // cells per zone row, the same for every zone so zones compare by shape
  columns: number;
}

const names = new Intl.Collator(undefined, { numeric: true });

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

export function shadeOf(ratio: number | null): string {
  if (ratio === null) {
    return 'unknown';
  }
  return ratio > 1 ? 'over' : `l${Math.floor(ratio * 20)}`;
}

// Zones by name; inside a zone hottest first, unknown last, then by name.
export function buildHeatmap(
  snapshot: CapacitySnapshot | null,
  key: string,
  topStateOnly: boolean,
): Heatmap {
  const instances = snapshot?.instances || [];
  const measure = topStateOnly ? `${key} top state` : key;
  const ratios = instances.map((i) => capacityRatio(i, key, topStateOnly));
  const zones = new Map<string, HeatZone>();
  const counts: Record<string, number> = {};
  instances.forEach((instance, n) => {
    const name = instance.zone || '';
    const zone: HeatZone = zones.get(name) || {
      name,
      label: name || 'No fault zone',
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
      shade: shadeOf(ratio),
      operation,
      text: percent(ratio),
      label: [
        instance.name,
        zone.label,
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
    zone.shade = shadeOf(zone.mean);
  }
  const largest = Math.max(0, ...list.map((z) => z.tiles.length));
  return {
    zones: list.sort(
      (a, b) => +!a.name - +!b.name || names.compare(a.name, b.name),
    ),
    counts,
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
