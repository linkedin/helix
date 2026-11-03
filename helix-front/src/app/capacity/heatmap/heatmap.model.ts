import {
  capacityRatio,
  CapacitySnapshot,
  InstanceCapacity,
  InstanceResourceUsage,
} from '../shared/capacity-snapshot.model';
import { Rect, squarify, Tile } from './treemap';

// Coolest to hottest. Unknown has its own grey band and never counts as low.
export const BANDS = [
  { id: 'unknown', label: 'Unknown' },
  { id: 'low', label: 'Under 50%' },
  { id: 'mid', label: '50 to 75%' },
  { id: 'high', label: '75 to 90%' },
  { id: 'hot', label: '90 to 100%' },
  { id: 'over', label: 'Over 100%' },
];

export interface HeatTile {
  instance: InstanceCapacity;
  ratio: number | null;
  band: string;
  weight: number;
  // any operation other than ENABLE
  operation: string | null;
  text: string;
  label: string;
}

export interface HeatZone {
  name: string;
  label: string;
  tiles: HeatTile[];
  weight: number;
  worst: number | null;
}

export interface ZoneBox extends Rect {
  zone: HeatZone;
  head: number;
  tiles: Tile<HeatTile>[];
}

const names = new Intl.Collator(undefined, { numeric: true });
const GAP = 2;
const HEAD = 18;

export function bandOf(ratio: number | null): string {
  if (ratio === null) {
    return 'unknown';
  }
  const steps = [0.5, 0.75, 0.9].filter((step) => ratio >= step).length;
  return ratio > 1 ? 'over' : ['low', 'mid', 'high', 'hot'][steps];
}

export function percent(ratio: number | null): string {
  return ratio === null ? 'unknown' : `${Math.round(ratio * 100)}%`;
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

// Zones worst first, instances by name so a node keeps its place in its zone.
export function buildHeatmap(
  snapshot: CapacitySnapshot | null,
  key: string,
  topStateOnly: boolean,
): HeatZone[] {
  const zones = new Map<string, HeatZone>();
  const mode = topStateOnly ? `${key} top state` : key;
  for (const instance of snapshot?.instances || []) {
    const name = instance.zone || '';
    const zone: HeatZone = zones.get(name) || {
      name,
      label: name || 'No fault zone',
      tiles: [],
      weight: 0,
      worst: null,
    };
    zones.set(name, zone);
    const ratio = capacityRatio(instance, key, topStateOnly);
    const operation =
      instance.operation && instance.operation !== 'ENABLE'
        ? instance.operation
        : null;
    const errors = instance.errorReplicas;
    const tile: HeatTile = {
      instance,
      ratio,
      band: bandOf(ratio),
      // sized by load with a floor so cool nodes stay visible; unknown gets a
      // middle size, neither hidden nor mistaken for a hot node
      weight: 0.4 + Math.min(ratio ?? 0.5, 1.3),
      operation,
      text: percent(ratio),
      label: [
        instance.name,
        zone.label,
        `${mode} ${usageText(instance, key, topStateOnly)}`,
        !instance.live && 'not live',
        operation && `operation ${operation}`,
        errors && `${errors} ERROR replica${errors > 1 ? 's' : ''}`,
      ]
        .filter(Boolean)
        .join(', '),
    };
    zone.tiles.push(tile);
    zone.weight += tile.weight;
    zone.worst =
      ratio === null ? zone.worst : Math.max(zone.worst ?? ratio, ratio);
  }
  const list = Array.from(zones.values());
  list.forEach((zone) =>
    zone.tiles.sort((a, b) => names.compare(a.instance.name, b.instance.name)),
  );
  return list.sort(
    (a, b) =>
      (b.worst ?? -1) - (a.worst ?? -1) || names.compare(a.label, b.label),
  );
}

// Zone boxes are inset by a gap; tiles are relative to their zone box.
export function layoutHeatmap(
  zones: HeatZone[],
  width: number,
  height: number,
): ZoneBox[] {
  const items = zones.map((zone) => ({ weight: zone.weight, value: zone }));
  return squarify(items, { x: 0, y: 0, w: width, h: height }).map((box) => {
    const [w, h] = [box.w - GAP, box.h - GAP];
    const head = h > 2 * HEAD ? HEAD : 0;
    const tiles = box.value.tiles.map((tile) => ({
      weight: tile.weight,
      value: tile,
    }));
    return {
      zone: box.value,
      x: box.x + GAP / 2,
      y: box.y + GAP / 2,
      w,
      h,
      head,
      tiles: squarify(tiles, { x: 0, y: head, w, h: h - head }),
    };
  });
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
