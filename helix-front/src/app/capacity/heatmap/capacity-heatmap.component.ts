import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  Input,
  OnChanges,
  ViewChild,
} from '@angular/core';

import { CapacitySnapshot } from '../shared/capacity-snapshot.model';
import {
  BANDS,
  buildHeatmap,
  ColorMode,
  Heatmap,
  HeatTile,
  HeatZone,
  instanceDetail,
  InstanceDetail,
  percent,
  shadeOf,
} from './heatmap.model';

// for a quoted attribute selector
const quoted = (value: string) => value.replace(/["\\]/g, '\\$&');

// widest the hover card gets, padding included
const CARD = 280;

// above this many instances cells shrink and drop their text
const DENSE = 800;

@Component({
  selector: 'hi-capacity-heatmap',
  templateUrl: './capacity-heatmap.component.html',
  styleUrls: ['./capacity-heatmap.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class CapacityHeatmapComponent implements OnChanges {
  @Input() snapshot: CapacitySnapshot | null = null;
  @Input() key = '';
  @Input() topStateOnly = false;
  @Input() mode: ColorMode = 'load';
  @Input() byZone = true;
  @ViewChild('map', { static: true }) map: ElementRef<HTMLElement>;

  readonly bands = BANDS;
  readonly loadSteps = Array.from({ length: 21 }, (_, i) => `l${i}`);
  readonly medianSteps = [5, 4, 3, 2, 1]
    .map((i) => `m${i}`)
    .concat(['p0', 'p1', 'p2', 'p3', 'p4', 'p5']);
  readonly percent = percent;
  readonly loadShade = (ratio: number | null) => shadeOf(ratio, 'load', null);
  heatmap: Heatmap = buildHeatmap(null, '', false);
  zonesByLoad: HeatZone[] = [];
  dense = false;
  // what was typed, kept as is; query is its normalised form
  queryText = '';
  query = '';
  matches = new Set<string>();
  selected: HeatTile | null = null;
  detail: InstanceDetail | null = null;
  hover: { tile: HeatTile; x: number; y: number; below: boolean } | null = null;
  private tiles = new Map<string, HeatTile>();

  ngOnChanges() {
    this.build();
  }

  select(tile: HeatTile | null) {
    this.selected = tile;
    this.detail =
      tile &&
      instanceDetail(
        tile.instance,
        this.snapshot.capacityKeys,
        this.key,
        this.topStateOnly,
      );
  }

  // selects from the summary and brings the tile into view
  reveal(tile: HeatTile) {
    this.select(tile);
    this.cell(tile.instance.name)?.scrollIntoView({ block: 'nearest' });
  }

  revealZone(zone: HeatZone) {
    this.map.nativeElement
      .querySelector(`[data-zone="${quoted(zone.label)}"]`)
      ?.scrollIntoView({ block: 'nearest' });
  }

  find(text: string) {
    this.queryText = text;
    this.query = text.trim().toLowerCase();
    this.matches = new Set(
      this.query
        ? [...this.tiles.keys()].filter((name) =>
            name.toLowerCase().includes(this.query),
          )
        : [],
    );
  }

  findFirst() {
    const first = [...this.matches][0];
    if (first) {
      this.reveal(this.tiles.get(first));
    }
  }

  // one shared card for every cell, placed under the cell (or above it near
  // the bottom of the map) and kept inside the map's width
  showCard(event: Event) {
    const cell = (event.target as HTMLElement).closest?.('[data-instance]');
    const tile = cell && this.tiles.get(cell.getAttribute('data-instance'));
    if (!tile) {
      this.hover = null;
      return;
    }
    const map = this.map.nativeElement.getBoundingClientRect();
    const box = cell.getBoundingClientRect();
    const below = box.bottom + 120 < map.bottom || box.top - map.top < 120;
    this.hover = {
      tile,
      // half the card's widest, so it never sticks out of the map
      x: Math.max(
        CARD / 2,
        Math.min(map.width - CARD / 2, box.left - map.left + box.width / 2),
      ),
      y: below ? box.bottom - map.top + 4 : box.top - map.top - 4,
      below,
    };
  }

  hideCard() {
    this.hover = null;
  }

  // lets a host name wrap after its dots and underscores
  breakable(name: string): string[] {
    return name.split(/(?<=[._])/);
  }

  zoneId(_: number, zone: HeatZone) {
    return zone.name;
  }

  byTile(_: number, tile: HeatTile) {
    return tile.instance.name;
  }

  private cell(name: string): HTMLElement | null {
    return this.map.nativeElement.querySelector(
      `[data-instance="${quoted(name)}"]`,
    );
  }

  private build() {
    this.heatmap = buildHeatmap(
      this.snapshot,
      this.key,
      this.topStateOnly,
      this.mode,
      this.byZone,
    );
    this.zonesByLoad = [...this.heatmap.zones].sort(
      (a, b) => (b.mean ?? -1) - (a.mean ?? -1),
    );
    this.dense = (this.snapshot?.instances.length || 0) > DENSE;
    this.tiles = new Map();
    this.heatmap.zones.forEach((zone) =>
      zone.tiles.forEach((t) => this.tiles.set(t.instance.name, t)),
    );
    this.hover = null;
    this.select(this.tiles.get(this.selected?.instance.name) || null);
    this.find(this.queryText);
  }
}
