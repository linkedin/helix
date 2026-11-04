import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  ViewChild,
} from '@angular/core';

import { CapacitySnapshot } from '../shared/capacity-snapshot.model';
import {
  BANDS,
  buildHeatmap,
  HeatTile,
  HeatZone,
  instanceDetail,
  InstanceDetail,
  layoutHeatmap,
  ZoneBox,
} from './heatmap.model';
import { Tile } from './treemap';

@Component({
  selector: 'hi-capacity-heatmap',
  templateUrl: './capacity-heatmap.component.html',
  styleUrls: ['./capacity-heatmap.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class CapacityHeatmapComponent implements OnInit, OnChanges, OnDestroy {
  @Input() snapshot: CapacitySnapshot | null = null;
  @Input() key = '';
  @Input() topStateOnly = false;
  @ViewChild('map', { static: true }) map: ElementRef<HTMLElement>;

  readonly bands = BANDS;
  zones: HeatZone[] = [];
  boxes: ZoneBox[] = [];
  counts: Record<string, number> = {};
  height = 0;
  selected: HeatTile | null = null;
  detail: InstanceDetail | null = null;
  private width = 0;
  private observer: ResizeObserver | null = null;

  constructor(private changes: ChangeDetectorRef) {}

  ngOnInit() {
    if (typeof ResizeObserver !== 'undefined') {
      this.observer = new ResizeObserver(() => {
        // runs outside the Angular zone, so render this view directly
        if (this.map.nativeElement.clientWidth !== this.width) {
          this.layout();
          this.changes.detectChanges();
        }
      });
      this.observer.observe(this.map.nativeElement);
    }
  }

  ngOnChanges() {
    this.zones = buildHeatmap(this.snapshot, this.key, this.topStateOnly);
    this.counts = {};
    const name = this.selected?.instance.name;
    let selected: HeatTile | null = null;
    this.zones.forEach((zone) =>
      zone.tiles.forEach((tile) => {
        this.counts[tile.band] = (this.counts[tile.band] || 0) + 1;
        selected = tile.instance.name === name ? tile : selected;
      }),
    );
    this.select(selected);
    this.layout();
  }

  ngOnDestroy() {
    this.observer?.disconnect();
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

  byZone(_: number, box: ZoneBox) {
    return box.zone.name;
  }

  byTile(_: number, tile: Tile<HeatTile>) {
    return tile.value.instance.name;
  }

  // Runs once per input change or width change, never per frame.
  private layout() {
    // 960 is only a fallback for an element that is not laid out yet
    this.width = this.map?.nativeElement.clientWidth || 960;
    this.height = Math.min(
      720,
      300 + 10 * (this.snapshot?.instances.length || 0),
    );
    this.boxes = layoutHeatmap(this.zones, this.width, this.height);
  }
}
