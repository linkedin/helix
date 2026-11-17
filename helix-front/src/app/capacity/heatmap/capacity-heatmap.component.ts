import {
  ChangeDetectionStrategy,
  Component,
  Input,
  OnChanges,
} from '@angular/core';

import { CapacitySnapshot } from '../shared/capacity-snapshot.model';
import {
  BANDS,
  buildHeatmap,
  Heatmap,
  HeatTile,
  HeatZone,
  instanceDetail,
  InstanceDetail,
  percent,
  shadeOf,
} from './heatmap.model';

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

  readonly bands = BANDS;
  readonly loadSteps = Array.from({ length: 21 }, (_, i) => `l${i}`);
  readonly percent = percent;
  readonly shadeOf = shadeOf;
  heatmap: Heatmap = buildHeatmap(null, '', false);
  dense = false;
  selected: HeatTile | null = null;
  detail: InstanceDetail | null = null;

  ngOnChanges() {
    this.heatmap = buildHeatmap(this.snapshot, this.key, this.topStateOnly);
    this.dense = (this.snapshot?.instances.length || 0) > DENSE;
    const name = this.selected?.instance.name;
    let selected: HeatTile | null = null;
    this.heatmap.zones.forEach((zone) =>
      zone.tiles.forEach((t) => {
        selected = t.instance.name === name ? t : selected;
      }),
    );
    this.select(selected);
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

  // lets a host name wrap after its dots and underscores
  breakable(name: string): string[] {
    return name.split(/(?<=[._])/);
  }

  byZone(_: number, zone: HeatZone) {
    return zone.name;
  }

  byTile(_: number, tile: HeatTile) {
    return tile.instance.name;
  }
}
