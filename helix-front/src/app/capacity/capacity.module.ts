import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { CapacityService } from './shared/capacity.service';
import { CapacityOverviewComponent } from './capacity-overview/capacity-overview.component';
import { CapacityHeatmapComponent } from './heatmap/capacity-heatmap.component';

@NgModule({
  imports: [CommonModule, SharedModule],
  providers: [CapacityService],
  declarations: [CapacityOverviewComponent, CapacityHeatmapComponent],
})
export class CapacityModule {}
