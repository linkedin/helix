import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { CapacityService } from './shared/capacity.service';
import { CapacityOverviewComponent } from './capacity-overview/capacity-overview.component';

@NgModule({
  imports: [CommonModule, SharedModule],
  providers: [CapacityService],
  declarations: [CapacityOverviewComponent],
})
export class CapacityModule {}
