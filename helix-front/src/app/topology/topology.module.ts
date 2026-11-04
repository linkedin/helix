import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { CapacityService } from '../capacity/shared/capacity.service';
import { InstanceService } from '../instance/shared/instance.service';
import { TopologyComponent } from './topology.component';

@NgModule({
  imports: [CommonModule, SharedModule],
  providers: [CapacityService, InstanceService],
  declarations: [TopologyComponent],
})
export class TopologyModule {}
