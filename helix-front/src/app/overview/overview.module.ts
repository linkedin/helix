import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { ClusterOverviewComponent } from './cluster-overview/cluster-overview.component';

@NgModule({
  imports: [CommonModule, SharedModule],
  declarations: [ClusterOverviewComponent],
})
export class OverviewModule {}
