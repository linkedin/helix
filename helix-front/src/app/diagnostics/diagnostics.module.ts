import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SharedModule } from '../shared/shared.module';
import { ClusterDiagnosticsComponent } from './cluster-diagnostics/cluster-diagnostics.component';

@NgModule({
  imports: [CommonModule, SharedModule],
  declarations: [ClusterDiagnosticsComponent],
})
export class DiagnosticsModule {}
