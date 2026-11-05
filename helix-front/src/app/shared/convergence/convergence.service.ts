import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { HelixService } from '../../core/helix.service';
import { ConvergenceReport } from './convergence.model';

@Injectable()
export class ConvergenceService extends HelixService {
  get(cluster: string, resource?: string): Observable<ConvergenceReport> {
    return this.request(
      resource
        ? `/clusters/${cluster}/resources/${resource}/convergence`
        : `/clusters/${cluster}/convergence`,
    );
  }
}
