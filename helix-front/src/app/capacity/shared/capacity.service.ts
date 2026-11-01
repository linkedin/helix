import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { timeout } from 'rxjs/operators';

import { HelixService } from '../../core/helix.service';
import { CapacitySnapshot } from './capacity-snapshot.model';

// The server lets a coalesced build run for up to 120 seconds, so wait a little
// longer than that before reporting a failure.
export const SNAPSHOT_TIMEOUT_MS = 130000;

@Injectable()
export class CapacityService extends HelixService {
  // maxStaleMs is a cache hint for the server, 0 forces a rebuild.
  public getSnapshot(
    clusterName: string,
    maxStaleMs?: number,
  ): Observable<CapacitySnapshot> {
    const query = maxStaleMs != null ? `?maxStaleMs=${maxStaleMs}` : '';
    return this.request(
      `/clusters/${encodeURIComponent(clusterName)}/capacity/heatmap${query}`,
    ).pipe(timeout(SNAPSHOT_TIMEOUT_MS));
  }
}
