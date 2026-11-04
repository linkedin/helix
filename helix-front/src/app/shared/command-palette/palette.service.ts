import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, map, shareReplay } from 'rxjs/operators';

import { HelixService } from '../../core/helix.service';

// Read-only name lookups for the command palette, cached per helix and cluster.
@Injectable()
export class PaletteService extends HelixService {
  private cache = new Map<string, Observable<string[]>>();

  clusters(helix: string) {
    return this.names(helix, '/clusters', (d) => d.clusters);
  }

  resources(helix: string, cluster: string) {
    return this.names(
      helix,
      `/clusters/${encodeURIComponent(cluster)}/resources`,
      (d) => d.idealStates,
    );
  }

  instances(helix: string, cluster: string) {
    return this.names(
      helix,
      `/clusters/${encodeURIComponent(cluster)}/instances`,
      (d) => d.instances,
    );
  }

  private names(helix: string, path: string, pick: (d: any) => string[]) {
    const key = `/${helix}${path}`;
    if (!this.cache.has(key)) {
      this.cache.set(
        key,
        this.request(path, `/${helix}`).pipe(
          map((d) => [...(pick(d) || [])].sort()),
          catchError(() => {
            this.cache.delete(key);
            return of([] as string[]);
          }),
          shareReplay(1),
        ),
      );
    }
    return this.cache.get(key);
  }
}
