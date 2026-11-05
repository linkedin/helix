import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, map, shareReplay } from 'rxjs/operators';

import { HelixService } from '../../core/helix.service';

const TTL_MS = 5 * 60 * 1000;

// Read-only name lookups for the command palette, cached per helix and cluster
// for a few minutes so repeated opens do not refetch.
@Injectable()
export class PaletteService extends HelixService {
  private cache = new Map<
    string,
    { at: number; names$: Observable<string[]> }
  >();

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
    const hit = this.cache.get(key);
    if (hit && Date.now() - hit.at < TTL_MS) return hit.names$;
    const names$ = this.request(path, `/${helix}`).pipe(
      map((d) => [...(pick(d) || [])].sort()),
      catchError(() => {
        this.cache.delete(key);
        return of([] as string[]);
      }),
      shareReplay(1),
    );
    this.cache.set(key, { at: Date.now(), names$ });
    return names$;
  }
}
