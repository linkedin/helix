import { map } from 'rxjs/operators';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { IdealState } from '../../shared/node-viewer/node-viewer.component';
import { HelixService } from '../../core/helix.service';
import { Resource, ResourceSummary } from './resource.model';

@Injectable()
export class ResourceService extends HelixService {
  public getAll(clusterName: string) {
    return this.request(`/clusters/${clusterName}/resources`).pipe(
      map((data) => {
        const res: Resource[] = [];
        for (const name of data.idealStates) {
          res.push(<Resource>{
            cluster: clusterName,
            name,
            alive: data.externalViews.indexOf(name) >= 0,
          });
        }
        return _.sortBy(res, 'name');
      })
    );
  }

  public getSummaries(clusterName: string) {
    return this.request(
      `/clusters/${clusterName}/resources?command=getResourceSummaries`,
    ).pipe(
      map((data) => {
        // an older REST server ignores the command and returns the plain list
        // with HTTP 200, which must not read as a cluster with no resources
        if (
          !data?.resource_summaries ||
          typeof data.resource_summaries !== 'object'
        ) {
          throw 'This REST server does not support resource summaries yet.';
        }
        return _.sortBy(
          Object.entries<any>(data.resource_summaries).map(
            ([name, summary]) =>
              <ResourceSummary>{
                ...summary,
                cluster: clusterName,
                name,
                alive: summary.hasExternalView,
              },
          ),
          'name',
        );
      }),
    );
  }

  public getAllOnInstance(clusterName: string, instanceName: string) {
    return this.request(
      `/clusters/${clusterName}/instances/${instanceName}/resources`
    ).pipe(
      map((data) => {
        const res: any[] = [];
        if (data) {
          for (const resource of data.resources) {
            res.push({
              name: resource,
            });
          }
        }
        return res;
      })
    );
  }

  public get(clusterName: string, resourceName: string) {
    return this.request(
      `/clusters/${clusterName}/resources/${resourceName}`
    ).pipe(
      map(
        (data) =>
          new Resource(
            clusterName,
            resourceName,
            data.resourceConfig,
            data.idealState,
            data.externalView
          )
      )
    );
  }

  public getOnInstance(
    clusterName: string,
    instanceName: string,
    resourceName: string
  ) {
    return this.request(
      `/clusters/${clusterName}/instances/${instanceName}/resources/${resourceName}`
    ).pipe(
      map((data) => {
        const ret = {
          bucketSize: data.simpleFields.BUCKET_SIZE,
          sessionId: data.simpleFields.SESSION_ID,
          stateModelDef: data.simpleFields.STATE_MODEL_DEF,
          stateModelFactoryName: data.simpleFields.STATE_MODEL_FACTORY_NAME,
          partitions: [],
        };

        for (const partition in data.mapFields) {
          const par = data.mapFields[partition];

          ret.partitions.push({
            name: partition.trim(),
            currentState: par.CURRENT_STATE.trim(),
            info: par.INFO.trim(),
          });
        }

        return ret;
      })
    );
  }

  public enable(clusterName: string, resourceName: string) {
    return this.post(
      `/clusters/${clusterName}/resources/${resourceName}?command=enable`,
      null
    );
  }

  public disable(clusterName: string, resourceName: string) {
    return this.post(
      `/clusters/${clusterName}/resources/${resourceName}?command=disable`,
      null
    );
  }

  public remove(clusterName: string, resourceName: string) {
    return this.delete(`/clusters/${clusterName}/resources/${resourceName}`);
  }

  public setIdealState(
    clusterName: string,
    resourceName: string,
    idealState: IdealState
  ) {
    return this.post(
      `/clusters/${clusterName}/resources/${resourceName}/idealState?command=update`,
      idealState
    );
  }
}
