import { map } from 'rxjs/operators';
import { Injectable } from '@angular/core';

import { Instance, InstanceOperationState } from './instance.model';
import { HelixService } from '../../core/helix.service';
import { Node } from '../../shared/models/node.model';

@Injectable()
export class InstanceService extends HelixService {
  public getAll(clusterName: string) {
    return this.request(`/clusters/${clusterName}/instances`).pipe(
      map((data) => {
        const onlineInstances: string[] = data.online || [];
        const enabledInstances: string[] = data.enabled || [];
        const disabledInstances: string[] = data.disabled || [];
        const evacuatedInstances: string[] = data.evacuated || [];
        const swapInInstances: string[] = data.swap_in || [];
        const unknownInstances: string[] = data.unknown || [];

        return data.instances
          .sort()
          .map((name) => {
            let operationState: InstanceOperationState = 'ENABLE';
            if (disabledInstances.indexOf(name) >= 0) {
              operationState = 'DISABLE';
            } else if (evacuatedInstances.indexOf(name) >= 0) {
              operationState = 'EVACUATE';
            } else if (swapInInstances.indexOf(name) >= 0) {
              operationState = 'SWAP_IN';
            } else if (unknownInstances.indexOf(name) >= 0) {
              operationState = 'UNKNOWN';
            } else if (enabledInstances.indexOf(name) >= 0) {
              operationState = 'ENABLE';
            }

            return new Instance(
              name,
              clusterName,
              operationState === 'ENABLE',
              onlineInstances.indexOf(name) >= 0,
              undefined,
              undefined,
              operationState
            );
          });
      })
    );
  }

  public get(clusterName: string, instanceName: string) {
    return this.request(
      `/clusters/${clusterName}/instances/${instanceName}`
    ).pipe(
      map((data) => {
        const liveInstance = data.liveInstance;
        const config = data.config;
        const enabled =
          config &&
          config.simpleFields &&
          config.simpleFields.HELIX_ENABLED != 'false';

        let operationState: InstanceOperationState = enabled
          ? 'ENABLE'
          : 'DISABLE';
        if (
          config &&
          config.simpleFields &&
          config.simpleFields.INSTANCE_OPERATION_STATE
        ) {
          const op = config.simpleFields.INSTANCE_OPERATION_STATE;
          if (['ENABLE', 'DISABLE', 'EVACUATE', 'SWAP_IN', 'UNKNOWN'].includes(op)) {
            operationState = op as InstanceOperationState;
          }
        }

        return liveInstance && liveInstance.simpleFields
          ? new Instance(
              data.id,
              clusterName,
              operationState === 'ENABLE',
              liveInstance.simpleFields.LIVE_INSTANCE,
              liveInstance.simpleFields.SESSION_ID,
              liveInstance.simpleFields.HELIX_VERSION,
              operationState
            )
          : new Instance(
              data.id,
              clusterName,
              operationState === 'ENABLE',
              null,
              undefined,
              undefined,
              operationState
            );
      })
    );
  }

  public create(
    clusterName: string,
    host: string,
    port: string,
    enabled: boolean
  ) {
    const name = `${host}_${port}`;

    const node = new Node(null);
    node.appendSimpleField('HELIX_ENABLED', enabled ? 'true' : 'false');
    node.appendSimpleField('HELIX_HOST', host);
    node.appendSimpleField('HELIX_PORT', port);

    return this.put(
      `/clusters/${clusterName}/instances/${name}`,
      JSON.parse(node.json(name))
    );
  }

  public remove(clusterName: string, instanceName: string) {
    return this.delete(`/clusters/${clusterName}/instances/${instanceName}`);
  }

  public enable(clusterName: string, instanceName: string) {
    return this.post(
      `/clusters/${clusterName}/instances/${instanceName}?command=enable`,
      null
    );
  }

  public disable(clusterName: string, instanceName: string) {
    return this.post(
      `/clusters/${clusterName}/instances/${instanceName}?command=disable`,
      null
    );
  }

  public setInstanceOperation(
    clusterName: string,
    instanceName: string,
    operation: InstanceOperationState,
    reason: string
  ) {
    const params = [
      `command=setInstanceOperation`,
      `instanceOperation=${operation}`,
      `instanceOperationSource=USER`,
      `reason=${encodeURIComponent(reason)}`,
    ].join('&');
    return this.post(
      `/clusters/${clusterName}/instances/${instanceName}?${params}`,
      null
    );
  }
}
