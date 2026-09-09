package org.apache.helix.cloud.event.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.InstanceValidationUtil;


class HelixEventHandlingUtil {

  /**
   * check if instance is disabled by cloud event.
   * @param instanceName
   * @param dataAccessor
   * @return return true only when instance is Helix disabled and the disabled reason in
   * instanceConfig is cloudEvent
   * @deprecated No need to check this if using InstanceOperation and specifying the trigger as CLOUD
   *            when enabling.
   */
  @Deprecated
  static boolean isInstanceDisabledForCloudEvent(String instanceName,
      HelixDataAccessor dataAccessor) {
    InstanceConfig instanceConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().instanceConfig(instanceName));
    if (instanceConfig == null) {
      throw new HelixException("Instance: " + instanceName
          + ", instance config does not exist");
    }
    return !InstanceValidationUtil.isEnabled(dataAccessor, instanceName) && instanceConfig
        .getInstanceDisabledType()
        .equals(InstanceConstants.InstanceDisabledType.CLOUD_EVENT.name());
  }

  /**
   * Return true if no instance is disabled due to a cloud event. Scans instance configs directly
   * (the authoritative source) instead of a denormalized cluster-config aggregate.
   * @param dataAccessor
   * @return
   */
  static boolean checkNoInstanceUnderCloudEvent(HelixDataAccessor dataAccessor) {
    List<String> instances =
        dataAccessor.getChildNames(dataAccessor.keyBuilder().instanceConfigs());
    if (instances == null || instances.isEmpty()) {
      return true;
    }
    for (String instance : instances) {
      if (isInstanceDisabledForCloudEvent(instance, dataAccessor)) {
        return false;
      }
    }
    return true;
  }
}
