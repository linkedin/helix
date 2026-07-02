package org.apache.helix.rest.clusterMaintenanceService;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.PropertyKey;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStoppableInstancesSelector {
  private static final String TEST_CLUSTER = "TestCluster";

  /**
   * findToBeStoppedInstances should read every instance config in a single batched
   * getChildValuesMap call (not one getProperty per instance), flag instances whose operation is
   * EVACUATE/SWAP_IN/UNKNOWN, and skip an instance that is present in the topology but missing an
   * InstanceConfig without throwing (previously an unguarded getInstanceOperation() would NPE).
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testFindToBeStoppedInstancesBatchesReadsAndSkipsMissingConfig() throws IOException {
    // Topology contains an instance ("instance_no_config") that has no InstanceConfig on purpose.
    Set<String> allInstances = new HashSet<>(Arrays.asList("instance_evacuate", "instance_swap_in",
        "instance_unknown", "instance_enable", "instance_no_config"));
    ClusterTopology clusterTopology = mock(ClusterTopology.class);
    when(clusterTopology.getAllInstances()).thenReturn(allInstances);

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put("instance_evacuate",
        instanceConfig("instance_evacuate", InstanceConstants.InstanceOperation.EVACUATE));
    instanceConfigMap.put("instance_swap_in",
        instanceConfig("instance_swap_in", InstanceConstants.InstanceOperation.SWAP_IN));
    instanceConfigMap.put("instance_unknown",
        instanceConfig("instance_unknown", InstanceConstants.InstanceOperation.UNKNOWN));
    instanceConfigMap.put("instance_enable",
        instanceConfig("instance_enable", InstanceConstants.InstanceOperation.ENABLE));
    // "instance_no_config" deliberately absent from the config map.

    ZKHelixDataAccessor dataAccessor = mock(ZKHelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(TEST_CLUSTER));
    when(dataAccessor.<InstanceConfig>getChildValuesMap(any(PropertyKey.class), anyBoolean()))
        .thenReturn(instanceConfigMap);

    // Capture the toBeStoppedInstances set that findToBeStoppedInstances produces and forwards to
    // the stoppable-check call. An empty instance list keeps the check itself a no-op.
    MaintenanceManagementService maintenanceService = mock(MaintenanceManagementService.class);
    ArgumentCaptor<Set<String>> toBeStoppedCaptor = ArgumentCaptor.forClass(Set.class);
    when(maintenanceService.batchGetInstancesStoppableChecks(anyString(), anyList(),
        nullable(String.class), toBeStoppedCaptor.capture(), anyBoolean()))
        .thenReturn(Collections.emptyMap());

    StoppableInstancesSelector selector =
        new StoppableInstancesSelector.StoppableInstancesSelectorBuilder().setClusterId(TEST_CLUSTER)
            .setClusterTopology(clusterTopology).setDataAccessor(dataAccessor)
            .setMaintenanceService(maintenanceService).setIncludeDetails(false).build();

    // A pre-existing presumed-stopped instance is passed through and must be retained.
    selector.getStoppableInstancesNonZoneBased(Collections.emptyList(),
        Collections.singletonList("instance_preexisting"));

    Set<String> capturedToBeStopped = toBeStoppedCaptor.getValue();
    Assert.assertEquals(capturedToBeStopped, new HashSet<>(Arrays.asList("instance_preexisting",
        "instance_evacuate", "instance_swap_in", "instance_unknown")));
    // ENABLE instance is not flagged; the config-less instance is skipped without throwing.
    Assert.assertFalse(capturedToBeStopped.contains("instance_enable"));
    Assert.assertFalse(capturedToBeStopped.contains("instance_no_config"));

    // Configs are read via a single batched call rather than one getProperty per instance.
    verify(dataAccessor).getChildValuesMap(any(PropertyKey.class), anyBoolean());
    verify(dataAccessor, never()).getProperty(any(PropertyKey.class));
  }

  private static InstanceConfig instanceConfig(String instanceName,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setInstanceOperation(operation);
    return instanceConfig;
  }
}
