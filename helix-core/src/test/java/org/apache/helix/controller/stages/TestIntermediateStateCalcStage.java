package org.apache.helix.controller.stages;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestIntermediateStateCalcStage extends BaseStageTest {
  private ClusterConfig _clusterConfig;

  @Test
  public void testNoStateMissing() {
    String resourcePrefix = "resource";
    int nResource = 4;
    int nPartition = 2;
    int nReplica = 3;

    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "_" + i;
    }

    preSetup(resources, nReplica, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    // Initialize bestpossible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();
    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(1);
    setClusterConfig(_clusterConfig);

    for (String resource : resources) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
          if (resource.endsWith("0")) {
            // Regular recovery balance
            currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
            // add blocked state transition messages
            Message pendingMessage = generateMessage("OFFLINE", "ONLINE", instanceName);
            currentStateOutput.setPendingMessage(resource, partition, instanceName, pendingMessage);

            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");

            // should be recovered:
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          } else if (resource.endsWith("1")) {
            // Regular load balance
            currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
            currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            messageSelectOutput.addMessage(resource, partition,
                generateMessage("OFFLINE", "DROPPED", instanceName + "-1"));
            // should be recovered:
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          } else if (resource.endsWith("2")) {
            // Recovery balance with transient states, should keep the current states in the output.
            currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "OFFLINE");
            // should be kept unchanged:
            expectedResult.setState(resource, partition, instanceName, "OFFLINE");
          } else if (resource.endsWith("3")) {
            // One unresolved error should not prevent recovery balance
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            if (p == 0) {
              if (r == 0) {
                currentStateOutput.setCurrentState(resource, partition, instanceName, "ERROR");
                bestPossibleStateOutput.setState(resource, partition, instanceName, "ERROR");
                // This partition is still ERROR
                expectedResult.setState(resource, partition, instanceName, "ERROR");
              } else {
                currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
                messageSelectOutput.addMessage(resource, partition, generateMessage("OFFLINE", "ONLINE", instanceName));
                // Recovery balance
                expectedResult.setState(resource, partition, instanceName, "ONLINE");
              }
            } else {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
              // load balance is throttled, so keep all current states
              messageSelectOutput.addMessage(resource, partition,
                  generateMessage("OFFLINE", "DROPPED", instanceName + "-1"));
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
              // The following must be removed because now downward state transitions are allowed
              // expectedResult.setState(resource, partition, instanceName + "-1", "OFFLINE");
            }
          } else if (resource.endsWith("4")) {
            // Test that partitions with replicas to drop are dropping them when recovery is
            // happening for other partitions
            if (p == 0) {
              // This partition requires recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              messageSelectOutput.addMessage(resource, partition, generateMessage("OFFLINE", "ONLINE", instanceName));
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              // Other partitions require dropping of replicas
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
              // BestPossibleState dictates that we only need one ONLINE replica
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "DROPPED");
              messageSelectOutput.addMessage(resource, partition,
                  generateMessage("OFFLINE", "DROPPED", instanceName + "-1"));
              // So instanceName-1 will NOT be expected to show up in expectedResult
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
              expectedResult.setState(resource, partition, instanceName + "-1", "DROPPED");
            }
          } else if (resource.endsWith("5")) {
            // Test that load balance bringing up a new replica does NOT happen with a recovery
            // partition
            if (p == 0) {
              // Set up a partition requiring recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              messageSelectOutput.addMessage(resource, partition, generateMessage("OFFLINE", "ONLINE", instanceName));
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              // Check that load balance (bringing up a new node) did not take place
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "ONLINE");
              messageSelectOutput.addMessage(resource, partition,
                  generateMessage("OFFLINE", "ONLINE", instanceName + "-1"));
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            }
          }
        }
      }
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    for (String resource : resources) {
      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertTrue(output.getPartitionStateMap(resource)
          .getStateMap()
          .equals(expectedResult.getPartitionStateMap(resource).getStateMap()));
    }
  }

  @Test
  public void testWithClusterConfigChange() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 2;
    int nReplica = 3;

    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "_" + i;
    }

    preSetup(resources, nReplica, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(_clusterName);
    monitor.active();
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);

    // Initialize best possible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    for (String resource : resources) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
          if (resource.endsWith("0")) {
            // Test that when the threshold is set at a number greater than the number of error and
            // recovery partitions, load balance DOES take place
            _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(Integer.MAX_VALUE);
            setClusterConfig(_clusterConfig);
            if (p == 0) {
              // Set up a partition requiring recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              messageSelectOutput.addMessage(resource, partition, generateMessage("OFFLINE", "ONLINE", instanceName));
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              // Ensure we have at least one ONLINE replica so that this partition does not need
              // recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              expectedResult.setState(resource, partition, instanceName, "ONLINE");

              // This partition to bring up a replica (load balance will happen)
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "ONLINE");
              messageSelectOutput.addMessage(resource, partition,
                  generateMessage("OFFLINE", "ONLINE", instanceName + "-1"));
              expectedResult.setState(resource, partition, instanceName + "-1", "ONLINE");
            }
          }
        }
      }
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());


    // Validate that there are 0 resourced load balance been throttled
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    Assert.assertEquals(clusterStatusMonitor.getNumOfResourcesRebalanceThrottledGauge(), 0);
    Assert.assertEquals(clusterStatusMonitor.getResourceMonitor("resource_0")
        .getRebalanceThrottledByErrorPartitionGauge(), 0);

    for (String resource : resources) {
      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertEquals(output.getPartitionStateMap(resource).getStateMap(),
          expectedResult.getPartitionStateMap(resource).getStateMap());
    }
  }

  @Test
  public void testThrottleByErrorPartition() {
    String resourcePrefix = "resource";
    int nResource = 3;
    int nPartition = 3;
    int nReplica = 3;

    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "_" + i;
    }

    preSetup(resources, nReplica, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(_clusterName);
    monitor.active();
    event.addAttribute(AttributeName.clusterStatusMonitor.name(), monitor);

    // Initialize best possible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(0);
    setClusterConfig(_clusterConfig);

    for (String resource : resources) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;
          partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
          // A resource with 2 replicas in error state and one need recovery in offline->online. error state
          // throttle won't block recovery rebalance
          if (resource.endsWith("0")) {
            if (p <= 1) {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ERROR");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ERROR");
              expectedResult.setState(resource, partition, instanceName, "ERROR");
            } else {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              expectedResult.setState(resource, partition, instanceName, "OFFLINE");
              if (r == 0) {
                messageSelectOutput.addMessage(resource, partition,
                    generateMessage("OFFLINE", "ONLINE", instanceName));
                expectedResult.setState(resource, partition, instanceName, "ONLINE");
              }
            }
          } else if (resource.endsWith("1")) {
            // A resource with 1 replicas in error state and one need load balance in offline->online. error state
            // throttle will block load rebalance
            if (p <= 0) {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ERROR");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ERROR");
              expectedResult.setState(resource, partition, instanceName, "ERROR");
            } else {
              if (r == 0) {
                currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
                bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
                expectedResult.setState(resource, partition, instanceName, "ONLINE");
              } else {
                // even though there is ST msg, it should be throttled
                currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
                bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
                messageSelectOutput.addMessage(resource, partition,
                    generateMessage("OFFLINE", "ONLINE", instanceName));
                expectedResult.setState(resource, partition, instanceName, "OFFLINE");
              }
            }
          } else {
            // A resource need regular load balance
            currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
            currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            messageSelectOutput.addMessage(resource, partition,
                generateMessage("OFFLINE", "DROPPED", instanceName + "-1"));
            // should be recovered:
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          }
        }
      }
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    // Validate that there are 2 resourced load balance been throttled
    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    Assert.assertEquals(clusterStatusMonitor.getNumOfResourcesRebalanceThrottledGauge(), 2);
    Assert.assertEquals(clusterStatusMonitor.getResourceMonitor("resource_0")
        .getRebalanceThrottledByErrorPartitionGauge(), 1);
    Assert.assertEquals(clusterStatusMonitor.getResourceMonitor("resource_1")
        .getRebalanceThrottledByErrorPartitionGauge(), 1);
    Assert.assertEquals(clusterStatusMonitor.getResourceMonitor("resource_2")
        .getRebalanceThrottledByErrorPartitionGauge(), 0);

    for (String resource : resources) {
      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertEquals(output.getPartitionStateMap(resource).getStateMap(),
          expectedResult.getPartitionStateMap(resource).getStateMap());
    }
  }

  @Test
  public void testPartitionMissing() {
    String resourcePrefix = "resource";
    int nResource = 4;
    int nPartition = 2;
    int nReplica = 3;

    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "_" + i;
    }

    preSetup(resources, nReplica, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    // Initialize bestpossible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();
    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(1);
    setClusterConfig(_clusterConfig);

    for (String resource : resources) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;

          // PartitionMap is used as a preferenceList.
          // For the last partition, let us add null as preferenceList.
          if (p != nPartition - 1) {
             partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
          } else {
             partitionMap.put(partition.getPartitionName(), null);
          }

          // TODO: The following code is same for testNoStateMissing
          if (resource.endsWith("0")) {
            // Regular recovery balance
            currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
            // add blocked state transition messages
            Message pendingMessage = generateMessage("OFFLINE", "ONLINE", instanceName);
            currentStateOutput.setPendingMessage(resource, partition, instanceName, pendingMessage);

            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");

            // should be recovered:
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          } else if (resource.endsWith("1")) {
            // Regular load balance
            currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
            currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            messageSelectOutput.addMessage(resource, partition,
                generateMessage("OFFLINE", "DROPPED", instanceName + "-1"));
            // should be recovered:
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          } else if (resource.endsWith("2")) {
            // Recovery balance with transient states, should keep the current states in the output.
            currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "OFFLINE");
            // should be kept unchanged:
            expectedResult.setState(resource, partition, instanceName, "OFFLINE");
          } else if (resource.endsWith("3")) {
            // One unresolved error should not prevent recovery balance
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            if (p == 0) {
              if (r == 0) {
                currentStateOutput.setCurrentState(resource, partition, instanceName, "ERROR");
                bestPossibleStateOutput.setState(resource, partition, instanceName, "ERROR");
                // This partition is still ERROR
                expectedResult.setState(resource, partition, instanceName, "ERROR");
              } else {
                currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
                messageSelectOutput.addMessage(resource, partition, generateMessage("OFFLINE", "ONLINE", instanceName));
                // Recovery balance
                expectedResult.setState(resource, partition, instanceName, "ONLINE");
              }
            } else {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
              // load balance is throttled, so keep all current states
              messageSelectOutput.addMessage(resource, partition,
                  generateMessage("OFFLINE", "DROPPED", instanceName + "-1"));
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
              // The following must be removed because now downward state transitions are allowed
              // expectedResult.setState(resource, partition, instanceName + "-1", "OFFLINE");
            }
          } else if (resource.endsWith("4")) {
            // Test that partitions with replicas to drop are dropping them when recovery is
            // happening for other partitions
            if (p == 0) {
              // This partition requires recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              messageSelectOutput.addMessage(resource, partition, generateMessage("OFFLINE", "ONLINE", instanceName));
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              // Other partitions require dropping of replicas
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              currentStateOutput.setCurrentState(resource, partition, instanceName + "-1", "OFFLINE");
              // BestPossibleState dictates that we only need one ONLINE replica
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "DROPPED");
              messageSelectOutput.addMessage(resource, partition,
                  generateMessage("OFFLINE", "DROPPED", instanceName + "-1"));
              // So instanceName-1 will NOT be expected to show up in expectedResult
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
              expectedResult.setState(resource, partition, instanceName + "-1", "DROPPED");
            }
          } else if (resource.endsWith("5")) {
            // Test that load balance bringing up a new replica does NOT happen with a recovery
            // partition
            if (p == 0) {
              // Set up a partition requiring recovery
              currentStateOutput.setCurrentState(resource, partition, instanceName, "OFFLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              messageSelectOutput.addMessage(resource, partition, generateMessage("OFFLINE", "ONLINE", instanceName));
              // After recovery, it should be back ONLINE
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            } else {
              currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
              bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
              // Check that load balance (bringing up a new node) did not take place
              bestPossibleStateOutput.setState(resource, partition, instanceName + "-1", "ONLINE");
              messageSelectOutput.addMessage(resource, partition,
                  generateMessage("OFFLINE", "ONLINE", instanceName + "-1"));
              expectedResult.setState(resource, partition, instanceName, "ONLINE");
            }
          }
        }
      }
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    for (String resource : resources) {
      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertTrue(output.getPartitionStateMap(resource)
          .getStateMap()
          .equals(expectedResult.getPartitionStateMap(resource).getStateMap()));
    }
  }

  @Test
  public void testMessageAlreadyApplied() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 5;
    int nReplica = 1;

    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "_" + i;
    }

    preSetup(resources, nReplica, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(), getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    // Initialize bestpossible state and current state
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();
    IntermediateStateOutput expectedResult = new IntermediateStateOutput();

    _clusterConfig.setErrorOrRecoveryPartitionThresholdForLoadBalance(1);
    setClusterConfig(_clusterConfig);

    for (String resource : resources) {
      IdealState is = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
      setSingleIdealState(is);

      Map<String, List<String>> partitionMap = new HashMap<>();
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        for (int r = 0; r < nReplica; r++) {
          String instanceName = HOSTNAME_PREFIX + r;

          // PartitionMap is used as a preferenceList.
          partitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
          if (p == 0) {
            // Dont set current state. set best possible on another node
            // Set a pending message to drop a replica that doesn't exist in currentState
            currentStateOutput.setPendingMessage(resource, partition, instanceName,
                generateMessage("OFFLINE", "DROPPED", instanceName));
            // Set pending message to bootstrap elsewhere
            String dummyInstance = "dummy_instance";
            bestPossibleStateOutput.setState(resource, partition, dummyInstance, "ONLINE");
            messageSelectOutput.addMessage(resource, partition,
                generateMessage("OFFLINE", "ONLINE", dummyInstance));
            expectedResult.setState(resource, partition, dummyInstance, "ONLINE");
          } else {
            currentStateOutput.setCurrentState(resource, partition, instanceName, "ONLINE");
            bestPossibleStateOutput.setState(resource, partition, instanceName, "ONLINE");
            expectedResult.setState(resource, partition, instanceName, "ONLINE");
          }
        }
      }
      bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);
    }

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    for (String resource : resources) {
      // Note Assert.assertEquals won't work. If "actual" is an empty map, it won't compare
      // anything.
      Assert.assertTrue(output.getPartitionStateMap(resource)
          .getStateMap()
          .equals(expectedResult.getPartitionStateMap(resource).getStateMap()));
    }
  }

  @Test
  public void testEnableRecoveryRebalanceForTopStateDownwardStateTransition() {
    String[] resources = {"TestResource"};
    String resource = resources[0];
    int nReplica = 3;
    int nPartition = 1;

    setupIdealState(4, resources, nPartition, nReplica, IdealState.RebalanceMode.FULL_AUTO,
        "MasterSlave", null, null, 2);
    setupStateModel();
    setupInstances(4);
    setupLiveInstances(4);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    // Set load balance throttling to allow 0 load balance messages per instance so that
    // a MASTER->SLAVE message classified as LOAD_BALANCE gets throttled
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    _clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 0)));
    setClusterConfig(_clusterConfig);

    Partition partition = new Partition(resource + "_0");
    Map<String, List<String>> partitionMap = new HashMap<>();
    partitionMap.put(partition.getPartitionName(),
        Arrays.asList("localhost_1", "localhost_2", "localhost_3"));
    bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);

    bestPossibleStateOutput.setState(resource, partition, "localhost_1", "MASTER");
    bestPossibleStateOutput.setState(resource, partition, "localhost_2", "SLAVE");
    bestPossibleStateOutput.setState(resource, partition, "localhost_3", "SLAVE");

    currentStateOutput.setCurrentState(resource, partition, "localhost_0", "MASTER");
    currentStateOutput.setCurrentState(resource, partition, "localhost_1", "SLAVE");
    currentStateOutput.setCurrentState(resource, partition, "localhost_2", "SLAVE");

    messageSelectOutput.addMessage(resource, partition,
        generateMessage("MASTER", "SLAVE", "localhost_0"));

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    // Without the config, the MASTER->SLAVE message is classified as LOAD_BALANCE and gets throttled
    Assert.assertEquals(messageSelectOutput.getMessages(resource, partition).size(), 0);
    Assert.assertTrue(
        output.getPartitionStateMap(resource).getPartitionMap(partition)
            .equals(currentStateOutput.getCurrentStateMap(resource, partition)));

    // Now enable the config to treat topState downward transition as recovery rebalance
    _clusterConfig.setRecoveryRebalanceForTopStateDownwardTransitionEnabled(true);
    setClusterConfig(_clusterConfig);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    messageSelectOutput.addMessage(resource, partition,
        generateMessage("MASTER", "SLAVE", "localhost_0"));
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    // With the config enabled, the MASTER->SLAVE message is now RECOVERY_REBALANCE and not throttled
    Assert.assertEquals(messageSelectOutput.getMessages(resource, partition).size(), 1);
    Assert.assertEquals(
        messageSelectOutput.getMessages(resource, partition).get(0).getSTRebalanceType(),
        Message.STRebalanceType.RECOVERY_REBALANCE);
    Map<String, String> stateMap =
        output.getPartitionStateMap(resource).getPartitionMap(partition);
    Assert.assertTrue(
        stateMap.values().stream().allMatch(state -> state.equals("SLAVE")),
        "All hosts should be in SLAVE state after MASTER->SLAVE transition is allowed");
  }

  @Test
  public void testNonTopStateDownwardTransitionNotReclassifiedAsRecovery() {
    String[] resources = {"TestResource"};
    String resource = resources[0];
    int nReplica = 3;
    int nPartition = 1;

    setupIdealState(4, resources, nPartition, nReplica, IdealState.RebalanceMode.FULL_AUTO,
        "MasterSlave", null, null, 2);
    setupStateModel();
    setupInstances(4);
    setupLiveInstances(4);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    // Enable recovery rebalance for top state downward transition AND
    // set load balance throttle to 0 so load balance messages get throttled
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    _clusterConfig.setRecoveryRebalanceForTopStateDownwardTransitionEnabled(true);
    _clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 0)));
    setClusterConfig(_clusterConfig);

    Partition partition = new Partition(resource + "_0");
    Map<String, List<String>> partitionMap = new HashMap<>();
    partitionMap.put(partition.getPartitionName(),
        Arrays.asList("localhost_1", "localhost_2", "localhost_3"));
    bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);

    bestPossibleStateOutput.setState(resource, partition, "localhost_1", "MASTER");
    bestPossibleStateOutput.setState(resource, partition, "localhost_2", "SLAVE");
    bestPossibleStateOutput.setState(resource, partition, "localhost_3", "SLAVE");

    // localhost_0 is SLAVE and needs to go OFFLINE (non-top-state downward transition)
    currentStateOutput.setCurrentState(resource, partition, "localhost_0", "SLAVE");
    currentStateOutput.setCurrentState(resource, partition, "localhost_1", "MASTER");
    currentStateOutput.setCurrentState(resource, partition, "localhost_2", "SLAVE");

    // SLAVE->OFFLINE is NOT a top-state downward transition, should remain LOAD_BALANCE
    messageSelectOutput.addMessage(resource, partition,
        generateMessage("SLAVE", "OFFLINE", "localhost_0"));

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    // SLAVE->OFFLINE should still be throttled because it is LOAD_BALANCE, not reclassified
    Assert.assertEquals(messageSelectOutput.getMessages(resource, partition).size(), 0,
        "SLAVE->OFFLINE should NOT be reclassified as RECOVERY_REBALANCE");
  }

  @Test
  public void testRecoveryRebalanceForTopStateDownwardWithLeaderStandby() {
    String[] resources = {"TestResource"};
    String resource = resources[0];
    int nReplica = 3;
    int nPartition = 1;

    setupIdealState(4, resources, nPartition, nReplica, IdealState.RebalanceMode.FULL_AUTO,
        "LeaderStandby", null, null, 2);
    setupStateModel();
    setupInstances(4);
    setupLiveInstances(4);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "LeaderStandby"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "LeaderStandby"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    // Enable recovery rebalance for top state downward transition AND
    // set load balance throttle to 0 so load balance messages get throttled
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    _clusterConfig.setRecoveryRebalanceForTopStateDownwardTransitionEnabled(true);
    _clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 0)));
    setClusterConfig(_clusterConfig);

    Partition partition = new Partition(resource + "_0");
    Map<String, List<String>> partitionMap = new HashMap<>();
    partitionMap.put(partition.getPartitionName(),
        Arrays.asList("localhost_1", "localhost_2", "localhost_3"));
    bestPossibleStateOutput.setPreferenceLists(resource, partitionMap);

    bestPossibleStateOutput.setState(resource, partition, "localhost_1", "LEADER");
    bestPossibleStateOutput.setState(resource, partition, "localhost_2", "STANDBY");
    bestPossibleStateOutput.setState(resource, partition, "localhost_3", "STANDBY");

    currentStateOutput.setCurrentState(resource, partition, "localhost_0", "LEADER");
    currentStateOutput.setCurrentState(resource, partition, "localhost_1", "STANDBY");
    currentStateOutput.setCurrentState(resource, partition, "localhost_2", "STANDBY");

    // LEADER->STANDBY is a top-state downward transition
    messageSelectOutput.addMessage(resource, partition,
        generateMessage("LEADER", "STANDBY", "localhost_0"));

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    // LEADER->STANDBY should be reclassified as RECOVERY_REBALANCE and not throttled
    Assert.assertEquals(messageSelectOutput.getMessages(resource, partition).size(), 1,
        "LEADER->STANDBY should be reclassified as RECOVERY_REBALANCE and not throttled");
    Assert.assertEquals(
        messageSelectOutput.getMessages(resource, partition).get(0).getSTRebalanceType(),
        Message.STRebalanceType.RECOVERY_REBALANCE);
    Map<String, String> stateMap =
        output.getPartitionStateMap(resource).getPartitionMap(partition);
    Assert.assertTrue(
        stateMap.values().stream().allMatch(state -> state.equals("STANDBY")),
        "All hosts should be in STANDBY state after LEADER->STANDBY transition is allowed");
  }

  @Test
  public void testPendingMessagesExceedThresholdBlockNewResource() {
    String existingResource = "existingResource";
    String newResource = "newResource";
    String[] resources = new String[]{existingResource, newResource};
    int nPartition = 3;
    int nInstances = 3;
    int nReplica = 1;

    preSetup(resources, nInstances, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    String instanceName = HOSTNAME_PREFIX + 0;

    // existingResource: 3 partitions on localhost_0 with pending OFFLINE→ONLINE messages.
    // These 3 pending messages will exhaust the instance-level RECOVERY_BALANCE quota of 3.
    IdealState existingIs =
        accessor.getProperty(accessor.keyBuilder().idealStates(existingResource));
    setSingleIdealState(existingIs);
    Map<String, List<String>> existingPartitionMap = new HashMap<>();
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(existingResource + "_" + p);
      existingPartitionMap
          .put(partition.getPartitionName(), Collections.singletonList(instanceName));
      currentStateOutput.setCurrentState(existingResource, partition, instanceName, "OFFLINE");
      bestPossibleStateOutput.setState(existingResource, partition, instanceName, "ONLINE");
      Message pendingMessage = generateMessage("OFFLINE", "ONLINE", instanceName);
      currentStateOutput
          .setPendingMessage(existingResource, partition, instanceName, pendingMessage);
    }
    bestPossibleStateOutput.setPreferenceLists(existingResource, existingPartitionMap);

    // newResource: 3 partitions on localhost_0 needing OFFLINE→ONLINE (recovery).
    // Since the throttle quota on localhost_0 is already exhausted by existingResource's pending
    // messages, all transitions for newResource should be throttled.
    IdealState newIs = accessor.getProperty(accessor.keyBuilder().idealStates(newResource));
    setSingleIdealState(newIs);
    Map<String, List<String>> newPartitionMap = new HashMap<>();
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(newResource + "_" + p);
      newPartitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
      currentStateOutput.setCurrentState(newResource, partition, instanceName, "OFFLINE");
      bestPossibleStateOutput.setState(newResource, partition, instanceName, "ONLINE");
      messageSelectOutput.addMessage(newResource, partition,
          generateMessage("OFFLINE", "ONLINE", instanceName));
    }
    bestPossibleStateOutput.setPreferenceLists(newResource, newPartitionMap);

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    Map<Partition, Map<String, String>> newResourceStateMap =
        output.getPartitionStateMap(newResource).getStateMap();
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(newResource + "_" + p);
      Assert.assertEquals(newResourceStateMap.get(partition).get(instanceName), "OFFLINE",
          "Partition " + partition.getPartitionName()
              + " should be throttled because pending messages already exhausted the quota");
    }
  }

  @Test
  public void testPendingMessagesBelowThresholdPartiallyThrottleNewResource() {
    String existingResource = "existingResource";
    String newResource = "newResource";
    String[] resources = new String[]{existingResource, newResource};
    int nPartition = 3;
    int nInstances = 3;
    int nReplica = 1;

    preSetup(resources, nInstances, nReplica);
    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    String instanceName = HOSTNAME_PREFIX + 0;

    // existingResource: 2 partitions with pending messages, 1 partition already ONLINE.
    // This charges 2 of the 3 instance-level RECOVERY_BALANCE quota, leaving 1 remaining.
    IdealState existingIs =
        accessor.getProperty(accessor.keyBuilder().idealStates(existingResource));
    setSingleIdealState(existingIs);
    Map<String, List<String>> existingPartitionMap = new HashMap<>();
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(existingResource + "_" + p);
      existingPartitionMap
          .put(partition.getPartitionName(), Collections.singletonList(instanceName));
      if (p < 2) {
        currentStateOutput.setCurrentState(existingResource, partition, instanceName, "OFFLINE");
        bestPossibleStateOutput.setState(existingResource, partition, instanceName, "ONLINE");
        Message pendingMessage = generateMessage("OFFLINE", "ONLINE", instanceName);
        currentStateOutput
            .setPendingMessage(existingResource, partition, instanceName, pendingMessage);
      } else {
        currentStateOutput.setCurrentState(existingResource, partition, instanceName, "ONLINE");
        bestPossibleStateOutput.setState(existingResource, partition, instanceName, "ONLINE");
      }
    }
    bestPossibleStateOutput.setPreferenceLists(existingResource, existingPartitionMap);

    // newResource: 3 partitions on localhost_0 needing OFFLINE→ONLINE (recovery).
    // With 1 quota remaining, exactly 1 partition should transition and 2 should be throttled.
    IdealState newIs = accessor.getProperty(accessor.keyBuilder().idealStates(newResource));
    setSingleIdealState(newIs);
    Map<String, List<String>> newPartitionMap = new HashMap<>();
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(newResource + "_" + p);
      newPartitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
      currentStateOutput.setCurrentState(newResource, partition, instanceName, "OFFLINE");
      bestPossibleStateOutput.setState(newResource, partition, instanceName, "ONLINE");
      messageSelectOutput.addMessage(newResource, partition,
          generateMessage("OFFLINE", "ONLINE", instanceName));
    }
    bestPossibleStateOutput.setPreferenceLists(newResource, newPartitionMap);

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    Map<Partition, Map<String, String>> newResourceStateMap =
        output.getPartitionStateMap(newResource).getStateMap();
    int onlineCount = 0;
    int offlineCount = 0;
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(newResource + "_" + p);
      String state = newResourceStateMap.get(partition).get(instanceName);
      if ("ONLINE".equals(state)) {
        onlineCount++;
      } else if ("OFFLINE".equals(state)) {
        offlineCount++;
      }
    }
    Assert.assertEquals(onlineCount, 1,
        "Exactly 1 partition should have transitioned with the remaining quota");
    Assert.assertEquals(offlineCount, 2,
        "2 partitions should be throttled and remain OFFLINE");
  }

  @Test
  public void testAnyTypeThrottleWithPendingMessagesBlockNewResource() {
    String existingResource = "existingResource";
    String newResource = "newResource";
    String[] resources = new String[]{existingResource, newResource};
    int nPartition = 4;
    int nInstances = 3;
    int nReplica = 1;

    // Use a custom setup with RebalanceType.ANY at INSTANCE scope, limit 3.
    // This directly reproduces the customer scenario where MAX_PARTITION_IN_TRANSITION
    // is configured at INSTANCE scope regardless of rebalance type.
    setupIdealState(nInstances, resources, nInstances, nReplica,
        IdealState.RebalanceMode.FULL_AUTO, "OnlineOffline");
    setupStateModel();
    setupInstances(nInstances);
    setupLiveInstances(nInstances);
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    _clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.ANY,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 3)));
    setClusterConfig(_clusterConfig);

    event.addAttribute(AttributeName.RESOURCES.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "OnlineOffline"));

    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageSelectOutput = new MessageOutput();

    String instanceName = HOSTNAME_PREFIX + 0;

    // existingResource: 3 partitions on localhost_0 with pending OFFLINE→ONLINE messages.
    // These 3 pending messages exhaust the ANY quota of 3 on this instance.
    IdealState existingIs =
        accessor.getProperty(accessor.keyBuilder().idealStates(existingResource));
    setSingleIdealState(existingIs);
    Map<String, List<String>> existingPartitionMap = new HashMap<>();
    for (int p = 0; p < 3; p++) {
      Partition partition = new Partition(existingResource + "_" + p);
      existingPartitionMap
          .put(partition.getPartitionName(), Collections.singletonList(instanceName));
      currentStateOutput.setCurrentState(existingResource, partition, instanceName, "OFFLINE");
      bestPossibleStateOutput.setState(existingResource, partition, instanceName, "ONLINE");
      Message pendingMessage = generateMessage("OFFLINE", "ONLINE", instanceName);
      currentStateOutput
          .setPendingMessage(existingResource, partition, instanceName, pendingMessage);
    }
    // 4th partition is already ONLINE
    Partition stablePartition = new Partition(existingResource + "_3");
    existingPartitionMap
        .put(stablePartition.getPartitionName(), Collections.singletonList(instanceName));
    currentStateOutput.setCurrentState(existingResource, stablePartition, instanceName, "ONLINE");
    bestPossibleStateOutput.setState(existingResource, stablePartition, instanceName, "ONLINE");
    bestPossibleStateOutput.setPreferenceLists(existingResource, existingPartitionMap);

    // newResource: 4 partitions on localhost_0 needing OFFLINE→ONLINE.
    // Since the ANY quota on localhost_0 is fully exhausted, all messages should be throttled.
    IdealState newIs = accessor.getProperty(accessor.keyBuilder().idealStates(newResource));
    setSingleIdealState(newIs);
    Map<String, List<String>> newPartitionMap = new HashMap<>();
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(newResource + "_" + p);
      newPartitionMap.put(partition.getPartitionName(), Collections.singletonList(instanceName));
      currentStateOutput.setCurrentState(newResource, partition, instanceName, "OFFLINE");
      bestPossibleStateOutput.setState(newResource, partition, instanceName, "ONLINE");
      messageSelectOutput.addMessage(newResource, partition,
          generateMessage("OFFLINE", "ONLINE", instanceName));
    }
    bestPossibleStateOutput.setPreferenceLists(newResource, newPartitionMap);

    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageSelectOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE_EXCLUDING_UNKNOWN.name(), currentStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());
    runStage(event, new ReadClusterDataStage());
    runStage(event, new IntermediateStateCalcStage());

    IntermediateStateOutput output = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    Map<Partition, Map<String, String>> newResourceStateMap =
        output.getPartitionStateMap(newResource).getStateMap();
    for (int p = 0; p < nPartition; p++) {
      Partition partition = new Partition(newResource + "_" + p);
      Assert.assertEquals(newResourceStateMap.get(partition).get(instanceName), "OFFLINE",
          "Partition " + partition.getPartitionName()
              + " should be throttled because ANY quota on the instance is exhausted");
    }
  }

  private void preSetup(String[] resources, int numOfLiveInstances, int numOfReplicas) {
    setupIdealState(numOfLiveInstances, resources, numOfLiveInstances, numOfReplicas,
        IdealState.RebalanceMode.FULL_AUTO, "OnlineOffline");
    setupStateModel();
    setupInstances(numOfLiveInstances);
    setupLiveInstances(numOfLiveInstances);

    // Set up cluster configs
    _clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    _clusterConfig.setStateTransitionThrottleConfigs(ImmutableList.of(
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 3),
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, 3)));
    setClusterConfig(_clusterConfig);
  }
}
