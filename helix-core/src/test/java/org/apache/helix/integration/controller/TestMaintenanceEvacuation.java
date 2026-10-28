package org.apache.helix.integration.controller;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestMaintenanceEvacuation extends ZkTestBase {
  private static final String RESOURCE = "resource";
  private static final String PARTITION = RESOURCE + "_0";
  private static final String OLD = "localhost_15000";
  private static final String MASTER = "localhost_15001";
  private static final String PEER = "localhost_15002";
  private static final String REPLACEMENT = "localhost_15003";
  private static final long TIMEOUT = 30_000;

  @DataProvider
  public Object[][] maintenanceScenarios() {
    return new Object[][] {
        {"slave"}, {"master"}, {"batch"}, {"bounce"}, {"dead"}, {"disabled"}
    };
  }

  @Test(dataProvider = "maintenanceScenarios")
  public void testEvacuationAcrossMaintenancePasses(String scenario) throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture.start(scenario.equals("master"));
      fixture.drainOldReplicaIntoMaintenance();
      if (scenario.equals("batch")) {
        fixture.setOperation(PEER, InstanceOperation.EVACUATE);
      } else if (scenario.equals("bounce")) {
        fixture.stopParticipant(PEER);
        fixture.startParticipant(PEER);
        fixture.awaitState(PEER, "OFFLINE");
      } else if (scenario.equals("dead")) {
        fixture.stopParticipant(PEER);
      } else if (scenario.equals("disabled")) {
        fixture.setOperation(PEER, InstanceOperation.DISABLE);
      }

      fixture.startController();
      boolean recoveryNeeded = scenario.equals("dead") || scenario.equals("disabled");
      if (recoveryNeeded) {
        Assert.assertTrue(fixture.oldModel.promotionStarted.await(TIMEOUT, TimeUnit.MILLISECONDS),
            "The old replica must recover when a peer is unavailable");
        fixture.awaitState(OLD, "OFFLINE");
        if (scenario.equals("dead")) {
          fixture.startParticipant(PEER);
        } else {
          fixture.setOperation(PEER, InstanceOperation.ENABLE);
        }
        fixture.awaitState(PEER, "SLAVE");
        fixture.verifyMaintenancePasses("SLAVE");
        Assert.assertEquals(fixture.oldModel.cancellations.get(), 0);
        fixture.oldModel.allowPromotion.countDown();
        fixture.awaitState(OLD, "SLAVE");
        Assert.assertEquals(fixture.oldModel.promotions.get(), 1);
      } else {
        fixture.awaitState(PEER, "SLAVE");
        fixture.verifyMaintenancePasses("OFFLINE");
        fixture.awaitState(OLD, "OFFLINE");
        Assert.assertEquals(fixture.oldModel.promotions.get(), 0,
            "Maintenance must not reactivate the old replica with enough holders");
      }
      Assert.assertEquals(fixture.oldModel.cancellations.get(), 0,
          "No cancellation should reach the old replica during maintenance");
    }
  }

  private class Fixture implements AutoCloseable {
    private final String cluster = "TestMaintenanceEvacuation_" + System.nanoTime();
    private final Map<String, HelixManager> participants = new HashMap<>();
    private final ConfigAccessor config = new ConfigAccessor(_gZkClient);
    private final RecordingStateModel oldModel = new RecordingStateModel();
    private ClusterControllerManager controller;
    private int controllerNumber;

    void start(boolean oldIsMaster) throws Exception {
      _gSetupTool.addCluster(cluster, true);
      ClusterConfig clusterConfig = config.getClusterConfig(cluster);
      clusterConfig.stateTransitionCancelEnabled(true);
      clusterConfig.setPersistBestPossibleAssignment(true);
      config.setClusterConfig(cluster, clusterConfig);
      for (String host : List.of(OLD, MASTER, PEER, REPLACEMENT)) {
        _gSetupTool.addInstanceToCluster(cluster, host);
        startParticipant(host);
      }
      _gSetupTool.getClusterManagementTool().addResource(cluster, RESOURCE, 1, "MasterSlave",
          IdealState.RebalanceMode.SEMI_AUTO.name());
      IdealState target = idealState();
      target.setReplicas("3");
      target.setPreferenceList(PARTITION, new ArrayList<>(oldIsMaster
          ? List.of(OLD, MASTER, PEER) : List.of(MASTER, OLD, PEER)));
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, RESOURCE, target);
      startController();
      awaitState(OLD, oldIsMaster ? "MASTER" : "SLAVE");
      awaitState(PEER, "SLAVE");
    }

    void startParticipant(String host) throws Exception {
      HelixManager participant =
          HelixManagerFactory.getZKHelixManager(cluster, host, InstanceType.PARTICIPANT, ZK_ADDR);
      participant.getStateMachineEngine().registerStateModelFactory("MasterSlave",
          new StateModelFactory<RecordingStateModel>() {
            @Override
            public RecordingStateModel createNewStateModel(String resource, String partition) {
              return host.equals(OLD) ? oldModel : new RecordingStateModel();
            }
          });
      participants.put(host, participant);
      participant.connect();
    }

    void stopParticipant(String host) {
      participants.remove(host).disconnect();
    }

    void startController() {
      controller = new ClusterControllerManager(ZK_ADDR, cluster, "controller_" + controllerNumber++);
      controller.syncStart();
    }

    IdealState idealState() {
      return _gSetupTool.getClusterManagementTool().getResourceIdealState(cluster, RESOURCE);
    }

    void setOperation(String host, InstanceOperation operation) {
      InstanceConfig instanceConfig = config.getInstanceConfig(cluster, host);
      instanceConfig.setInstanceOperation(operation);
      config.setInstanceConfig(cluster, host, instanceConfig);
    }

    void drainOldReplicaIntoMaintenance() throws Exception {
      oldModel.blockDrain = true;
      IdealState target = idealState();
      target.setPreferenceList(PARTITION, new ArrayList<>(List.of(MASTER, PEER, REPLACEMENT)));
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, RESOURCE, target);
      Assert.assertTrue(oldModel.drainStarted.await(TIMEOUT, TimeUnit.MILLISECONDS));
      awaitState(REPLACEMENT, "SLAVE");
      controller.syncStop();
      controller = null;
      setOperation(OLD, InstanceOperation.EVACUATE);
      _gSetupTool.getClusterManagementTool()
          .manuallyEnableMaintenanceMode(cluster, true, "test evacuation", null);
      target = idealState();
      target.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      target.getRecord().getMapFields().clear();
      _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, RESOURCE, target);
      oldModel.blockPromotion = true;
      oldModel.allowDrain.countDown();
      awaitState(OLD, "OFFLINE");
      HelixManager old = participants.get(OLD);
      Assert.assertTrue(TestHelper.verify(() -> old.getHelixDataAccessor()
          .getChildNames(old.getHelixDataAccessor().keyBuilder().messages(OLD)).isEmpty(), TIMEOUT));
    }

    void awaitState(String host, String state) throws Exception {
      Assert.assertTrue(TestHelper.verify(() -> {
        HelixManager participant = participants.get(host);
        CurrentState current = participant.getHelixDataAccessor().getProperty(
            participant.getHelixDataAccessor().keyBuilder()
                .currentState(host, participant.getSessionId(), RESOURCE));
        return current != null && state.equals(current.getState(PARTITION));
      }, TIMEOUT), host + " did not reach " + state);
    }

    void verifyMaintenancePasses(String expected) throws Exception {
      // Clear the persisted output so each assertion observes a newly executed pipeline.
      for (int pass = 0; pass < 3; pass++) {
        IdealState target = idealState();
        target.getRecord().getMapFields().clear();
        _gSetupTool.getClusterManagementTool().setResourceIdealState(cluster, RESOURCE, target);
        Assert.assertTrue(TestHelper.verify(() -> {
          Map<String, String> assignment = idealState().getInstanceStateMap(PARTITION);
          return assignment != null && expected.equals(assignment.get(OLD));
        }, TIMEOUT), "Unexpected maintenance assignment for old replica");
        Assert.assertEquals(oldModel.cancellations.get(), 0);
        Assert.assertTrue(_gSetupTool.getClusterManagementTool().isInMaintenanceMode(cluster));
      }
    }

    @Override
    public void close() {
      oldModel.allowDrain.countDown();
      oldModel.allowPromotion.countDown();
      if (controller != null) {
        controller.syncStop();
      }
      participants.values().forEach(HelixManager::disconnect);
      _gSetupTool.deleteCluster(cluster);
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public static class RecordingStateModel extends StateModel {
    final CountDownLatch drainStarted = new CountDownLatch(1);
    final CountDownLatch allowDrain = new CountDownLatch(1);
    final CountDownLatch promotionStarted = new CountDownLatch(1);
    final CountDownLatch allowPromotion = new CountDownLatch(1);
    final AtomicInteger promotions = new AtomicInteger();
    final AtomicInteger cancellations = new AtomicInteger();
    volatile boolean blockDrain;
    volatile boolean blockPromotion;

    @Transition(from = "*", to = "*")
    public void transition(Message message, NotificationContext context) throws InterruptedException {
      if (blockDrain && message.getFromState().equals("SLAVE")
          && message.getToState().equals("OFFLINE")) {
        drainStarted.countDown();
        Assert.assertTrue(allowDrain.await(TIMEOUT, TimeUnit.MILLISECONDS));
      }
      if (blockPromotion && message.getFromState().equals("OFFLINE")
          && message.getToState().equals("SLAVE")) {
        promotions.incrementAndGet();
        promotionStarted.countDown();
        Assert.assertTrue(allowPromotion.await(TIMEOUT, TimeUnit.MILLISECONDS));
      }
    }

    @Override
    public void cancel() {
      cancellations.incrementAndGet();
    }
  }
}
