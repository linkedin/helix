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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.helix.MockAccessor;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.model.ConvergenceStatus.OptimizerStatus;
import org.apache.helix.model.ConvergenceStatus.Reason;
import org.apache.helix.model.ConvergenceStatus.Scope;
import org.apache.helix.model.ConvergenceStatus.Status;
import org.apache.helix.model.ConvergenceStatus.TargetFreshness;
import org.apache.helix.model.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestConvergenceStatusPersistStage {
  private static final String CLUSTER = "TestConvergencePersistence";
  private static final String RESOURCE = "TestDB";
  private static final String SESSION = "controller_session";

  private MockAccessor _accessor;
  private DummyClusterManager _manager;
  private ClusterEvent _event;
  private ConvergenceStatusPersistStage _stage;

  @BeforeMethod
  public void setUp() {
    _accessor = new MockAccessor();
    _manager = new DummyClusterManager(CLUSTER, _accessor, SESSION) {
      @Override
      public boolean isLeader() {
        return true;
      }
    };
    _event = new ClusterEvent(CLUSTER, ClusterEventType.CurrentStateChange, "event_0");
    _event.addAttribute(AttributeName.helixmanager.name(), _manager);
    _event.addAttribute(AttributeName.EVENT_SESSION.name(), Optional.of(SESSION));
    _stage = new ConvergenceStatusPersistStage();
  }

  @Test
  public void testExecute_continuousMismatch_preservesFirstObservedTime() {
    persistSnapshot(Status.BLOCKED, Reason.NO_PROGRESS_PATH);
    ConvergenceStatus first =
        _accessor.getProperty(_accessor.keyBuilder().convergenceStatus(RESOURCE));
    long firstObserved = first.getUnconvergedSince();

    persistSnapshot(Status.IN_PROGRESS, Reason.PENDING_TRANSITION);
    ConvergenceStatus second =
        _accessor.getProperty(_accessor.keyBuilder().convergenceStatus(RESOURCE));

    Assert.assertTrue(firstObserved > 0);
    Assert.assertEquals(second.getUnconvergedSince(), firstObserved);
    Assert.assertEquals(second.getStatus(), Status.IN_PROGRESS);
  }

  @Test
  public void testExecute_convergedResource_clearsFirstObservedTime() {
    persistSnapshot(Status.BLOCKED, Reason.NO_PROGRESS_PATH);

    persistSnapshot(Status.CONVERGED, Reason.NONE);
    ConvergenceStatus status =
        _accessor.getProperty(_accessor.keyBuilder().convergenceStatus(RESOURCE));

    Assert.assertEquals(status.getStatus(), Status.CONVERGED);
    Assert.assertEquals(status.getUnconvergedSince(), 0L);
  }

  @Test
  public void testExecute_removedResource_deletesPersistedStatus() {
    persistSnapshot(Status.BLOCKED, Reason.NO_PROGRESS_PATH);
    ConvergenceStatus emptyCluster = createStatus(CLUSTER, Scope.CLUSTER, Status.CONVERGED,
        Reason.NONE, 0);
    _event.addAttribute(AttributeName.CONVERGENCE_STATUS.name(),
        new ConvergenceStatusSnapshot(emptyCluster, Collections.emptyMap()));

    _stage.execute(_event);

    Assert.assertNull(_accessor.getProperty(_accessor.keyBuilder().convergenceStatus(RESOURCE)));
    ConvergenceStatus cluster =
        _accessor.getProperty(_accessor.keyBuilder().convergenceStatus());
    Assert.assertEquals(cluster.getTotalResourceCount(), 0);
  }

  @Test
  public void testProcess_monitoringDisabled_skipsAsyncWork() throws Exception {
    ClusterConfig clusterConfig = new ClusterConfig(CLUSTER);
    _accessor.setProperty(_accessor.keyBuilder().clusterConfig(), clusterConfig);
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER);
    cache.refresh(_accessor);
    Resource resource = new Resource(RESOURCE);
    Map<String, Resource> resources = Collections.singletonMap(RESOURCE, resource);
    _event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    _event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resources);

    _stage.process(_event);

    Assert.assertFalse(_event.containsAttribute(
        AttributeName.CONVERGENCE_STATUS_CONTEXT.name()));
    Assert.assertFalse(clusterConfig.isConvergenceMonitoringEnabled());
  }

  private void persistSnapshot(Status status, Reason reason) {
    ConvergenceStatus resourceStatus =
        createStatus(RESOURCE, Scope.RESOURCE, status, reason, 1);
    resourceStatus.setResourceName(RESOURCE);
    ConvergenceStatus clusterStatus =
        createStatus(CLUSTER, Scope.CLUSTER, status, reason, 1);
    clusterStatus.setTotalResourceCount(1);
    _event.addAttribute(AttributeName.CONVERGENCE_STATUS.name(),
        new ConvergenceStatusSnapshot(clusterStatus,
            Collections.singletonMap(RESOURCE, resourceStatus)));

    _stage.execute(_event);
  }

  private ConvergenceStatus createStatus(String id, Scope scope, Status status, Reason reason,
      int partitionCount) {
    ConvergenceStatus convergenceStatus = new ConvergenceStatus(id);
    convergenceStatus.setScope(scope);
    convergenceStatus.setStatus(status);
    convergenceStatus.setPrimaryReason(reason);
    convergenceStatus.setTargetFreshness(TargetFreshness.CURRENT);
    convergenceStatus.setOptimizerStatus(OptimizerStatus.NOT_APPLICABLE);
    convergenceStatus.setControllerSessionId(SESSION);
    convergenceStatus.setSourceEventId(_event.getEventId());
    convergenceStatus.setGeneratedAt(System.currentTimeMillis());
    convergenceStatus.setTotalPartitionCount(partitionCount);
    convergenceStatus.setConvergedPartitionCount(status == Status.CONVERGED ? partitionCount : 0);
    convergenceStatus.setInProgressPartitionCount(
        status == Status.IN_PROGRESS ? partitionCount : 0);
    convergenceStatus.setBlockedPartitionCount(status == Status.BLOCKED ? partitionCount : 0);
    convergenceStatus.setUnknownPartitionCount(status == Status.UNKNOWN ? partitionCount : 0);
    convergenceStatus.setAffectedPartitionCount(
        status == Status.CONVERGED ? 0 : partitionCount);
    return convergenceStatus;
  }
}
