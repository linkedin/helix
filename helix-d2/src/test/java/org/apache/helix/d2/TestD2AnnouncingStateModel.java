package org.apache.helix.d2;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class TestD2AnnouncingStateModel {

  private D2PartitionAnnouncer mockD2Announcer;
  private HelixD2Announcer helixD2Announcer;
  private TrackingStateModelFactory delegateFactory;
  private D2AnnouncingStateModelFactory<TrackingStateModel> factory;

  @BeforeMethod
  public void setUp() {
    mockD2Announcer = mock(D2PartitionAnnouncer.class);
    helixD2Announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockD2Announcer)
        .build();
    delegateFactory = new TrackingStateModelFactory();
    factory = new D2AnnouncingStateModelFactory<>(delegateFactory, helixD2Announcer, "LEADER");
  }

  @Test
  public void testLeaderTransitionTriggersAnnounce() throws Exception {
    StateModel model = factory.createNewStateModel("resource", "resource_0");
    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    // Simulate STANDBY → LEADER
    D2AnnouncingStateModel d2Model = (D2AnnouncingStateModel) model;
    d2Model.onBecomeLeaderFromStandby(msg, ctx);

    // Verify delegate was called
    TrackingStateModel delegate = delegateFactory.lastCreated;
    Assert.assertTrue(delegate.becameLeader, "Delegate should have been called for LEADER");

    // Verify D2 was notified
    Assert.assertTrue(factory.getCurrentLeaderPartitions().contains("resource_0"),
        "resource_0 should be in leader partitions after STANDBY→LEADER");
  }

  @Test
  public void testStandbyTransitionTriggersDeannounce() throws Exception {
    StateModel model = factory.createNewStateModel("resource", "resource_0");
    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);
    D2AnnouncingStateModel d2Model = (D2AnnouncingStateModel) model;

    // First make it LEADER
    d2Model.onBecomeLeaderFromStandby(msg, ctx);
    Assert.assertTrue(factory.getCurrentLeaderPartitions().contains("resource_0"));

    // Then LEADER → STANDBY
    d2Model.onBecomeStandbyFromLeader(msg, ctx);

    // Verify delegate was called
    TrackingStateModel delegate = delegateFactory.lastCreated;
    Assert.assertTrue(delegate.becameStandby, "Delegate should have been called for STANDBY");

    // Verify D2 was notified (partition removed)
    Assert.assertFalse(factory.getCurrentLeaderPartitions().contains("resource_0"),
        "resource_0 should be removed from leader partitions after LEADER→STANDBY");
  }

  @Test
  public void testDelegateExceptionPropagated() {
    // Create a factory with an error-throwing delegate
    StateModelFactory<ErrorStateModel> errorFactory = new StateModelFactory<ErrorStateModel>() {
      @Override
      public ErrorStateModel createNewStateModel(String resourceName, String partitionName) {
        return new ErrorStateModel();
      }
    };

    D2AnnouncingStateModelFactory<ErrorStateModel> errorWrappingFactory =
        new D2AnnouncingStateModelFactory<>(errorFactory, helixD2Announcer, "LEADER");

    StateModel model = errorWrappingFactory.createNewStateModel("resource", "resource_0");
    D2AnnouncingStateModel d2Model = (D2AnnouncingStateModel) model;
    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    try {
      d2Model.onBecomeLeaderFromStandby(msg, ctx);
      Assert.fail("Expected exception from delegate");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Intentional test error"),
          "Should propagate delegate's exception");
    }
  }

  @Test
  public void testOfflineToStandbyTransition() throws Exception {
    StateModel model = factory.createNewStateModel("resource", "resource_0");
    D2AnnouncingStateModel d2Model = (D2AnnouncingStateModel) model;
    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    d2Model.onBecomeStandbyFromOffline(msg, ctx);

    TrackingStateModel delegate = delegateFactory.lastCreated;
    Assert.assertTrue(delegate.becameStandbyFromOffline);
    // STANDBY is not a leader state, so it should not be in leader partitions
    Assert.assertFalse(factory.getCurrentLeaderPartitions().contains("resource_0"));
  }

  @Test
  public void testStandbyToOfflineTransition() throws Exception {
    StateModel model = factory.createNewStateModel("resource", "resource_0");
    D2AnnouncingStateModel d2Model = (D2AnnouncingStateModel) model;
    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    // First make LEADER, then drop to OFFLINE
    d2Model.onBecomeLeaderFromStandby(msg, ctx);
    Assert.assertTrue(factory.getCurrentLeaderPartitions().contains("resource_0"));

    d2Model.onBecomeStandbyFromLeader(msg, ctx);
    d2Model.onBecomeOfflineFromStandby(msg, ctx);

    // Should be fully removed from leader partitions
    Assert.assertFalse(factory.getCurrentLeaderPartitions().contains("resource_0"));
  }

  @Test
  public void testMultiplePartitionsTrackedIndependently() throws Exception {
    StateModel model0 = factory.createNewStateModel("resource", "resource_0");
    StateModel model5 = factory.createNewStateModel("resource", "resource_5");
    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    // Both become LEADER
    ((D2AnnouncingStateModel) model0).onBecomeLeaderFromStandby(msg, ctx);
    ((D2AnnouncingStateModel) model5).onBecomeLeaderFromStandby(msg, ctx);
    Assert.assertEquals(factory.getCurrentLeaderPartitions().size(), 2);

    // Only resource_0 drops to STANDBY
    ((D2AnnouncingStateModel) model0).onBecomeStandbyFromLeader(msg, ctx);
    Assert.assertEquals(factory.getCurrentLeaderPartitions().size(), 1);
    Assert.assertTrue(factory.getCurrentLeaderPartitions().contains("resource_5"));
    Assert.assertFalse(factory.getCurrentLeaderPartitions().contains("resource_0"));
  }

  // ─── Test helpers ────────────────────────────────────────────────────────────

  @StateModelInfo(initialState = "OFFLINE", states = {"LEADER", "STANDBY"})
  static class TrackingStateModel extends StateModel {
    boolean becameLeader = false;
    boolean becameStandby = false;
    boolean becameStandbyFromOffline = false;
    boolean becameOffline = false;

    @Transition(to = "STANDBY", from = "OFFLINE")
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
      becameStandbyFromOffline = true;
    }

    @Transition(to = "LEADER", from = "STANDBY")
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
      becameLeader = true;
    }

    @Transition(to = "STANDBY", from = "LEADER")
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
      becameStandby = true;
    }

    @Transition(to = "OFFLINE", from = "STANDBY")
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
      becameOffline = true;
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    }
  }

  static class TrackingStateModelFactory extends StateModelFactory<TrackingStateModel> {
    TrackingStateModel lastCreated;

    @Override
    public TrackingStateModel createNewStateModel(String resourceName, String partitionName) {
      lastCreated = new TrackingStateModel();
      return lastCreated;
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"LEADER", "STANDBY"})
  static class ErrorStateModel extends StateModel {
    @Transition(to = "LEADER", from = "STANDBY")
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
        throws Exception {
      throw new RuntimeException("Intentional test error");
    }

    @Transition(to = "STANDBY", from = "LEADER")
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    }

    @Transition(to = "STANDBY", from = "OFFLINE")
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    }

    @Transition(to = "OFFLINE", from = "STANDBY")
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    }
  }
}
