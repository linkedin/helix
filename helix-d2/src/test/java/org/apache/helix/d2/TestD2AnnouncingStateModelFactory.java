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
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class TestD2AnnouncingStateModelFactory {

  @Test
  public void testFactoryWrapsDelegate() {
    StateModelFactory<MockLeaderStandbyModel> delegate = new MockStateModelFactory();
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer d2Announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    D2AnnouncingStateModelFactory<MockLeaderStandbyModel> factory =
        new D2AnnouncingStateModelFactory<>(delegate, d2Announcer, "LEADER");

    StateModel wrapped = factory.createNewStateModel("resource", "resource_0");
    Assert.assertNotNull(wrapped);
    Assert.assertTrue(wrapped instanceof D2AnnouncingStateModel,
        "Factory should create D2AnnouncingStateModel instances");
  }

  @Test
  public void testOnPartitionStateChangedTracksLeaders() {
    StateModelFactory<MockLeaderStandbyModel> delegate = new MockStateModelFactory();
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer d2Announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    D2AnnouncingStateModelFactory<MockLeaderStandbyModel> factory =
        new D2AnnouncingStateModelFactory<>(delegate, d2Announcer, "LEADER");

    // Simulate partition becoming LEADER
    factory.onPartitionStateChanged("resource_0", "LEADER");
    Assert.assertTrue(factory.getCurrentLeaderPartitions().contains("resource_0"));

    factory.onPartitionStateChanged("resource_5", "LEADER");
    Assert.assertEquals(factory.getCurrentLeaderPartitions().size(), 2);

    // Simulate partition losing LEADER
    factory.onPartitionStateChanged("resource_0", "STANDBY");
    Assert.assertFalse(factory.getCurrentLeaderPartitions().contains("resource_0"));
    Assert.assertEquals(factory.getCurrentLeaderPartitions().size(), 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullDelegateThrows() {
    D2PartitionAnnouncer mockAnnouncer = mock(D2PartitionAnnouncer.class);
    HelixD2Announcer d2Announcer = new HelixD2Announcer.Builder()
        .addAnnouncer(mockAnnouncer)
        .build();

    new D2AnnouncingStateModelFactory<>(null, d2Announcer, "LEADER");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullAnnouncerThrows() {
    StateModelFactory<MockLeaderStandbyModel> delegate = new MockStateModelFactory();
    new D2AnnouncingStateModelFactory<>(delegate, null, "LEADER");
  }

  // ─── Test helpers ────────────────────────────────────────────────────────────

  @StateModelInfo(initialState = "OFFLINE", states = {"LEADER", "STANDBY"})
  static class MockLeaderStandbyModel extends StateModel {
    boolean leaderCalled = false;
    boolean standbyCalled = false;

    @Transition(to = "STANDBY", from = "OFFLINE")
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    }

    @Transition(to = "LEADER", from = "STANDBY")
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
      leaderCalled = true;
    }

    @Transition(to = "STANDBY", from = "LEADER")
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
      standbyCalled = true;
    }

    @Transition(to = "OFFLINE", from = "STANDBY")
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    }
  }

  static class MockStateModelFactory extends StateModelFactory<MockLeaderStandbyModel> {
    @Override
    public MockLeaderStandbyModel createNewStateModel(String resourceName, String partitionName) {
      return new MockLeaderStandbyModel();
    }
  }
}
