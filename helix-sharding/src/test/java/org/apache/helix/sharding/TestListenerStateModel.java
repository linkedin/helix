package org.apache.helix.sharding;

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
import java.util.List;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.sharding.internal.ListenerStateModel;
import org.apache.helix.sharding.internal.ListenerStateModelFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


/**
 * Tests for {@link ListenerStateModel} and {@link ListenerStateModelFactory}.
 */
public class TestListenerStateModel {

  @Test
  public void testAllTransitionsCallListener() throws Exception {
    List<String> transitions = new ArrayList<>();
    ShardingStateTransitionListener listener = (partition, from, to) ->
        transitions.add(partition + ":" + from + "->" + to);

    ListenerStateModelFactory factory = new ListenerStateModelFactory(listener);
    ListenerStateModel model = factory.createNewStateModel("res", "res_0");

    Message msg = mock(Message.class);
    NotificationContext ctx = mock(NotificationContext.class);

    // OFFLINE → STANDBY
    model.onBecomeStandbyFromOffline(msg, ctx);
    Assert.assertEquals(transitions.get(0), "res_0:OFFLINE->STANDBY");

    // STANDBY → LEADER
    model.onBecomeLeaderFromStandby(msg, ctx);
    Assert.assertEquals(transitions.get(1), "res_0:STANDBY->LEADER");

    // LEADER → STANDBY
    model.onBecomeStandbyFromLeader(msg, ctx);
    Assert.assertEquals(transitions.get(2), "res_0:LEADER->STANDBY");

    // STANDBY → OFFLINE
    model.onBecomeOfflineFromStandby(msg, ctx);
    Assert.assertEquals(transitions.get(3), "res_0:STANDBY->OFFLINE");

    // OFFLINE → DROPPED
    model.onBecomeDroppedFromOffline(msg, ctx);
    Assert.assertEquals(transitions.get(4), "res_0:OFFLINE->DROPPED");

    Assert.assertEquals(transitions.size(), 5);
  }

  @Test
  public void testFactoryCreatesDistinctModels() {
    ShardingStateTransitionListener listener = (p, f, t) -> {};
    ListenerStateModelFactory factory = new ListenerStateModelFactory(listener);

    ListenerStateModel m1 = factory.createNewStateModel("res", "res_0");
    ListenerStateModel m2 = factory.createNewStateModel("res", "res_1");

    Assert.assertNotSame(m1, m2, "Factory should create distinct instances");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFactoryNullListenerThrows() {
    new ListenerStateModelFactory(null);
  }
}
