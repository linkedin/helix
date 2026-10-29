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
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class TestPersistAssignmentStageSchedulerQueue {
  @Test
  public void testSchedulerMessageMetadataIsNotOverwritten() throws Exception {
    String resourceName = "queue";
    String partitionName = "queue_0";
    String instanceName = "localhost_0";
    IdealState idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setStateModelDefRef(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE);
    idealState.setBucketSize(DefaultSchedulerMessageHandlerFactory.TASKQUEUE_BUCKET_NUM);
    idealState.setPreferenceList(partitionName, Collections.singletonList(instanceName));
    Map<String, String> messageFields = new HashMap<>();
    messageFields.put(DefaultSchedulerMessageHandlerFactory.CONTROLLER_MSG_ID, "controllerMessage");
    messageFields.put(Message.Attributes.MSG_ID.name(), "taskMessage");
    messageFields.put(Message.Attributes.TGT_NAME.name(), instanceName);
    messageFields.put(instanceName, "COMPLETED");
    idealState.getRecord().setMapField(partitionName, messageFields);

    Resource resource = new Resource(resourceName);
    resource.addPartition(partitionName);
    BestPossibleStateOutput assignment = new BestPossibleStateOutput();
    assignment.setState(resourceName, new Partition(partitionName), instanceName, "COMPLETED");

    ResourceControllerDataProvider cache = Mockito.mock(ResourceControllerDataProvider.class);
    Mockito.when(cache.getIdealState(resourceName)).thenReturn(idealState);
    HelixDataAccessor accessor = Mockito.mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder("cluster");
    Mockito.when(accessor.keyBuilder()).thenReturn(keyBuilder);
    HelixManager manager = Mockito.mock(HelixManager.class);
    Mockito.when(manager.getHelixDataAccessor()).thenReturn(accessor);

    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.RESOURCES.name(),
        Collections.singletonMap(resourceName, resource));
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), assignment);
    new PersistAssignmentStage().execute(event);

    Mockito.verify(accessor, Mockito.never()).updateProperty(
        Mockito.eq(keyBuilder.idealStates(resourceName)), Mockito.any(), Mockito.eq(idealState));
  }
}
