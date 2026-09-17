package org.apache.helix.model;

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

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class TestLegacyResourceGroupMetadata {
  @Test
  public void testIdealStateRetainsLegacyGroupName() {
    IdealState idealState = new IdealState("resource");
    Assert.assertNull(idealState.getResourceGroupName());
    idealState.getRecord().setSimpleField("RESOURCE_GROUP_NAME", "legacyGroup");
    Assert.assertEquals(idealState.getResourceGroupName(), "legacyGroup");
  }

  @Test
  public void testExternalViewRetainsLegacyMetadata() {
    ExternalView externalView = new ExternalView("resource");
    Assert.assertNull(externalView.getResourceGroupName());
    Assert.assertFalse(externalView.isGroupRoutingEnabled());
    externalView.getRecord().setSimpleField("RESOURCE_GROUP_NAME", "legacyGroup");
    externalView.getRecord().setBooleanField("GROUP_ROUTING_ENABLED", true);
    Assert.assertEquals(externalView.getResourceGroupName(), "legacyGroup");
    Assert.assertTrue(externalView.isGroupRoutingEnabled());
    externalView.getRecord().setBooleanField("GROUP_ROUTING_ENABLED", false);
    Assert.assertFalse(externalView.isGroupRoutingEnabled());
  }

  @Test
  public void testMessageRetainsLegacyGroupNameWriter() {
    Message message = new Message(new ZNRecord("message"));
    message.setResourceGroupName("legacyGroup");
    Assert.assertEquals(message.getRecord().getSimpleField("RESOURCE_GROUP_NAME"), "legacyGroup");
    message.setResourceGroupName(null);
    Assert.assertNull(message.getRecord().getSimpleField("RESOURCE_GROUP_NAME"));
  }
}
