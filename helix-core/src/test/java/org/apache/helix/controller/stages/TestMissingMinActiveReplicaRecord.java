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
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.model.Partition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMissingMinActiveReplicaRecord {
  private static final String RESOURCE = "resource";
  private static final Partition PARTITION = new Partition("partition");
  private static final Set<String> ACTIVE = ImmutableSet.of("MASTER", "SLAVE");

  @Test
  public void testExtraRecoveryAndActiveStateChangeDoNotChangeRestoringReplica() {
    MissingMinActiveReplicaRecord record = new MissingMinActiveReplicaRecord(1000);
    record.observeInitialState(Collections.singletonMap("a", "MASTER"), ACTIVE, 2);
    CurrentStateOutput output = new CurrentStateOutput();
    output.setCurrentState(RESOURCE, PARTITION, "a", "SLAVE");
    output.setEndTime(RESOURCE, PARTITION, "a", 5000L);
    output.setCurrentState(RESOURCE, PARTITION, "b", "SLAVE");
    output.setEndTime(RESOURCE, PARTITION, "b", 3000L);
    output.setCurrentState(RESOURCE, PARTITION, "c", "MASTER");
    output.setEndTime(RESOURCE, PARTITION, "c", 4000L);

    Assert.assertEquals(record.findRestoringInstance(output, RESOURCE, PARTITION, ACTIVE, 2), "b");
  }

  @Test
  public void testInitiallyActiveReplicaThatDropsMustRecoverAgain() {
    MissingMinActiveReplicaRecord record = new MissingMinActiveReplicaRecord(1000);
    record.observeInitialState(Collections.singletonMap("a", "MASTER"), ACTIVE, 2);
    record.observeInitialState(Collections.singletonMap("a", "OFFLINE"), ACTIVE, 2);
    CurrentStateOutput output = new CurrentStateOutput();
    output.setCurrentState(RESOURCE, PARTITION, "a", "MASTER");
    output.setEndTime(RESOURCE, PARTITION, "a", 5000L);
    output.setCurrentState(RESOURCE, PARTITION, "b", "SLAVE");
    output.setEndTime(RESOURCE, PARTITION, "b", 3000L);

    Assert.assertEquals(record.findRestoringInstance(output, RESOURCE, PARTITION, ACTIVE, 2), "a");
  }

  @Test
  public void testLastRequiredReplicaRestoresMinimum() {
    MissingMinActiveReplicaRecord record = new MissingMinActiveReplicaRecord(1000);
    record.observeInitialState(Collections.singletonMap("a", "MASTER"), ACTIVE, 3);
    CurrentStateOutput output = new CurrentStateOutput();
    output.setCurrentState(RESOURCE, PARTITION, "a", "MASTER");
    output.setCurrentState(RESOURCE, PARTITION, "b", "SLAVE");
    output.setEndTime(RESOURCE, PARTITION, "b", 3000L);
    output.setCurrentState(RESOURCE, PARTITION, "c", "SLAVE");
    output.setEndTime(RESOURCE, PARTITION, "c", 4000L);

    Assert.assertEquals(record.findRestoringInstance(output, RESOURCE, PARTITION, ACTIVE, 3), "c");
    Assert.assertNull(record.findRestoringInstance(output, RESOURCE, PARTITION, ACTIVE, 2),
        "A changed minimum must not fabricate recovery timing");
    output.setEndTime(RESOURCE, PARTITION, "c", -1L);
    Assert.assertNull(record.findRestoringInstance(output, RESOURCE, PARTITION, ACTIVE, 3));
  }
}
