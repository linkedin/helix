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

package org.apache.helix.guardrail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestReusingWagedAssignmentProvider {
  private AtomicInteger _computed;
  private WagedAssignmentProvider _provider;

  @BeforeMethod
  public void beforeMethod() {
    AtomicInteger computed = new AtomicInteger();
    _computed = computed;
    // Changes the cluster config it is given, as the WAGED util does.
    _provider = new ReusingWagedAssignmentProvider(
        (clusterConfig, instanceConfigs, liveInstances, idealStates, resourceConfigs) -> {
          computed.incrementAndGet();
          if (instanceConfigs.stream().anyMatch(c -> c.getInstanceName().equals("fail"))) {
            throw new IllegalStateException("no assignment");
          }
          clusterConfig.setGlobalRebalanceAsyncMode(false);
          return Collections.singletonMap("db", new ResourceAssignment("db"));
        });
  }

  @Test
  public void testComputesEachDistinctInputOnceAmongTheLastTwo() throws Exception {
    Map<String, ResourceAssignment> baseline = compute("a", "b");
    Assert.assertSame(compute("a", "b"), baseline);
    Assert.assertSame(compute("b", "a"), baseline);
    compute("a");
    // The baseline was used last, so it outlives the older of the two candidates.
    Assert.assertSame(compute("a", "b"), baseline);
    compute("b");
    Assert.assertSame(compute("a", "b"), baseline);
    Assert.assertEquals(_computed.get(), 3);

    InstanceConfig changed = new InstanceConfig("a");
    changed.setInstanceEnabled(false);
    _provider.computeTargetAssignment(new ClusterConfig("c"),
        Arrays.asList(changed, new InstanceConfig("b")), Arrays.asList("a", "b"),
        Collections.<IdealState>emptyList(), Collections.emptyList());
    Assert.assertEquals(_computed.get(), 4);
  }

  @Test
  public void testFailureIsNotRemembered() throws Exception {
    for (int i = 0; i < 2; i++) {
      try {
        compute("fail");
        Assert.fail("Expected the computation to fail.");
      } catch (IllegalStateException expected) {
        // Computed again rather than remembered.
      }
    }
    Assert.assertEquals(_computed.get(), 2);
  }

  private Map<String, ResourceAssignment> compute(String... instances) throws Exception {
    List<InstanceConfig> configs = new ArrayList<>();
    for (String instance : instances) {
      configs.add(new InstanceConfig(instance));
    }
    return _provider.computeTargetAssignment(new ClusterConfig("c"), configs,
        Arrays.asList(instances), Collections.<IdealState>emptyList(), Collections.emptyList());
  }
}
