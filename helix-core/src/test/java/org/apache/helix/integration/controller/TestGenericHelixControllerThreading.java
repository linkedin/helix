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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.controller.GenericHelixController;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestGenericHelixControllerThreading extends ZkUnitTestBase {

  private static final String EVENT_PROCESS_THREAD_NAME_PREFIX =
      "GerenricHelixController-event_process";

  //    Temporarily disabling the test as it's not stable when running under the entire mvn test suite.
  //    Some other crashed tests might cause this one to fail as controllers might not be gracefully
  //    shutdown
  @Test(enabled = false)
  public void testGenericHelixControllerThreadCount() throws Exception {
    System.out.println("testGenericHelixControllerThreadCount STARTs");
    final int numControllers = 100;
    ArrayList<GenericHelixController> controllers = createHelixControllers(numControllers);
    Assert.assertEquals(getThreadCountByNamePrefix(EVENT_PROCESS_THREAD_NAME_PREFIX), numControllers * 2);

    int remainingExpectedEventProcessThreadsCount = numControllers * 2;
    for (GenericHelixController ctrl : controllers) {
      ctrl.shutdown();
      remainingExpectedEventProcessThreadsCount -= 2;
      Assert.assertEquals(getThreadCountByNamePrefix(EVENT_PROCESS_THREAD_NAME_PREFIX),
          remainingExpectedEventProcessThreadsCount);
    }
    System.out.println("testGenericHelixControllerThreadCount ENDs");
  }

  // Each pipeline thread is an inner class of its controller, so one that outlives shutdown keeps the whole
  // controller reachable, and a distributed controller makes a new one every time it regains a cluster.
  @Test
  public void testShutdownStopsEveryPipelineThread() throws Exception {
    GenericHelixController controller = new GenericHelixController("TestShutdownStopsEveryPipelineThread");
    Map<String, Thread> threads = new LinkedHashMap<>();
    for (String name : new String[] {"_eventThread", "_taskEventThread", "_managementModeEventThread"}) {
      Field field = GenericHelixController.class.getDeclaredField(name);
      field.setAccessible(true);
      threads.put(name, (Thread) field.get(controller));
    }
    threads.forEach((name, thread) -> Assert.assertTrue(thread.isAlive(), name + " did not start"));
    controller.shutdown();
    threads.forEach((name, thread) -> Assert.assertFalse(thread.isAlive(), name + " outlived its controller's shutdown"));
  }

  private ArrayList<GenericHelixController> createHelixControllers(int count) {
    ArrayList<GenericHelixController> ret = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ret.add(new GenericHelixController());
    }
    return ret;
  }

  private int getThreadCountByNamePrefix(String threadNamePrefix) {
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    int eventThreadCount = 0;
    for (Thread t : threadSet) {
      if (t.getName().startsWith(threadNamePrefix)) {
        eventThreadCount += 1;
      }
    }
    return eventThreadCount;
  }

}
