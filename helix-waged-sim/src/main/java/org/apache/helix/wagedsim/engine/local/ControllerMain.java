package org.apache.helix.wagedsim.engine.local;

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

import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;

/**
 * Entry point of the controller process a local cluster starts. Constraint weights come from
 * {@code soft-constraint-weight.properties} at the front of its classpath, as in production.
 * Arguments: ZooKeeper address, cluster name.
 */
public final class ControllerMain {
  private ControllerMain() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: ControllerMain <zkAddress> <cluster>");
      System.exit(2);
    }
    HelixManager manager = HelixControllerMain.startHelixController(args[0], args[1], "waged-sim-controller",
        HelixControllerMain.STANDALONE);
    Runtime.getRuntime().addShutdownHook(new Thread(manager::disconnect));
    // The parent holds this process's stdin; end of input means the parent is gone, so stop too.
    Thread watchdog = new Thread(() -> {
      try {
        while (System.in.read() >= 0) {
          // Ignore input.
        }
      } catch (java.io.IOException ignored) {
        // Treat as end of input.
      }
      System.exit(0);
    }, "parent-watchdog");
    watchdog.setDaemon(true);
    watchdog.start();
    System.out.println("controller started for " + args[1] + " at " + args[0]);
    Thread.currentThread().join();
  }
}
