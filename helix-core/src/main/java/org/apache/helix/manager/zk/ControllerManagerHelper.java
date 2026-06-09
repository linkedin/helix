package org.apache.helix.manager.zk;

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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.messaging.handling.MultiTypeMessageHandlerFactory;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * helper class for controller manager
 */
public class ControllerManagerHelper {
  private static Logger LOG = LoggerFactory.getLogger(ControllerManagerHelper.class);

  // Each per-instance listener init involves ZK roundtrips for watch subscription (~200ms).
  // With 300 instances * 4 listener types = 1200 sequential registrations: ~240 sec.
  // 20 parallel threads: ~12 sec. All threads share one ZkClient (one ZK connection).
  private static final int INSTANCE_LISTENER_PARALLELISM = 20;

  final HelixManager _manager;
  final DefaultMessagingService _messagingService;
  final List<HelixTimerTask> _controllerTimerTasks;

  public ControllerManagerHelper(HelixManager manager, List<HelixTimerTask> controllerTimerTasks) {
    _manager = manager;
    _messagingService = (DefaultMessagingService) manager.getMessagingService();
    _controllerTimerTasks = controllerTimerTasks;
  }

  public void addListenersToController(GenericHelixController controller) {
    try {
      /**
       * setup controller message listener and register message handlers
       */
      _manager.addControllerMessageListener(_messagingService.getExecutor());
      MultiTypeMessageHandlerFactory defaultControllerMsgHandlerFactory =
          new DefaultControllerMessageHandlerFactory();
      for (String type : defaultControllerMsgHandlerFactory.getMessageTypes()) {
        _messagingService.getExecutor()
            .registerMessageHandlerFactory(type, defaultControllerMsgHandlerFactory);
      }

      MultiTypeMessageHandlerFactory defaultSchedulerMsgHandlerFactory =
          new DefaultSchedulerMessageHandlerFactory(_manager);
      for (String type : defaultSchedulerMsgHandlerFactory.getMessageTypes()) {
        _messagingService.getExecutor()
            .registerMessageHandlerFactory(type, defaultSchedulerMsgHandlerFactory);
      }

      MultiTypeMessageHandlerFactory defaultParticipantErrorMessageHandlerFactory =
          new DefaultParticipantErrorMessageHandlerFactory(_manager);

      for (String type : defaultParticipantErrorMessageHandlerFactory.getMessageTypes()) {
        _messagingService.getExecutor()
            .registerMessageHandlerFactory(type, defaultParticipantErrorMessageHandlerFactory);
      }

      /**
       * setup generic-controller
       */
      _manager.addControllerListener(controller);
      _manager.addInstanceConfigChangeListener(controller);
      _manager.addResourceConfigChangeListener(controller);
      _manager.addClusterfigChangeListener(controller);
      _manager.addCustomizedStateConfigChangeListener(controller);
      _manager.addLiveInstanceChangeListener(controller);
      _manager.addIdealStateChangeListener(controller);

      // Register per-instance listeners (current-state, message, customized-state-root) in parallel.
      // These were deferred during the INIT callback in checkLiveInstancesObservation() to avoid
      // sequential ZK roundtrips while holding the manager lock.
      GenericHelixController.PendingInstanceListeners pending =
          controller.takePendingInstanceListeners();
      if (pending != null && !pending.isEmpty()) {
        registerInstanceListenersInParallel(controller, pending);
      }
    } catch (ZkInterruptedException e) {
      LOG.warn("zk connection is interrupted during HelixManagerMain.addListenersToController(). "
          + e);
    } catch (Exception e) {
      LOG.error("Error when creating HelixManagerContollerMonitor", e);
    }
  }

  private void registerInstanceListenersInParallel(GenericHelixController controller,
      GenericHelixController.PendingInstanceListeners pending) {
    long start = System.currentTimeMillis();
    int sessionCount = pending.getSessionToInstance().size();
    int instanceCount = pending.getNewInstances().size();
    LOG.info("Registering per-instance listeners in parallel for cluster: {} "
            + "(sessions: {}, instances: {})",
        _manager.getClusterName(), sessionCount, instanceCount);

    List<Runnable> tasks = new ArrayList<>();

    for (Map.Entry<String, String> entry : pending.getSessionToInstance().entrySet()) {
      String session = entry.getKey();
      String instanceName = entry.getValue();
      tasks.add(() -> {
        try {
          _manager.addCurrentStateChangeListener(controller, instanceName, session);
          _manager.addTaskCurrentStateChangeListener(controller, instanceName, session);
          LOG.info(_manager.getInstanceName() + " added current-state listener for instance: "
              + instanceName + ", session: " + session + ", listener: " + controller);
        } catch (Exception e) {
          LOG.error("Failed to register current-state listeners for instance: " + instanceName
              + " with session: " + session, e);
        }
      });
    }

    for (String instance : pending.getNewInstances()) {
      tasks.add(() -> {
        try {
          _manager.addMessageListener(controller, instance);
          _manager.addCustomizedStateRootChangeListener(controller, instance);
          LOG.info(_manager.getInstanceName() + " added message/customizedStateRoot listener for "
              + instance + ", listener: " + controller);
        } catch (Exception e) {
          LOG.error("Failed to register message/customizedStateRoot listeners for instance: "
              + instance, e);
        }
      });
    }

    if (tasks.size() == 1) {
      tasks.get(0).run();
    } else {
      int poolSize = Math.min(tasks.size(), INSTANCE_LISTENER_PARALLELISM);
      ExecutorService executor = Executors.newFixedThreadPool(poolSize, r -> {
        Thread t = new Thread(r, "registerInstanceListener-" + _manager.getClusterName());
        t.setDaemon(true);
        return t;
      });
      try {
        List<Future<?>> futures = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
          futures.add(executor.submit(task));
        }
        for (Future<?> future : futures) {
          try {
            future.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while registering per-instance listeners", e);
            break;
          } catch (ExecutionException e) {
            LOG.error("Failed to register per-instance listener", e.getCause());
          }
        }
      } finally {
        executor.shutdownNow();
      }
    }

    LOG.info("Registered per-instance listeners in parallel for cluster: {}, "
            + "sessions: {}, instances: {}, took: {}ms",
        _manager.getClusterName(), sessionCount, instanceCount,
        System.currentTimeMillis() - start);
  }

  public void removeListenersFromController(GenericHelixController controller) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(_manager.getClusterName());
    /**
     * reset generic-controller
     */
    _manager.removeListener(keyBuilder.idealStates(), controller);
    _manager.removeListener(keyBuilder.liveInstances(), controller);
    _manager.removeListener(keyBuilder.customizedStateConfig(), controller);
    _manager.removeListener(keyBuilder.clusterConfig(), controller);
    _manager.removeListener(keyBuilder.resourceConfigs(), controller);
    _manager.removeListener(keyBuilder.instanceConfigs(), controller);
    _manager.removeListener(keyBuilder.controller(), controller);

    /**
     * reset controller message listener and unregister all message handlers
     */
    _manager.removeListener(keyBuilder.controllerMessages(), _messagingService.getExecutor());
  }

  public void startControllerTimerTasks() {
    for (HelixTimerTask task : _controllerTimerTasks) {
      task.start();
    }
  }

  public void stopControllerTimerTasks() {
    for (HelixTimerTask task : _controllerTimerTasks) {
      task.stop();
    }
  }

}
