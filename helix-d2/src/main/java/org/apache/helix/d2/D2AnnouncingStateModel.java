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

import java.lang.reflect.Method;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StateModel} wrapper that intercepts state transitions for D2 announcement.
 *
 * <p>For the {@code STANDBY → LEADER} transition, the delegate is called first, then D2 is
 * notified. This ensures the partition is ready before D2 starts routing to it.</p>
 *
 * <p>For the {@code LEADER → STANDBY} transition, D2 is notified first (deannounce), then
 * the delegate is called. This ensures D2 stops routing before the partition is torn down.</p>
 *
 * <p>For all other transitions, the delegate is called and then the factory is notified
 * (which will remove the partition from D2 if it was previously a leader).</p>
 *
 * <p>This class targets the {@code LeaderStandby} state model. The transitions covered are:
 * {@code OFFLINE↔STANDBY}, {@code STANDBY↔LEADER}, and {@code OFFLINE→DROPPED}.</p>
 */
@StateModelInfo(initialState = "OFFLINE", states = {"LEADER", "STANDBY"})
class D2AnnouncingStateModel extends StateModel {

  private static final Logger LOG = LoggerFactory.getLogger(D2AnnouncingStateModel.class);

  private final StateModel delegate;
  private final String partitionName;
  private final D2AnnouncingStateModelFactory<?> factory;

  D2AnnouncingStateModel(StateModel delegate, String partitionName,
      D2AnnouncingStateModelFactory<?> factory) {
    this.delegate = delegate;
    this.partitionName = partitionName;
    this.factory = factory;
  }

  @Transition(to = "LEADER", from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    // 1. Delegate first — ensure partition is ready before announcing to D2
    invokeTransition("STANDBY", "LEADER", message, context);
    // 2. Announce to D2
    factory.onPartitionStateChanged(partitionName, "LEADER");
  }

  @Transition(to = "STANDBY", from = "LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context)
      throws Exception {
    // 1. Deannounce from D2 FIRST — stop routing before tearing down
    factory.onPartitionStateChanged(partitionName, "STANDBY");
    // 2. Delegate
    invokeTransition("LEADER", "STANDBY", message, context);
  }

  @Transition(to = "STANDBY", from = "OFFLINE")
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context)
      throws Exception {
    invokeTransition("OFFLINE", "STANDBY", message, context);
    factory.onPartitionStateChanged(partitionName, "STANDBY");
  }

  @Transition(to = "OFFLINE", from = "STANDBY")
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context)
      throws Exception {
    factory.onPartitionStateChanged(partitionName, "OFFLINE");
    invokeTransition("STANDBY", "OFFLINE", message, context);
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
      throws Exception {
    factory.onPartitionStateChanged(partitionName, "DROPPED");
    invokeTransition("OFFLINE", "DROPPED", message, context);
  }

  /**
   * Invoke the corresponding @Transition method on the delegate state model using reflection.
   *
   * <p>Looks for a method annotated with {@code @Transition(from=fromState, to=toState)}
   * on the delegate and invokes it. If no matching method is found, logs a warning
   * (the delegate may not handle all transitions).</p>
   */
  private void invokeTransition(String fromState, String toState,
      Message message, NotificationContext context) throws Exception {
    Method transitionMethod = findTransitionMethod(delegate.getClass(), fromState, toState);
    if (transitionMethod != null) {
      transitionMethod.setAccessible(true);
      try {
        transitionMethod.invoke(delegate, message, context);
      } catch (java.lang.reflect.InvocationTargetException e) {
        // Unwrap and rethrow the actual exception from the delegate
        Throwable cause = e.getCause();
        if (cause instanceof Exception) {
          throw (Exception) cause;
        }
        throw new RuntimeException("Delegate transition failed: " + fromState + " → " + toState,
            cause);
      }
    } else {
      LOG.warn("No @Transition method found on delegate {} for {} → {}",
          delegate.getClass().getSimpleName(), fromState, toState);
    }
  }

  /**
   * Find a method annotated with @Transition matching the given from/to states.
   */
  private static Method findTransitionMethod(Class<?> clazz, String fromState, String toState) {
    for (Method method : clazz.getMethods()) {
      Transition annotation = method.getAnnotation(Transition.class);
      if (annotation != null
          && annotation.from().equals(fromState)
          && annotation.to().equals(toState)) {
        return method;
      }
    }
    // Also check declared methods (for non-public methods in the class hierarchy)
    Class<?> current = clazz;
    while (current != null && current != Object.class) {
      for (Method method : current.getDeclaredMethods()) {
        Transition annotation = method.getAnnotation(Transition.class);
        if (annotation != null
            && annotation.from().equals(fromState)
            && annotation.to().equals(toState)) {
          return method;
        }
      }
      current = current.getSuperclass();
    }
    return null;
  }
}
