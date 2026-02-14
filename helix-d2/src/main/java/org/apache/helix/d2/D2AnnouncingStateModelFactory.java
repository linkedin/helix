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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StateModelFactory} wrapper that intercepts state transitions and updates
 * D2 announcements via a {@link HelixD2Announcer}.
 *
 * <p>This factory wraps a delegate factory. For each partition, it creates a
 * {@link D2AnnouncingStateModel} that delegates actual state transition logic to the
 * user's state model and additionally tracks LEADER state changes for D2 announcements.</p>
 *
 * <p>Thread-safe: maintains a concurrent set of current leader partition names and
 * notifies the {@link HelixD2Announcer} on each change.</p>
 *
 * @param <T> The type of the delegate state model.
 */
public class D2AnnouncingStateModelFactory<T extends StateModel>
    extends StateModelFactory<StateModel> {

  private static final Logger LOG = LoggerFactory.getLogger(D2AnnouncingStateModelFactory.class);

  private final StateModelFactory<T> delegate;
  private final HelixD2Announcer d2Announcer;
  private final String leaderState;
  private final Set<String> currentLeaderPartitions = ConcurrentHashMap.newKeySet();

  /**
   * Create a new D2-announcing state model factory.
   *
   * @param delegate     The underlying state model factory that handles actual business logic.
   * @param d2Announcer  The D2 announcer to notify on leader partition changes.
   * @param leaderState  The state name that represents "leader" (typically "LEADER").
   */
  public D2AnnouncingStateModelFactory(StateModelFactory<T> delegate,
      HelixD2Announcer d2Announcer, String leaderState) {
    if (delegate == null) {
      throw new IllegalArgumentException("Delegate factory must not be null");
    }
    if (d2Announcer == null) {
      throw new IllegalArgumentException("D2 announcer must not be null");
    }
    if (leaderState == null || leaderState.isEmpty()) {
      throw new IllegalArgumentException("Leader state must not be null or empty");
    }
    this.delegate = delegate;
    this.d2Announcer = d2Announcer;
    this.leaderState = leaderState;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    T delegateModel = delegate.createNewStateModel(resourceName, partitionName);
    return new D2AnnouncingStateModel(delegateModel, partitionName, this);
  }

  /**
   * Called by {@link D2AnnouncingStateModel} after a state transition completes.
   * Updates the leader partition tracking set and notifies the D2 announcer.
   *
   * @param partitionName The partition that transitioned.
   * @param toState       The new state of the partition.
   */
  void onPartitionStateChanged(String partitionName, String toState) {
    if (leaderState.equals(toState)) {
      currentLeaderPartitions.add(partitionName);
    } else {
      currentLeaderPartitions.remove(partitionName);
    }

    LOG.debug("Partition {} → {}. Current leader partitions: {}",
        partitionName, toState, currentLeaderPartitions.size());

    d2Announcer.onLeaderPartitionsChanged(
        Collections.unmodifiableSet(currentLeaderPartitions));
  }

  /**
   * @return An unmodifiable view of the current leader partition names (for testing).
   */
  public Set<String> getCurrentLeaderPartitions() {
    return Collections.unmodifiableSet(currentLeaderPartitions);
  }

  /**
   * @return The delegate factory (for testing).
   */
  StateModelFactory<T> getDelegate() {
    return delegate;
  }
}
