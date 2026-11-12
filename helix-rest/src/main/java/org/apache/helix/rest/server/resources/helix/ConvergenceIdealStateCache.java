package org.apache.helix.rest.server.resources.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.controller.rebalancer.util.WagedValidationUtil;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.zookeeper.data.Stat;

/**
 * The few ideal state fields a resource's convergence answer needs, read again only when the ideal
 * state changes, so a large SEMI_AUTO or CUSTOMIZED ideal state is not parsed on every call.
 */
final class ConvergenceIdealStateCache {
  private static final int MAX_ENTRIES = 10_000;
  private static final long IDLE_MINUTES = 10L;

  /** What the resource endpoint reads from an ideal state. */
  static final class Summary {
    final long _mzxid;
    final int _partitions;
    final boolean _task;
    final boolean _waged;

    Summary(long mzxid, int partitions, boolean task, boolean waged) {
      _mzxid = mzxid;
      _partitions = partitions;
      _task = task;
      _waged = waged;
    }
  }

  private static final class Key {
    final RealmAwareZkClient _zkClient;
    final String _path;

    Key(RealmAwareZkClient zkClient, String path) {
      _zkClient = zkClient;
      _path = path;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Key && ((Key) other)._zkClient == _zkClient
          && ((Key) other)._path.equals(_path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(System.identityHashCode(_zkClient), _path);
    }
  }

  static final ConvergenceIdealStateCache INSTANCE = new ConvergenceIdealStateCache();
  private final Cache<Key, Summary> _cache = CacheBuilder.newBuilder()
      .maximumSize(MAX_ENTRIES)
      .expireAfterAccess(IDLE_MINUTES, TimeUnit.MINUTES)
      .build();

  /**
   * @param stat the stat of the ideal state the caller already read; a summary cached for the same
   *     mzxid is returned without reading the ideal state
   * @return null if the ideal state is gone
   */
  Summary get(RealmAwareZkClient zkClient, HelixDataAccessor accessor, String resource,
      Stat stat) {
    Key key = new Key(zkClient, accessor.keyBuilder().idealStates(resource).getPath());
    Summary summary = _cache.getIfPresent(key);
    if (summary != null && summary._mzxid == stat.getMzxid()) {
      return summary;
    }
    IdealState idealState = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
    if (idealState == null) {
      _cache.invalidate(key);
      return null;
    }
    summary = new Summary(stat.getMzxid(),
        Math.max(idealState.getNumPartitions(), idealState.getPartitionSet().size()),
        TaskConstants.STATE_MODEL_NAME.equals(idealState.getStateModelDefRef()),
        WagedValidationUtil.isWagedEnabled(idealState));
    _cache.put(key, summary);
    return summary;
  }
}
