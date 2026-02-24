package org.apache.helix.common.caches;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Maps;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataCache<T extends HelixProperty> {
  private static Logger LOG = LoggerFactory.getLogger(AbstractDataCache.class.getName());
  public static final String UNKNOWN_CLUSTER = "UNKNOWN_CLUSTER";
  public static final String UNKNOWN_EVENT_ID = "NO_ID";
  public static final String UNKNOWN_PIPELINE = "UNKNOWN_PIPELINE";

  protected ControlContextProvider _controlContextProvider;

  // Paths that have received a ZK watch notification since the last refresh.
  // Populated by _watchListener on the ZkClient event thread; consumed (and cleared per-path)
  // inside refreshProperties on the controller pipeline thread.
  private final Set<String> _dirtyPaths = ConcurrentHashMap.newKeySet();

  // Paths that currently have an active ZK data watch subscription.
  // Maintained entirely on the controller pipeline thread inside refreshProperties,
  // so no concurrent modification occurs between subscribe/unsubscribe calls.
  private final Set<String> _watchedPaths = ConcurrentHashMap.newKeySet();

  // Single shared listener instance registered on every watched path.
  // handleDataChange / handleDataDeleted are called on the ZkClient event thread;
  // ConcurrentHashMap.newKeySet ensures the add is thread-safe.
  private final IZkDataListener _watchListener = new IZkDataListener() {
    @Override
    public void handleDataChange(String dataPath, Object data) {
      _dirtyPaths.add(dataPath);
    }

    @Override
    public void handleDataDeleted(String dataPath) {
      // Mark dirty so the next refresh detects the removal via PopulateParticipantKeys /
      // genSelectiveUpdateInput and skips the stale entry.
      _dirtyPaths.add(dataPath);
    }
  };

  public AbstractDataCache(ControlContextProvider controlContextProvider) {
    _controlContextProvider = controlContextProvider;
  }

  /**
   * Selectively fetch Helix Properties from ZK by comparing the version of the locally cached
   * entry with the one on ZK. Entries that have not changed since the last refresh are reused
   * directly from the cache without any ZK round-trip.
   *
   * <p>Watch-based optimization: after a property is loaded for the first time, a ZK data watch
   * is registered on its path. Subsequent refreshes can skip the {@code getPropertyStats} call
   * for that path as long as no watch notification has arrived. When ZK fires the watch (data
   * changed or deleted), the path is added to {@code _dirtyPaths} and the next refresh
   * unconditionally reloads it from ZK. This eliminates the dominant cost of calling
   * {@code getPropertyStats} for every cached key on every controller pipeline cycle.
   *
   * @param accessor         the HelixDataAccessor
   * @param reloadKeysIn     keys that must be reloaded (new entries not yet in the cache)
   * @param cachedKeys       keys already present in the local cache
   * @param cachedPropertyMap cached map of PropertyKey → property object
   * @param reloadedKeys     output set; populated with all keys that were actually reloaded
   * @return updated properties map
   */
  protected Map<PropertyKey, T> refreshProperties(
      HelixDataAccessor accessor, Set<PropertyKey> reloadKeysIn, List<PropertyKey> cachedKeys,
      Map<PropertyKey, T> cachedPropertyMap, Set<PropertyKey> reloadedKeys) {

    List<PropertyKey> reloadKeys = new ArrayList<>(reloadKeysIn);
    Map<PropertyKey, T> refreshedPropertyMap = Maps.newHashMap();
    BaseDataAccessor<ZNRecord> baseAccessor = accessor.getBaseDataAccessor();

    // Partition cached keys into three buckets:
    //   (1) dirty    – watch fired since last refresh → must reload from ZK
    //   (2) clean    – watch active, no notification  → reuse without any ZK call
    //   (3) no-watch – first time or watch unregistered → fall back to stat check
    //
    // Bucketed properties are always stat-checked regardless of watch state because a watch
    // on the parent ZNode does not cover changes to child bucket ZNodes.
    List<PropertyKey> statCheckKeys = new ArrayList<>();
    for (PropertyKey key : cachedKeys) {
      String path = key.getPath();
      if (_dirtyPaths.remove(path)) {
        // Watch fired → unconditional reload; re-watch happens after successful getProperty.
        reloadKeys.add(key);
      } else if (_watchedPaths.contains(path)) {
        T property = cachedPropertyMap.get(key);
        if (property != null && property.getBucketSize() == 0) {
          // Active watch, no change notification, non-bucketed → safe to reuse with no ZK call.
          refreshedPropertyMap.put(key, property);
        } else {
          // Bucketed property: fall through to stat check.
          statCheckKeys.add(key);
        }
      } else {
        // No watch registered yet → stat check, then register watch on success.
        statCheckKeys.add(key);
      }
    }

    // Call getPropertyStats only for the (typically small) set of un-watched keys.
    if (!statCheckKeys.isEmpty()) {
      List<HelixProperty.Stat> stats = accessor.getPropertyStats(statCheckKeys);
      for (int i = 0; i < statCheckKeys.size(); i++) {
        PropertyKey key = statCheckKeys.get(i);
        HelixProperty.Stat stat = stats.get(i);
        if (stat != null) {
          T property = cachedPropertyMap.get(key);
          if (property != null && property.getBucketSize() == 0 && property.getStat().equals(stat)) {
            refreshedPropertyMap.put(key, property);
            // Stat confirms data is current; register a watch so future refreshes skip this path.
            subscribeWatch(baseAccessor, key.getPath());
          } else {
            reloadKeys.add(key);
          }
        } else {
          LOG.warn("stat is null for key: " + key);
          reloadKeys.add(key);
        }
      }
    }

    reloadedKeys.clear();
    reloadedKeys.addAll(reloadKeys);

    List<T> reloadedProperty = accessor.getProperty(reloadKeys, true);
    Iterator<PropertyKey> csKeyIter = reloadKeys.iterator();
    for (T property : reloadedProperty) {
      PropertyKey key = csKeyIter.next();
      if (property != null) {
        refreshedPropertyMap.put(key, property);
        // Register (or re-register) a watch after a successful load so the next change is detected.
        subscribeWatch(baseAccessor, key.getPath());
      } else {
        LOG.warn("znode is null for key: " + key);
      }
    }

    // Unsubscribe watches for paths that are no longer present in the refreshed cache.
    // This covers instance removals, session expirations, and resource deletions.
    Set<String> activePaths = new HashSet<>(refreshedPropertyMap.size() * 2);
    for (PropertyKey key : refreshedPropertyMap.keySet()) {
      activePaths.add(key.getPath());
    }
    Iterator<String> watchIter = _watchedPaths.iterator();
    while (watchIter.hasNext()) {
      String watchedPath = watchIter.next();
      if (!activePaths.contains(watchedPath)) {
        baseAccessor.unsubscribeDataChanges(watchedPath, _watchListener);
        watchIter.remove();
        _dirtyPaths.remove(watchedPath);
      }
    }

    LogUtil.logInfo(LOG, genEventInfo(),
        String.format("%s properties refreshed from ZK.", reloadKeys.size()));
    LOG.debug("refreshed keys: {}", reloadKeys);

    return refreshedPropertyMap;
  }

  /**
   * Subscribe a ZK data watch on {@code path} if one is not already registered.
   * Idempotent: a second call for the same path is a no-op.
   */
  private void subscribeWatch(BaseDataAccessor<ZNRecord> baseAccessor, String path) {
    if (_watchedPaths.add(path)) {
      baseAccessor.subscribeDataChanges(path, _watchListener);
    }
  }

  protected String genEventInfo() {
    return String.format("%s::%s::%s", _controlContextProvider.getClusterName(),
        _controlContextProvider.getPipelineName(), _controlContextProvider.getClusterEventId());
  }

  public AbstractDataSnapshot getSnapshot() {
    throw new HelixException(String.format("DataCache %s does not support generating snapshot.",
        getClass().getSimpleName()));
  }

  // for backward compatibility, used in scenarios where we only initialize child
  // classes with cluster name
  protected static ControlContextProvider createDefaultControlContextProvider(
      final String clusterName) {
    return new ControlContextProvider() {
      private String _clusterName = clusterName;
      private String _eventId = UNKNOWN_EVENT_ID;

      @Override
      public String getClusterName() {
        return _clusterName;
      }

      @Override
      public String getClusterEventId() {
        return _eventId;
      }

      @Override
      public void setClusterEventId(String eventId) {
        _eventId = eventId;
      }

      @Override
      public String getPipelineName() {
        return UNKNOWN_PIPELINE;
      }
    };
  }
}
