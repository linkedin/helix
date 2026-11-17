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
import java.util.concurrent.locks.Lock;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.model.ConvergenceStatus;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;

/**
 * The parsed convergence record of each cluster, read again only when its znode changes. A request
 * then costs one small stat read instead of reading and decompressing the record, which matters
 * when a caller polls a large cluster. Bounded by total stored bytes and by idle time; cached
 * records are only read, never changed.
 */
final class ConvergenceRecordCache {
  // A parsed record takes roughly this many times its compressed znode size on the heap.
  private static final int PARSED_PER_STORED_BYTE = 15;
  private static final long MAX_HEAP_BYTES = 64L * 1024 * 1024;
  private static final long IDLE_MINUTES = 10L;

  private static final class Key {
    final RealmAwareZkClient _zkClient;
    final String _clusterId;

    Key(RealmAwareZkClient zkClient, String clusterId) {
      _zkClient = zkClient;
      _clusterId = clusterId;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Key && ((Key) other)._zkClient == _zkClient
          && ((Key) other)._clusterId.equals(_clusterId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(System.identityHashCode(_zkClient), _clusterId);
    }
  }

  private static final class Entry {
    final long _mzxid;
    final int _bytes;
    final ConvergenceStatus _record;

    Entry(long mzxid, int bytes, ConvergenceStatus record) {
      _mzxid = mzxid;
      _bytes = bytes;
      _record = record;
    }
  }

  static final ConvergenceRecordCache INSTANCE = new ConvergenceRecordCache();
  private final Striped<Lock> _locks = Striped.lazyWeakLock(64);
  private final Cache<Key, Entry> _cache = CacheBuilder.newBuilder()
      .maximumWeight(MAX_HEAP_BYTES)
      .<Key, Entry>weigher((key, entry) -> Math.max(1, entry._bytes * PARSED_PER_STORED_BYTE))
      .expireAfterAccess(IDLE_MINUTES, TimeUnit.MINUTES)
      .build();

  ConvergenceStatus get(RealmAwareZkClient zkClient, HelixDataAccessor accessor,
      String clusterId) {
    String path = accessor.keyBuilder().convergenceStatus().getPath();
    Key key = new Key(zkClient, clusterId);
    Stat stat = zkClient.getStat(path);
    if (stat == null) {
      _cache.invalidate(key);
      return null;
    }
    // mzxid, unlike the version, also changes when a cluster is deleted and created again.
    Entry entry = _cache.getIfPresent(key);
    if (entry != null && entry._mzxid == stat.getMzxid()) {
      return entry._record;
    }
    // One reader per cluster: the others wait for it instead of reading the same record again.
    Lock lock = _locks.get(key);
    lock.lock();
    try {
      entry = _cache.getIfPresent(key);
      if (entry != null && entry._mzxid >= stat.getMzxid()) {
        return entry._record;
      }
      Stat read = new Stat();
      ZNRecord record = accessor.getBaseDataAccessor().get(path, read, AccessOption.PERSISTENT);
      if (record == null) {
        return null;
      }
      ConvergenceStatus status = new ConvergenceStatus(record);
      _cache.put(key, new Entry(read.getMzxid(), read.getDataLength(), status));
      return status;
    } finally {
      lock.unlock();
    }
  }
}
