package org.apache.helix.rest.metadatastore.concurrency;

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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkDistributedLeaderElection implements IZkDataListener, IZkStateListener,
    IZkChildListener {
  private static final Logger LOG = LoggerFactory.getLogger(ZkDistributedLeaderElection.class);
  private static final String PREFIX = "MSDS_SERVER_";
  private static final String PARTICIPANT_ID = "MSDS_PARTICIPANT_ID";

  private final HelixZkClient _zkClient;
  private final String _basePath;
  private final ZNRecord _participantInfo;
  private volatile ZNRecord _currentLeaderInfo;

  private volatile String _myEphemeralSequentialPath;
  private volatile String _sessionId;
  private volatile boolean _isLeader;
  private volatile boolean _closed;

  public ZkDistributedLeaderElection(HelixZkClient zkClient, String basePath,
      ZNRecord participantInfo) {
    synchronized (this) {
      if (zkClient == null || zkClient.isClosed()) {
        throw new IllegalArgumentException("ZkClient cannot be null or closed!");
      }
      _zkClient = zkClient;
      _zkClient.setZkSerializer(new ZNRecordSerializer());
      if (basePath == null || basePath.isEmpty()) {
        throw new IllegalArgumentException("lockBasePath cannot be null or empty!");
      }
      _basePath = basePath;
      _participantInfo = new ZNRecord(participantInfo);
      _participantInfo.setSimpleField(PARTICIPANT_ID, UUID.randomUUID().toString());
      _isLeader = false;
    }
    _zkClient.subscribeStateChanges(this);
    try {
      init(Long.toHexString(_zkClient.getSessionId()));
    } catch (RuntimeException e) {
      close();
      throw e;
    }
  }

  /**
   * Create the base path if it doesn't exist and create an ephemeral sequential ZNode.
   */
  private synchronized void init(String sessionId) {
    if (!isCurrentSession(sessionId)) {
      return;
    }
    if (!sessionId.equals(_sessionId)) {
      _isLeader = false;
      _currentLeaderInfo = null;
    }
    try {
      _zkClient.createPersistent(_basePath, true);
    } catch (ZkNodeExistsException e) {
      // Okay if it exists already
    }

    // Watching the parent also refreshes followers whose predecessor survives a leader change.
    _zkClient.subscribeChildChanges(_basePath, this);
    String candidate = findCandidate(sessionId);
    if (candidate == null) {
      try {
        _zkClient.createEphemeralSequential(_basePath + "/" + PREFIX, _participantInfo, sessionId);
      } catch (RuntimeException e) {
        if (!isCurrentSession(sessionId) || findCandidate(sessionId) == null) {
          throw e;
        }
      }
      candidate = findCandidate(sessionId);
    }
    if (!isCurrentSession(sessionId)) {
      return;
    }
    if (candidate == null) {
      throw new IllegalStateException("Unable to find election candidate at " + _basePath);
    }
    _sessionId = sessionId;
    _myEphemeralSequentialPath = candidate;
    tryAcquiringLeadership();
  }

  private String findCandidate(String sessionId) {
    List<String> children = _zkClient.getChildren(_basePath);
    Collections.sort(children);
    String candidate = null;
    for (String child : children) {
      if (!child.startsWith(PREFIX)) {
        continue;
      }
      String path = _basePath + "/" + child;
      Stat stat = new Stat();
      try {
        ZNRecord record = _zkClient.readData(path, stat);
        if (record != null && sessionId.equals(Long.toHexString(stat.getEphemeralOwner()))
            && _participantInfo.getSimpleField(PARTICIPANT_ID)
                .equals(record.getSimpleField(PARTICIPANT_ID))) {
          if (candidate == null) {
            candidate = path;
          } else {
            // A retried sequential create can leave more than one node for this attempt.
            _zkClient.delete(path);
          }
        }
      } catch (ZkNoNodeException e) {
        // A candidate may disappear while its session is expiring.
      }
    }
    return candidate;
  }

  private synchronized void tryAcquiringLeadership() {
    String sessionId = _sessionId;
    if (!isCurrentSession(sessionId)) {
      _isLeader = false;
      _currentLeaderInfo = null;
      return;
    }
    List<String> children = _zkClient.getChildren(_basePath);
    children.removeIf(child -> !child.startsWith(PREFIX));
    Collections.sort(children);
    if (children.isEmpty() || _myEphemeralSequentialPath == null
        || !children.contains(_myEphemeralSequentialPath.substring(_basePath.length() + 1))) {
      _isLeader = false;
      _currentLeaderInfo = null;
      return;
    }
    String leaderPath = _basePath + "/" + children.get(0);
    ZNRecord leaderInfo = _zkClient.readData(leaderPath, true);
    if (isCurrentSession(sessionId) && leaderInfo != null) {
      boolean wasLeader = _isLeader;
      _currentLeaderInfo = leaderInfo;
      _isLeader = leaderPath.equals(_myEphemeralSequentialPath);
      if (_isLeader && !wasLeader) {
        LOG.info("{} acquired leadership! Info: {}", leaderPath, leaderInfo);
      }
    } else {
      _isLeader = false;
      _currentLeaderInfo = null;
    }
  }

  private boolean isCurrentSession(String sessionId) {
    return !_closed && !_zkClient.isClosed() && sessionId != null && !"0".equals(sessionId)
        && _zkClient.waitUntilConnected(0, TimeUnit.MILLISECONDS)
        && sessionId.equals(Long.toHexString(_zkClient.getSessionId()));
  }

  public boolean isLeader() {
    return _isLeader && isCurrentSession(_sessionId);
  }

  public String getLeaderPath() {
    String path = _myEphemeralSequentialPath;
    return isLeader() ? path : null;
  }

  public ZNRecord getCurrentLeaderInfo() {
    return isCurrentSession(_sessionId) ? _currentLeaderInfo : null;
  }

  public void close() {
    _closed = true;
    _isLeader = false;
    _currentLeaderInfo = null;
    _zkClient.unsubscribeStateChanges(this);
    if (!_zkClient.isClosed()) {
      _zkClient.unsubscribeChildChanges(_basePath, this);
    }
  }

  private void refreshSession(String sessionId) {
    try {
      init(sessionId);
    } catch (RuntimeException e) {
      if (!_closed && !_zkClient.isClosed()
          && sessionId.equals(Long.toHexString(_zkClient.getSessionId()))) {
        throw e;
      }
    }
  }

  @Override
  public synchronized void handleStateChanged(Watcher.Event.KeeperState state) {
    if (state == Watcher.Event.KeeperState.SyncConnected) {
      if (isCurrentSession(_sessionId)) {
        refreshSession(_sessionId);
      }
    } else {
      _isLeader = false;
      _currentLeaderInfo = null;
    }
  }

  @Override
  public void handleNewSession(String sessionId) {
    refreshSession(sessionId);
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) {
    _isLeader = false;
    _currentLeaderInfo = null;
  }

  @Override
  public void handleDataChange(String s, Object o) {
    return;
  }

  @Override
  public void handleDataDeleted(String s) {
    refreshSession(Long.toHexString(_zkClient.getSessionId()));
  }

  @Override
  public void handleChildChange(String path, List<String> children) {
    refreshSession(Long.toHexString(_zkClient.getSessionId()));
  }
}
