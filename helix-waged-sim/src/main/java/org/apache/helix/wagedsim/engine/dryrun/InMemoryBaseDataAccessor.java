package org.apache.helix.wagedsim.engine.dryrun;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.data.Stat;

/**
 * A ZooKeeper-like tree held in memory. Every ancestor of a written node exists (with no data), as
 * in ZooKeeper; removing a node removes its subtree. Versions and times are kept in {@link Stat} so
 * the controller's caches see changes. Watches are not supported: the dry run refreshes explicitly.
 */
public class InMemoryBaseDataAccessor implements BaseDataAccessor<ZNRecord> {
  private static final class Node {
    ZNRecord record;
    final Stat stat = new Stat();
    final TreeSet<String> children = new TreeSet<>();
  }

  private final Map<String, Node> _nodes = new HashMap<>();
  private long _zxid = 1;

  public InMemoryBaseDataAccessor() {
    _nodes.put("/", new Node());
  }

  private static String parent(String path) {
    int index = path.lastIndexOf('/');
    return index <= 0 ? "/" : path.substring(0, index);
  }

  private static String name(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  private static void checkPath(String path) {
    if (path == null || !path.startsWith("/") || (path.length() > 1 && path.endsWith("/"))) {
      throw new IllegalArgumentException("Invalid path: " + path);
    }
  }

  private Node ensure(String path) {
    Node node = _nodes.get(path);
    if (node == null) {
      Node parentNode = ensure(parent(path));
      node = new Node();
      long now = System.currentTimeMillis();
      node.stat.setCtime(now);
      node.stat.setMtime(now);
      node.stat.setCzxid(_zxid);
      node.stat.setMzxid(_zxid++);
      _nodes.put(path, node);
      parentNode.children.add(name(path));
      parentNode.stat.setCversion(parentNode.stat.getCversion() + 1);
      parentNode.stat.setNumChildren(parentNode.children.size());
    }
    return node;
  }

  private void write(Node node, ZNRecord record) {
    node.record = record == null ? null : new ZNRecord(record);
    node.stat.setVersion(node.stat.getVersion() + 1);
    node.stat.setMtime(System.currentTimeMillis());
    node.stat.setMzxid(_zxid++);
  }

  private ZNRecord read(Node node, Stat stat) {
    if (stat != null) {
      copyStat(node.stat, stat);
    }
    if (node.record == null) {
      return null;
    }
    ZNRecord copy = new ZNRecord(node.record);
    copy.setVersion(node.stat.getVersion());
    copy.setCreationTime(node.stat.getCtime());
    copy.setModifiedTime(node.stat.getMtime());
    copy.setEphemeralOwner(node.stat.getEphemeralOwner());
    return copy;
  }

  private static void copyStat(Stat from, Stat to) {
    to.setVersion(from.getVersion());
    to.setCtime(from.getCtime());
    to.setMtime(from.getMtime());
    to.setCzxid(from.getCzxid());
    to.setMzxid(from.getMzxid());
    to.setCversion(from.getCversion());
    to.setNumChildren(from.getNumChildren());
    to.setEphemeralOwner(from.getEphemeralOwner());
  }

  private static Stat statCopy(Stat from) {
    Stat stat = new Stat();
    copyStat(from, stat);
    return stat;
  }

  @Override
  public synchronized boolean create(String path, ZNRecord record, int options) {
    checkPath(path);
    if (_nodes.containsKey(path)) {
      return false;
    }
    Node node = ensure(path);
    node.record = record == null ? null : new ZNRecord(record);
    return true;
  }

  @Override
  public boolean create(String path, ZNRecord record, int options, long ttl) {
    return create(path, record, options);
  }

  @Override
  public synchronized boolean set(String path, ZNRecord record, int options) {
    checkPath(path);
    write(ensure(path), record);
    return true;
  }

  @Override
  public synchronized boolean set(String path, ZNRecord record, int expectVersion, int options) {
    checkPath(path);
    Node node = _nodes.get(path);
    if (expectVersion >= 0 && (node == null || node.stat.getVersion() != expectVersion)) {
      return false;
    }
    write(ensure(path), record);
    return true;
  }

  @Override
  public synchronized boolean update(String path, DataUpdater<ZNRecord> updater, int options) {
    checkPath(path);
    Node node = _nodes.get(path);
    ZNRecord current = node == null || node.record == null ? null : new ZNRecord(node.record);
    ZNRecord updated = updater.update(current);
    write(ensure(path), updated);
    return true;
  }

  @Override
  public synchronized boolean remove(String path, int options) {
    checkPath(path);
    Node node = _nodes.get(path);
    if (node == null) {
      return false;
    }
    for (String child : new ArrayList<>(node.children)) {
      remove(path.equals("/") ? "/" + child : path + "/" + child, options);
    }
    if (!path.equals("/")) {
      _nodes.remove(path);
      Node parentNode = _nodes.get(parent(path));
      if (parentNode != null) {
        parentNode.children.remove(name(path));
        parentNode.stat.setCversion(parentNode.stat.getCversion() + 1);
        parentNode.stat.setNumChildren(parentNode.children.size());
      }
    }
    return true;
  }

  @Override
  public synchronized boolean[] createChildren(List<String> paths, List<ZNRecord> records, int options) {
    boolean[] result = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      result[i] = create(paths.get(i), records.get(i), options);
    }
    return result;
  }

  @Override
  public synchronized boolean[] setChildren(List<String> paths, List<ZNRecord> records, int options) {
    boolean[] result = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      result[i] = set(paths.get(i), records.get(i), options);
    }
    return result;
  }

  @Override
  public synchronized boolean[] updateChildren(List<String> paths,
      List<DataUpdater<ZNRecord>> updaters, int options) {
    boolean[] result = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      result[i] = update(paths.get(i), updaters.get(i), options);
    }
    return result;
  }

  @Override
  public synchronized boolean[] remove(List<String> paths, int options) {
    boolean[] result = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      result[i] = remove(paths.get(i), options);
    }
    return result;
  }

  @Override
  public synchronized ZNRecord get(String path, Stat stat, int options) {
    Node node = _nodes.get(path);
    if (node == null) {
      if (AccessOption.isThrowExceptionIfNotExist(options)) {
        throw new ZkNoNodeException("No node " + path);
      }
      return null;
    }
    return read(node, stat);
  }

  @Override
  public synchronized List<ZNRecord> get(List<String> paths, List<Stat> stats, int options) {
    return get(paths, stats, options, false);
  }

  @Override
  public synchronized List<ZNRecord> get(List<String> paths, List<Stat> stats, int options,
      boolean throwException) {
    List<ZNRecord> result = new ArrayList<>(paths.size());
    List<Stat> statList = new ArrayList<>(paths.size());
    for (String path : paths) {
      Node node = _nodes.get(path);
      if (node == null) {
        if (throwException) {
          throw new ZkNoNodeException("No node " + path);
        }
        result.add(null);
        statList.add(null);
        continue;
      }
      statList.add(statCopy(node.stat));
      result.add(read(node, null));
    }
    if (stats != null) {
      stats.clear();
      stats.addAll(statList);
    }
    return result;
  }

  @Override
  public synchronized List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
    if (stats != null) {
      stats.clear();
    }
    Node parentNode = _nodes.get(parentPath);
    if (parentNode == null) {
      return Collections.emptyList();
    }
    List<ZNRecord> result = new ArrayList<>();
    for (String child : parentNode.children) {
      Node node = _nodes.get(parentPath.equals("/") ? "/" + child : parentPath + "/" + child);
      if (node != null && node.record != null) {
        result.add(read(node, null));
        if (stats != null) {
          stats.add(statCopy(node.stat));
        }
      }
    }
    return result;
  }

  @Override
  public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options, int retryCount,
      int retryInterval) {
    return getChildren(parentPath, stats, options);
  }

  @Override
  public synchronized List<String> getChildNames(String parentPath, int options) {
    Node parentNode = _nodes.get(parentPath);
    return parentNode == null ? Collections.emptyList() : new ArrayList<>(parentNode.children);
  }

  @Override
  public synchronized boolean exists(String path, int options) {
    return _nodes.containsKey(path);
  }

  @Override
  public synchronized boolean[] exists(List<String> paths, int options) {
    boolean[] result = new boolean[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      result[i] = _nodes.containsKey(paths.get(i));
    }
    return result;
  }

  @Override
  public synchronized Stat[] getStats(List<String> paths, int options) {
    Stat[] result = new Stat[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      Node node = _nodes.get(paths.get(i));
      result[i] = node == null ? null : statCopy(node.stat);
    }
    return result;
  }

  @Override
  public synchronized Stat getStat(String path, int options) {
    Node node = _nodes.get(path);
    return node == null ? null : statCopy(node.stat);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
  }

  @Override
  public void unsubscribeDataChanges(String path, IZkDataListener listener) {
  }

  @Override
  public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
    return getChildNames(path, 0);
  }

  @Override
  public void unsubscribeChildChanges(String path, IZkChildListener listener) {
  }

  @Override
  public void reset() {
  }

  @Override
  public void close() {
  }
}
