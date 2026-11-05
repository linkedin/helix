package org.apache.helix.metaclient.impl.zk;

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

import com.google.common.collect.ImmutableList;
import org.apache.helix.metaclient.api.ChildChangeListener;
import org.apache.helix.metaclient.api.MetaClientCacheInterface;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.factories.MetaClientCacheConfig;
import org.apache.helix.metaclient.impl.zk.adapter.ChildListenerAdapter;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZkMetaClientCache<T> extends ZkMetaClient<T> implements MetaClientCacheInterface<T> {

    private ConcurrentHashMap<String, T> _dataCacheMap;
    private final String _rootEntry;
    private TrieNode _childrenCacheTree;
    private ChildChangeListener _eventListener;
    private boolean _cacheData;
    private boolean _cacheChildren;
    private static final Logger LOG = LoggerFactory.getLogger(ZkMetaClientCache.class);
    private  ZkClient _cacheClient;
    private ExecutorService executor;

    // TODO: Look into using conditional variable instead of latch.
    private final CountDownLatch _initializedCache = new CountDownLatch(1);

    /**
     * Constructor for ZkMetaClientCache.
     * @param config ZkMetaClientConfig
     * @param cacheConfig MetaClientCacheConfig
     */
    public ZkMetaClientCache(ZkMetaClientConfig config, MetaClientCacheConfig cacheConfig) {
        super(config);
        _cacheClient = getZkClient();
        _rootEntry = cacheConfig.getRootEntry();
        _cacheData = cacheConfig.getCacheData();
        _cacheChildren = cacheConfig.getCacheChildren();

        if (_cacheData) {
            _dataCacheMap = new ConcurrentHashMap<>();
        }
        if (_cacheChildren) {
            _childrenCacheTree = new TrieNode(_rootEntry, _rootEntry.substring(1));
        }
    }

    /**
     * Get data for a given key.
     * If datacache is enabled, will fetch for cache. If it doesn't exist
     * returns null (for when initial populating cache is in progress).
     * @param key key to identify the entry
     * @return data for the key
     */
    @Override
    public T get(final String key) {
        if (_cacheData) {
            T data = getDataCacheMap().get(key);
            if (data == null) {
                LOG.debug("Data not found in cache for key: {}. This could be because the cache is still being populated.", key);
            }
            return data;
        }
        return super.get(key);
    }

    @Override
    public List<T> get(List<String> keys) {
        List<T> dataList = new ArrayList<>();
        for (String key : keys) {
            dataList.add(get(key));
        }
        return dataList;
    }

    /**
     * Get the direct children for a given key.
     * @param key For metadata storage that has hierarchical key space (e.g. ZK), the key would be
     *            a parent key,
     *            For metadata storage that has non-hierarchical key space (e.g. etcd), the key would
     *            be a prefix key.
     * @return list of direct children or null if key doesn't exist / cache is not populated yet.
     */
    @Override
    public List<String> getDirectChildrenKeys(final String key) {
        if (_cacheChildren) {
            TrieNode node = _childrenCacheTree.processPath(key, true);
            if (node == null) {
                LOG.debug("Children not found in cache for key: {}. This could be because the cache is still being populated.", key);
                return null;
            }
            return ImmutableList.copyOf(node.getChildren().keySet());
        }
        return super.getDirectChildrenKeys(key);
    }

    /**
     * Get the number of direct children for a given key.
     * @param key For metadata storage that has hierarchical key space (e.g. ZK), the key would be
     *            a parent key,
     *            For metadata storage that has non-hierarchical key space (e.g. etcd), the key would
     *            be a prefix key.
     * @return number of direct children or 0 if key doesn't exist / has no children / cache is not populated yet.
     */
    @Override
    public int countDirectChildren(final String key) {
        if (_cacheChildren) {
            TrieNode node = _childrenCacheTree.processPath(key, true);
            if (node == null) {
                LOG.debug("Children not found in cache for key: {}. This could be because the cache is still being populated.", key);
                return 0;
            }
            return node.getChildren().size();
        }
        return super.countDirectChildren(key);
    }

    private void populateAllCache() {
        // TODO: Concurrently populate children and data cache.
        // The data map and the children tree are null when they are not cached.
        if (!readTree(_dataCacheMap, _childrenCacheTree)) {
            LOG.warn("Root entry: {} does not exist.", _rootEntry);
        }
        // Let the other threads know that the cache is populated.
        _initializedCache.countDown();
    }

    /**
     * Reads the data and the children of every entry under the root entry, root included.
     * Entries deleted during the read are skipped.
     * @param dataMap map to put the data into, or null to skip reading data
     * @param childrenTree trie to add the entries into, or null to skip it
     * @return false if the root entry does not exist
     */
    private boolean readTree(Map<String, T> dataMap, TrieNode childrenTree) {
        Queue<String> queue = new ArrayDeque<>();
        queue.add(_rootEntry);

        while (!queue.isEmpty()) {
            String node = queue.poll();
            List<String> childNodes;
            try {
                childNodes = _cacheClient.getChildren(node);
            } catch (ZkNoNodeException e) {
                if (node.equals(_rootEntry)) {
                    return false;
                }
                continue;
            }
            if (dataMap != null) {
                T dataRecord = _cacheClient.readData(node, true);
                if (dataRecord != null) {
                    dataMap.put(node, dataRecord);
                }
            }
            if (childrenTree != null) {
                childrenTree.processPath(node, true);
            }
            for (String child : childNodes) {
                queue.add(node + "/" + child); // Add child nodes to the queue with their full path.
            }
        }
        return true;
    }

    /**
     * Re-reads the tree under the root entry and updates the cache to match it. Called after the
     * client connects again, as the changes made while it was disconnected are not delivered. The
     * cache is only updated after the whole tree is read, so a failed read leaves it unchanged.
     */
    private void rebuildCache() {
        Map<String, T> dataMap = new HashMap<>();
        TrieNode childrenTree = new TrieNode(_rootEntry, _rootEntry.substring(1));
        try {
            if (!readTree(_cacheData ? dataMap : null, _cacheChildren ? childrenTree : null)) {
                LOG.warn("Root entry: {} does not exist.", _rootEntry);
            }
        } catch (Exception e) {
            LOG.error("Failed to rebuild the cache of root entry: {}. It may be stale until the "
                + "next reconnect.", _rootEntry, e);
            return;
        }
        if (_cacheData) {
            _dataCacheMap.keySet().retainAll(dataMap.keySet());
            _dataCacheMap.putAll(dataMap);
        }
        if (_cacheChildren) {
            syncChildren(_childrenCacheTree, childrenTree);
        }
    }

    // Makes the subtree of the cached trie node match the subtree of the current one.
    private static void syncChildren(TrieNode cachedNode, TrieNode currentNode) {
        Map<String, TrieNode> cachedChildren = cachedNode.getChildren();
        cachedChildren.keySet().retainAll(currentNode.getChildren().keySet());
        for (Map.Entry<String, TrieNode> child : currentNode.getChildren().entrySet()) {
            TrieNode cachedChild = cachedChildren.putIfAbsent(child.getKey(), child.getValue());
            if (cachedChild != null) {
                syncChildren(cachedChild, child.getValue());
            }
        }
    }

    private class CacheUpdateRunnable implements Runnable {
        private final String path;
        private final ChildChangeListener.ChangeType changeType;

        public CacheUpdateRunnable(String path, ChildChangeListener.ChangeType changeType) {
            this.path = path;
            this.changeType = changeType;
        }

        @Override
        public void run() {
            waitForPopulateAllCache();
            //  TODO: HANDLE DEDUP EVENT CHANGES
            switch (changeType) {
                case ENTRY_CREATED:
                    _childrenCacheTree.processPath(path, true);
                    modifyDataInCache(path, false);
                    break;
                case ENTRY_DELETED:
                    _childrenCacheTree.processPath(path, false);
                    modifyDataInCache(path, true);
                    break;
                case ENTRY_DATA_CHANGE:
                    modifyDataInCache(path, false);
                    break;
                case ENTRY_RESYNC:
                    rebuildCache();
                    break;
                default:
                    LOG.error("Unknown change type: " + changeType);
            }
        }
    }

    private void waitForPopulateAllCache() {
        try {
            _initializedCache.await();
        } catch (InterruptedException e) {
            throw new MetaClientException("Interrupted while waiting for cache to populate.", e);
        }
    }

    private void modifyDataInCache(String path, Boolean isDelete) {
        if (_cacheData) {
            if (isDelete) {
                getDataCacheMap().remove(path);
            } else {
                T dataRecord = _cacheClient.readData(path, true);
                if (dataRecord == null) {
                    // The entry was deleted after the event.
                    getDataCacheMap().remove(path);
                } else {
                    getDataCacheMap().put(path, dataRecord);
                }
            }
        }
    }

    public ConcurrentHashMap<String, T> getDataCacheMap() {
        return _dataCacheMap;
    }


    /**
     * Connect to the underlying ZkClient.
     */
    @Override
    public void connect() {
        super.connect();
        _eventListener = (path, changeType) -> {
            Runnable cacheUpdateRunnable = new CacheUpdateRunnable(path, changeType);
            executor.execute(cacheUpdateRunnable);
        };
        executor = Executors.newSingleThreadExecutor();
        _cacheClient.subscribePersistRecursiveListener(_rootEntry, new ChildListenerAdapter(_eventListener));
        populateAllCache();
    }
}
