package org.apache.helix;

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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.integration.manager.ClusterSpectatorManager;
import org.apache.helix.integration.manager.ZkTestManager;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Pins the allocation optimization in {@link CallbackHandler#subscribeForChanges}. The method used
 * to re-read and fully deserialize every child ZNRecord on every callback purely to read each
 * child's {@code getBucketSize()}; it now caches that value per child path. This test verifies the
 * cache still reflects the exact bucketized-vs-non-bucketized classification the old per-record
 * read produced, and that the cache is maintained across callbacks (stale entries evicted, new
 * children read on demand).
 */
public class TestCallbackHandlerBucketCache extends ZkUnitTestBase {

  private static final String CLUSTER_NAME = TestHelper.getTestClassName();
  private static final List<String> INSTANCES =
      Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920");
  private static final int BUCKET_SIZE = 2;
  private static final String BUCKETIZED_DB = "BucketizedDB";
  private static final String PLAIN_DB = "PlainDB";

  private ClusterSpectatorManager _spectator;
  private ZKHelixDataAccessor _accessor;
  private String _idealStatesPath;

  @BeforeClass
  public void beforeClass() throws Exception {
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addInstancesToCluster(CLUSTER_NAME, INSTANCES.toArray(new String[0]));

    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
    _idealStatesPath = keyBuilder.idealStates().getPath();

    // A bucketized resource: its node carries BUCKET_SIZE and its payload is split into buckets.
    _accessor.setProperty(keyBuilder.idealStates(BUCKETIZED_DB), newIdealState(BUCKETIZED_DB,
        BUCKET_SIZE));
    // A normal (non-bucketized) resource.
    _accessor.setProperty(keyBuilder.idealStates(PLAIN_DB), newIdealState(PLAIN_DB, 0));

    _spectator = new ClusterSpectatorManager(ZK_ADDR, CLUSTER_NAME, "spectator_0");
    _spectator.syncStart();
  }

  @AfterClass
  public void afterClass() {
    if (_spectator != null) {
      _spectator.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testBucketSizeCacheClassifiesAndEvicts() throws Exception {
    final IdealStateChangeListener listener = (idealStates, context) -> {
    };
    // Registering the listener synchronously runs INIT, which populates the cache.
    _spectator.addIdealStateChangeListener(listener);

    CallbackHandler handler = findHandler(_spectator, listener);
    Assert.assertNotNull(handler, "Expected a CallbackHandler for the IdealState listener");

    Map<String, Integer> cache = getBucketSizeCache(handler);
    String bucketizedPath = _idealStatesPath + "/" + BUCKETIZED_DB;
    String plainPath = _idealStatesPath + "/" + PLAIN_DB;

    // The cache must reflect the exact getBucketSize() of each child: bucketized > 0, plain == 0.
    // This is the classification the old code recomputed by deserializing every child each time.
    Assert.assertEquals(cache.get(bucketizedPath), Integer.valueOf(BUCKET_SIZE),
        "Bucketized resource must be cached with its real bucket size");
    Assert.assertEquals(cache.get(plainPath), Integer.valueOf(0),
        "Non-bucketized resource must be cached with bucket size 0");

    // Dropping a child must evict its cache entry on the next (child-change) callback, and must
    // leave the surviving bucketized entry untouched.
    _accessor.removeProperty(_accessor.keyBuilder().idealStates(PLAIN_DB));
    Assert.assertTrue(TestHelper.verify(() -> !cache.containsKey(plainPath), 10000),
        "Dropped resource must be evicted from the bucket-size cache");
    Assert.assertEquals(cache.get(bucketizedPath), Integer.valueOf(BUCKET_SIZE),
        "Surviving bucketized resource must remain cached with its bucket size");

    // Adding a new child must be read and cached (as non-bucketized) on the next callback.
    _accessor.setProperty(_accessor.keyBuilder().idealStates(PLAIN_DB), newIdealState(PLAIN_DB, 0));
    Assert.assertTrue(
        TestHelper.verify(() -> Integer.valueOf(0).equals(cache.get(plainPath)), 10000),
        "Newly added resource must be read and cached on a callback");
  }

  private static IdealState newIdealState(String dbName, int bucketSize) {
    // calculateIdealState sorts its instance list in place, so pass a mutable copy.
    ZNRecord record = DefaultIdealStateCalculator.calculateIdealState(new ArrayList<>(INSTANCES), 4,
        2, dbName, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas("3");
    if (bucketSize > 0) {
      idealState.setBucketSize(bucketSize);
    }
    return idealState;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Integer> getBucketSizeCache(CallbackHandler handler) throws Exception {
    Field field = CallbackHandler.class.getDeclaredField("_childPathBucketSize");
    field.setAccessible(true);
    return (Map<String, Integer>) field.get(handler);
  }

  private static CallbackHandler findHandler(ZkTestManager manager, Object listener) {
    for (CallbackHandler handler : manager.getHandlers()) {
      if (handler.getListener() == listener) {
        return handler;
      }
    }
    return null;
  }
}
