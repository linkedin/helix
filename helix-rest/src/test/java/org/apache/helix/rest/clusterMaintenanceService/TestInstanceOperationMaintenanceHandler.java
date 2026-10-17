package org.apache.helix.rest.clusterMaintenanceService;

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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.constants.InstanceConstants.InstanceOperationSource;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.clusterMaintenanceService.InstanceOperationMaintenanceHandler.BadRequestException;
import org.apache.helix.rest.clusterMaintenanceService.InstanceOperationMaintenanceHandler.Outcome;
import org.apache.helix.rest.clusterMaintenanceService.InstanceOperationMaintenanceHandler.Status;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Runs against a real ZooKeeper so the multi, its version checks and the retries are real. */
public class TestInstanceOperationMaintenanceHandler {
  // A port of its own so this class never shares state with the REST suite's ZooKeeper.
  private static final String ZK_ADDR = "localhost:2197";
  private static final String CLUSTER = "AtomicMaintenanceCluster";
  private static final long HOUR_MS = 3_600_000L;

  private ZkServer _zkServer;
  private HelixZkClient _zkClient;
  private BaseDataAccessor<ZNRecord> _accessor;
  private ConfigAccessor _configAccessor;
  private ClusterSetup _setupTool;
  private long _expiresAt;

  @BeforeClass
  public void beforeClass() throws Exception {
    _zkServer = TestHelper.startZkServer(ZK_ADDR);
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer());
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR), clientConfig);
    _accessor = new ZkBaseDataAccessor<>(_zkClient);
    _configAccessor = new ConfigAccessor(_zkClient);
    _setupTool = new ClusterSetup(_zkClient);
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    _zkClient.close();
    _zkServer.shutdown();
  }

  @BeforeMethod
  public void beforeMethod() {
    _setupTool.addCluster(CLUSTER, true);
    for (int i = 0; i < 6; i++) {
      _setupTool.addInstanceToCluster(CLUSTER, instance(i));
    }
    _expiresAt = System.currentTimeMillis() + HOUR_MS;
  }

  @Test
  public void testOverTheCapTheAdmittedPartCommitsAtOnce() {
    setCap(2, -1);
    int[] before = {version(instance(0)), version(instance(1)), version(instance(2))};
    AtomicInteger commits = new AtomicInteger();
    RealmAwareZkClient counting = withMulti(ops -> {
      commits.incrementAndGet();
      _zkClient.multi(ops);
    });

    Map<String, Outcome> outcomes =
        marker(counting, _expiresAt, instance(0), instance(1), instance(2), instance(3));

    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED, Status.BUDGET_EXHAUSTED,
        Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(),
        "Took a maintenance marker until " + at(_expiresAt) + ".");
    Assert.assertEquals(outcomes.get(instance(0)).getExpiresAtMillis(), Long.valueOf(_expiresAt));
    Assert.assertEquals(outcomes.get(instance(2)).getMessage(), "The maintenance marker cap of 2 "
        + "had room for 2 new markers (0 live), and instances earlier in this request used it up. "
        + "Retry once markers are cleared or expire.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        "Applied to 2 of 4 instances (2 BUDGET_EXHAUSTED).");
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(read(i).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
      Assert.assertEquals(version(instance(i)), before[i] + 1);
    }
    Assert.assertEquals(version(instance(2)), before[2]);
    Assert.assertEquals(commits.get(), 1);
  }

  @Test
  public void testPercentageCapAndUnlimitedWhenNoCapIsSet() {
    setCap(-1, 34); // 34% of 6 instances is 2.
    marker(_expiresAt, instance(0));
    Map<String, Outcome> outcomes = marker(_expiresAt, instance(1),
        instance(2));
    assertStatuses(outcomes, Status.APPLIED, Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(outcomes.get(instance(2)).getMessage(), "The maintenance marker cap of 2 "
        + "(34% of 6 instances) had room for 1 new marker (1 live), and instances earlier in this "
        + "request used it up. Retry once markers are cleared or expire.");
    Assert.assertEquals(marker(_expiresAt, instance(2)).get(instance(2))
        .getMessage(), "The maintenance marker cap of 2 (34% of 6 instances) is full with 2 live "
        + "markers. Retry once markers are cleared or expire.");

    setCap(-1, -1);
    outcomes = marker(_expiresAt, instance(2), instance(3));
    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED);
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        "Applied to all 2 instances.");
  }

  @Test
  public void testExpiredMarkerNeedsBudgetAgain() {
    setCap(1, -1);
    InstanceConfig expired = read(0);
    expired.setInstanceOperationMaintenanceUntilMs(System.currentTimeMillis() - 1);
    _accessor.set(path(0), expired.getRecord(), AccessOption.PERSISTENT);

    assertStatuses(marker(_expiresAt, instance(1), instance(0)),
        Status.APPLIED, Status.BUDGET_EXHAUSTED);
  }

  @Test
  public void testRenewalTakesNoBudgetAndNeverShortensTheMarker() {
    setCap(1, -1);
    marker(_expiresAt, instance(0));
    long first = _expiresAt;
    _expiresAt += HOUR_MS;

    Map<String, Outcome> outcomes = marker(_expiresAt, instance(0));
    assertStatuses(outcomes, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(),
        "Renewed the maintenance marker until " + at(_expiresAt) + ".");
    Assert.assertEquals(outcomes.get(instance(0)).getExpiresAtMillis(), Long.valueOf(_expiresAt));
    int version = version(instance(0));

    Assert.assertEquals(marker(first, instance(0)).get(instance(0)).getMessage(), "The "
        + "maintenance marker already runs until " + at(_expiresAt) + ", past the requested "
        + "expiry.");
    Assert.assertEquals(marker(_expiresAt, instance(0)).get(instance(0)).getMessage(),
        "The maintenance marker already runs until " + at(_expiresAt) + ".");
    // A request that changes nothing writes nothing.
    Assert.assertEquals(version(instance(0)), version);
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
  }

  @Test
  public void testMarkerAloneClearsAndAnInstanceWithoutOneIsNotWritten() {
    marker(_expiresAt, instance(0));
    // Cleared even while another source keeps the instance offline.
    writeOperation(0, InstanceOperation.DISABLE, InstanceOperationSource.USER);
    int[] before = {version(instance(0)), version(instance(1))};

    Map<String, Outcome> outcomes = marker(InstanceOperationMaintenanceHandler.CLEAR_MARKER,
        instance(0), instance(1));

    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Cleared the maintenance marker.");
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(),
        "It had no maintenance marker to clear.");
    Assert.assertNull(outcomes.get(instance(0)).getExpiresAtMillis());
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
    Assert.assertEquals(version(instance(0)), before[0] + 1);
    Assert.assertEquals(version(instance(1)), before[1]);
  }

  @Test
  public void testDefaultExpiryUsesTheClusterDefault() {
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER);
    config.setDefaultInstanceOperationMaintenanceDurationMs(HOUR_MS);
    _configAccessor.setClusterConfig(CLUSTER, config);
    long start = System.currentTimeMillis();

    Long expiresAt = marker(InstanceOperationMaintenanceHandler.DEFAULT_EXPIRY, instance(0))
        .get(instance(0)).getExpiresAtMillis();

    Assert.assertTrue(expiresAt >= start + HOUR_MS
        && expiresAt <= System.currentTimeMillis() + HOUR_MS, String.valueOf(expiresAt));
  }

  @Test
  public void testAdmissionJudgedAgainstAChangedCapIsJudgedAgain() {
    setCap(1, -1);
    AtomicInteger commits = new AtomicInteger();
    // The cap is lowered after this admission has read it, before it commits.
    RealmAwareZkClient raced = withMulti(ops -> {
      if (commits.getAndIncrement() == 0) {
        setCap(0, -1);
      }
      _zkClient.multi(ops);
    });

    Map<String, Outcome> outcomes = marker(raced, _expiresAt, instance(0));

    assertStatuses(outcomes, Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
  }

  @Test
  public void testLiveMarkerClearedBeforeTheCommitIsTakenAgain() {
    marker(_expiresAt, instance(0));
    AtomicInteger commits = new AtomicInteger();
    // A writer outside this handler clears the marker this request leaves as is.
    RealmAwareZkClient clearing = withMulti(ops -> {
      if (commits.getAndIncrement() == 0) {
        InstanceConfig cleared = read(0);
        cleared.setInstanceOperationMaintenanceUntilMs(
            InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
        _accessor.set(path(0), cleared.getRecord(), AccessOption.PERSISTENT);
      }
      _zkClient.multi(ops);
    });

    Map<String, Outcome> outcomes = marker(clearing, _expiresAt, instance(0), instance(1));

    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED);
    Assert.assertEquals(commits.get(), 2);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(),
        "Took a maintenance marker until " + at(_expiresAt) + ".");
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
  }

  @Test
  public void testConflictAfterEveryAttemptLosesWritesNothing() {
    int before = version(instance(0));
    AtomicInteger attempts = new AtomicInteger();
    // Another write lands between every read and commit of this request.
    RealmAwareZkClient alwaysBeaten = withMulti(ops -> {
      attempts.incrementAndGet();
      touchClusterConfig();
      _zkClient.multi(ops);
    });

    Map<String, Outcome> outcomes = marker(alwaysBeaten, _expiresAt, instance(0));

    assertStatuses(outcomes, Status.CONFLICT);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Concurrent writes to this "
        + "cluster won all 5 attempts. Resend the request.");
    Assert.assertEquals(attempts.get(), InstanceOperationMaintenanceHandler.MAX_ATTEMPTS);
    Assert.assertEquals(version(instance(0)), before);
  }

  @Test
  public void testClusterRemovedMidRequestIsABadRequest() {
    RealmAwareZkClient removing = withMulti(ops -> {
      _accessor.remove(PropertyPathBuilder.clusterConfig(CLUSTER), AccessOption.PERSISTENT);
      _zkClient.multi(ops);
    });

    assertBadRequest(() -> marker(removing, _expiresAt, instance(0)),
        "Cluster " + CLUSTER + " does not exist.");
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
  }

  @Test
  public void testCapOfZeroAdmitsNoMarker() {
    setCap(0, -1);
    Map<String, Outcome> outcomes = marker(_expiresAt, instance(0), instance(1), instance(2),
        instance(3), "missing");
    assertStatuses(outcomes, Status.BUDGET_EXHAUSTED, Status.BUDGET_EXHAUSTED,
        Status.BUDGET_EXHAUSTED, Status.BUDGET_EXHAUSTED, Status.INSTANCE_NOT_FOUND);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "The maintenance marker cap is 0, "
        + "so no instance can take a marker. Raise the cap to allow any.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        "Applied to 0 of 5 instances (1 INSTANCE_NOT_FOUND, 4 BUDGET_EXHAUSTED).");
  }

  @Test
  public void testBadRequests() {
    assertBadRequest(() -> marker(_expiresAt),
        "instances must be a non-empty list of instance names.");
    assertBadRequest(() -> marker(_expiresAt, instance(0), ""),
        "instances[1] is \"\", which is not an instance name.");
    assertBadRequest(() -> marker(_expiresAt, instance(0), null),
        "instances[1] is null, which is not an instance name.");
    // A name that would resolve to another znode path, or that ZooKeeper cannot hold.
    for (String path : Arrays.asList("a/b", ".", "..", "a\tb", "a\u0000b", "a\uD83D\uDE00b")) {
      assertBadRequest(() -> marker(_expiresAt, path),
          "instances[0] is \"" + path + "\", which is not an instance name.");
    }
    char[] longName = new char[300];
    Arrays.fill(longName, 'n');
    assertBadRequest(() -> marker(_expiresAt, new String(longName) + "\t"), "instances[0] is \""
        + new String(longName, 0, 197) + "...\", which is not an instance name.");
    assertBadRequest(() -> new InstanceOperationMaintenanceHandler(_zkClient).apply(
        "noSuchCluster", Collections.singletonList(instance(0)), _expiresAt),
        "Cluster noSuchCluster does not exist.");
    String noDefault = "expiresAtMillis is missing or 0, which asks for the cluster default, but "
        + "cluster " + CLUSTER + " has no positive DEFAULT_INSTANCE_OPERATION_MAINTENANCE_"
        + "DURATION_MS. Give an expiry, or set the cluster default.";
    assertBadRequest(() -> marker(InstanceOperationMaintenanceHandler.DEFAULT_EXPIRY,
        instance(0)), noDefault);
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER);
    config.setDefaultInstanceOperationMaintenanceDurationMs(0L);
    _configAccessor.setClusterConfig(CLUSTER, config);
    assertBadRequest(() -> marker(InstanceOperationMaintenanceHandler.DEFAULT_EXPIRY,
        instance(0)), noDefault);
    assertBadRequest(() -> marker(1000L, instance(0)), "expiresAtMillis 1000 (" + at(1000L)
        + ") is not in the future. Give a later expiry.");
    assertBadRequest(() -> marker(-5L, instance(0)), "expiresAtMillis -5 is negative. Give a "
        + "future epoch time in milliseconds, 0 for the cluster default, or -1 to clear the "
        + "marker.");
    Assert.assertFalse(_accessor.exists("/noSuchCluster", AccessOption.PERSISTENT));
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
  }

  /** A client whose multi runs the given action instead; every other call goes to ZooKeeper. */
  private RealmAwareZkClient withMulti(Consumer<Iterable<Op>> multi) {
    return (RealmAwareZkClient) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[]{RealmAwareZkClient.class}, (proxy, method, args) -> {
          if (method.getName().equals("multi")) {
            @SuppressWarnings("unchecked")
            Iterable<Op> ops = (Iterable<Op>) args[0];
            multi.accept(ops);
            return null;
          }
          try {
            return method.invoke(_zkClient, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  private Map<String, Outcome> marker(long expiresAt, String... instances) {
    return marker(_zkClient, expiresAt, instances);
  }

  private Map<String, Outcome> marker(RealmAwareZkClient zkClient, long expiresAt,
      String... instances) {
    return new InstanceOperationMaintenanceHandler(zkClient).apply(CLUSTER,
        Arrays.asList(instances), expiresAt);
  }

  private void writeOperation(int i, InstanceOperation operation, InstanceOperationSource source) {
    InstanceConfig config = read(i);
    config.setInstanceOperation(new InstanceConfig.InstanceOperation.Builder()
        .setOperation(operation).setSource(source).build());
    _accessor.set(path(i), config.getRecord(), AccessOption.PERSISTENT);
  }

  /** Rewrites the cluster config as is, which moves its version. */
  private void touchClusterConfig() {
    _configAccessor.setClusterConfig(CLUSTER, _configAccessor.getClusterConfig(CLUSTER));
  }

  private void setCap(int absolute, int percentage) {
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER);
    config.setInstanceOperationMaintenanceBudget(-1);
    config.setInstanceOperationMaintenanceBudgetPercentage(percentage);
    config.setInstanceOperationMaintenanceBudget(absolute);
    _configAccessor.setClusterConfig(CLUSTER, config);
  }

  private static void assertStatuses(Map<String, Outcome> outcomes, Status... expected) {
    Assert.assertEquals(outcomes.size(), expected.length, outcomes.toString());
    int i = 0;
    for (Outcome outcome : outcomes.values()) {
      Assert.assertEquals(outcome.getStatus(), expected[i++], outcomes.toString());
    }
  }

  private static void assertBadRequest(Runnable request, String message) {
    try {
      request.run();
      Assert.fail("expected BadRequestException");
    } catch (BadRequestException e) {
      Assert.assertEquals(e.getMessage(), message);
    }
  }

  private static String at(long epochMillis) {
    return Instant.ofEpochMilli(epochMillis).toString();
  }

  private InstanceConfig read(int i) {
    return new InstanceConfig(_accessor.get(path(i), null, AccessOption.PERSISTENT));
  }

  private int version(String instance) {
    Stat stat = _accessor.getStat(PropertyPathBuilder.instanceConfig(CLUSTER, instance),
        AccessOption.PERSISTENT);
    return stat.getVersion();
  }

  private static String path(int i) {
    return PropertyPathBuilder.instanceConfig(CLUSTER, instance(i));
  }

  private static String instance(int i) {
    return "localhost_" + (12000 + i);
  }
}
