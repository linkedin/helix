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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.constants.InstanceConstants.InstanceOperationSource;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
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
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkTimeoutException;
import org.apache.zookeeper.KeeperException;
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
  private static final Function<Collection<String>, ValidationResult> SAFE =
      instances -> ValidationResult.feasible();

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
  public void testLiveMarkerIsKeptAndTakesNoBudget() {
    setCap(1, -1);
    marker(_expiresAt, instance(0));
    int version = version(instance(0));

    // Neither a later nor an earlier expiry changes a live marker, and the full cap admits it.
    for (long expiresAt : new long[]{_expiresAt + HOUR_MS, _expiresAt - 1}) {
      Map<String, Outcome> outcomes = marker(expiresAt, instance(0));
      assertStatuses(outcomes, Status.APPLIED);
      Assert.assertEquals(outcomes.get(instance(0)).getMessage(),
          "Kept its live maintenance marker, which runs until " + at(_expiresAt) + ".");
      Assert.assertEquals(outcomes.get(instance(0)).getExpiresAtMillis(), Long.valueOf(_expiresAt));
    }
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
    assertBadRequest(() -> handler(SAFE).apply("noSuchCluster",
        Collections.singletonList(instance(0)), null, _expiresAt),
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
        + "future epoch time in milliseconds, 0 for the cluster default, or -1 with no "
        + "instanceOperation to clear the marker.");
    Assert.assertFalse(_accessor.exists("/noSuchCluster", AccessOption.PERSISTENT));
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
  }

  @Test
  public void testOperationAndMarkerCommitTogether() {
    setCap(1, -1);
    int[] before = {version(instance(0)), version(instance(1))};

    // An instance the cap has no room for gets neither the operation nor the marker.
    Map<String, Outcome> outcomes =
        apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1));
    assertStatuses(outcomes, Status.APPLIED, Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "EVACUATE is now set by "
        + "AUTOMATION. Took a maintenance marker until " + at(_expiresAt) + ".");
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.EVACUATE);
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
    Assert.assertEquals(version(instance(0)), before[0] + 1);
    Assert.assertEquals(version(instance(1)), before[1]);
    // A marker alone takes the same budget, and sets no operation.
    assertStatuses(marker(_expiresAt, instance(1)), Status.BUDGET_EXHAUSTED);
    setCap(-1, -1);
    marker(_expiresAt, instance(1));
    Assert.assertNull(read(1).getInstanceOperation(InstanceOperationSource.AUTOMATION));
  }

  @Test
  public void testHeldOperationKeepsItsMarkerAndItsPlaceInTheStack() {
    setCap(1, -1);
    apply(SAFE, InstanceOperation.EVACUATE, instance(0));
    long first = _expiresAt;
    long opTimestamp = read(0).getInstanceOperation().getTimestamp();
    // A later operation from another source is now the active one.
    writeOperation(0, InstanceOperation.DISABLE, InstanceOperationSource.USER);
    _expiresAt += HOUR_MS;

    Map<String, Outcome> outcomes =
        apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1));
    assertStatuses(outcomes, Status.APPLIED, Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "EVACUATE was already set by "
        + "AUTOMATION, but DISABLE set by USER still applies. Kept its live maintenance marker, "
        + "which runs until " + at(first) + ".");

    InstanceConfig held = read(0);
    Assert.assertEquals(held.getInstanceOperationMaintenanceUntilMs(), first);
    Assert.assertEquals(held.getInstanceOperation().getSource(), InstanceOperationSource.USER);
    Assert.assertEquals(
        held.getInstanceOperation(InstanceOperationSource.AUTOMATION).getTimestamp(),
        opTimestamp);
  }

  @Test
  public void testEnableClearsTheMarkerEvenWhileAnotherSourceHoldsAnOperation() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1));
    writeOperation(1, InstanceOperation.DISABLE, InstanceOperationSource.USER);

    // ENABLE needs no expiry and no cluster default.
    Map<String, Outcome> outcomes =
        apply(SAFE, InstanceOperation.ENABLE, instance(0), instance(1));
    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(),
        "ENABLE is now set by AUTOMATION. Cleared the maintenance marker.");
    // The operator's DISABLE keeps the instance down, and the outcome says so.
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(), "ENABLE is now set by AUTOMATION, "
        + "but DISABLE set by USER still applies. Cleared the maintenance marker.");
    for (int i = 0; i < 2; i++) {
      Assert.assertNull(outcomes.get(instance(i)).getExpiresAtMillis());
      Assert.assertEquals(read(i).getInstanceOperationMaintenanceUntilMs(),
          InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
    }
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);
    Assert.assertEquals(read(1).getInstanceOperation().getOperation(), InstanceOperation.DISABLE);
  }

  @Test
  public void testRejectedInstancesAreNotWritten() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(3));
    // SWAP_IN cannot move to EVACUATE, which is checked before the budget a cap of 0 exhausts.
    writeOperation(0, InstanceOperation.SWAP_IN, InstanceOperationSource.USER);
    setCap(0, -1);
    int[] before = {version(instance(0)), version(instance(1)), version(instance(3))};
    List<Collection<String>> judged = new ArrayList<>();
    Function<Collection<String>, ValidationResult> guardrail = instances -> {
      judged.add(new ArrayList<>(instances));
      return ValidationResult.infeasible(Violation.newBuilder("RULE").resource("db")
          .partition("db_0").message("retry with force=true").build());
    };

    Map<String, Outcome> outcomes = apply(guardrail, InstanceOperation.EVACUATE, "missing",
        instance(0), instance(1));
    assertStatuses(outcomes, Status.INSTANCE_NOT_FOUND, Status.INVALID_TRANSITION,
        Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Cannot change to EVACUATE from "
        + "its current operation SWAP_IN, set by USER. Valid transitions from SWAP_IN: [SWAP_IN, "
        + "UNKNOWN]. An operator must resolve this before the instance is resent.");
    Assert.assertTrue(judged.isEmpty());
    setCap(-1, -1);
    // The admitted part is judged despite the missing instance, so every rejection shows at once.
    // Instance 3 already holds the operation and the marker, so it is applied as is, unjudged.
    outcomes = apply(guardrail, InstanceOperation.EVACUATE, "missing", instance(1), instance(2),
        instance(3));
    assertStatuses(outcomes, Status.INSTANCE_NOT_FOUND, Status.GUARDRAIL_REJECTED,
        Status.GUARDRAIL_REJECTED, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(), "Rejected by guardrail RULE: "
        + "EVACUATE on this instance alone would leave partitions unable to place all their "
        + "replicas, including db_0. Add assignable capacity before resending it.");
    Assert.assertEquals(outcomes.get(instance(2)).getMessage(),
        outcomes.get(instance(1)).getMessage());
    Assert.assertEquals(outcomes.get(instance(3)).getMessage(), "EVACUATE was already set by "
        + "AUTOMATION. Kept its live maintenance marker, which runs until " + at(_expiresAt) + ".");
    Assert.assertEquals(outcomes.get("missing").getMessage(),
        "No such instance in cluster " + CLUSTER + ". Check the instance name, and whether it "
            + "has been added to the cluster.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes), "Applied to 1 of "
        + "4 instances (1 INSTANCE_NOT_FOUND, 2 GUARDRAIL_REJECTED).");
    // Neither fits even alone, so each was probed once more after the two were judged together.
    Assert.assertEquals(judged, Arrays.asList(Arrays.asList(instance(1), instance(2)),
        Collections.singletonList(instance(1)), Collections.singletonList(instance(2))));

    Assert.assertEquals(version(instance(0)), before[0]);
    Assert.assertEquals(version(instance(1)), before[1]);
    Assert.assertEquals(version(instance(3)), before[2]);
  }

  @Test
  public void testInstancesSharingALogicalIdCannotChangeTogether() {
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER);
    config.setTopologyAwareEnabled(true);
    config.setTopology("/zone/host");
    config.setFaultZoneType("zone");
    _configAccessor.setClusterConfig(CLUSTER, config);
    for (int i = 0; i < 3; i++) {
      InstanceConfig instance = read(i);
      instance.setDomain("zone=z0,host=" + (i < 2 ? "shared" : "h" + i));
      _accessor.set(path(i), instance.getRecord(), AccessOption.PERSISTENT);
    }

    Map<String, Outcome> outcomes =
        apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1), instance(2));
    assertStatuses(outcomes, Status.APPLIED, Status.DUPLICATE_LOGICAL_ID, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(), "Shares logical ID shared with "
        + instance(0) + ", which this request also changes. Send it in a separate request.");
    Assert.assertEquals(read(2).getInstanceOperation().getOperation(), InstanceOperation.EVACUATE);
    Assert.assertTrue(read(1).isAssignable());

    // Releasing waits for a swap on the same logical ID, and says which instance holds it.
    InstanceConfig swapping = read(1);
    swapping.setInstanceOperation(new InstanceConfig.InstanceOperation.Builder()
        .setOperation(InstanceOperation.SWAP_IN).setSource(InstanceOperationSource.USER)
        .setReason("host swap").build());
    _accessor.set(path(1), swapping.getRecord(), AccessOption.PERSISTENT);
    outcomes = apply(SAFE, InstanceOperation.ENABLE, instance(0));
    assertStatuses(outcomes, Status.INVALID_TRANSITION);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Cannot change to ENABLE from its "
        + "current operation EVACUATE, set by AUTOMATION. All matching logical ID instances must "
        + "be in UNKNOWN state. Matching instances: " + instance(1) + " (operation=SWAP_IN). An "
        + "operator must resolve this before the instance is resent.");
  }

  @Test
  public void testRepeatedDisableUndoesALegacyEnable() {
    apply(SAFE, InstanceOperation.DISABLE, instance(0));
    InstanceConfig legacyEnabled = read(0);
    legacyEnabled.setInstanceEnabled(true);
    _accessor.set(path(0), legacyEnabled.getRecord(), AccessOption.PERSISTENT);
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);

    Map<String, Outcome> outcomes = apply(SAFE, InstanceOperation.DISABLE, instance(0));
    assertStatuses(outcomes, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "DISABLE is now set by "
        + "AUTOMATION. Kept its live maintenance marker, which runs until " + at(_expiresAt) + ".");

    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.DISABLE);
  }

  @Test
  public void testRepeatedRequestWritesNothing() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(0));
    InstanceConfig first = read(0);
    int version = version(instance(0));
    AtomicInteger guardrailCalls = new AtomicInteger();

    Map<String, Outcome> outcomes = apply(instances -> {
      guardrailCalls.incrementAndGet();
      return ValidationResult.feasible();
    }, InstanceOperation.EVACUATE, instance(0));
    assertStatuses(outcomes, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "EVACUATE was already set by "
        + "AUTOMATION. Kept its live maintenance marker, which runs until " + at(_expiresAt) + ".");

    Assert.assertEquals(guardrailCalls.get(), 0);
    Assert.assertEquals(read(0).getInstanceOperation().getTimestamp(),
        first.getInstanceOperation().getTimestamp());
    // The repeat changes nothing, so nothing is written.
    Assert.assertEquals(version(instance(0)), version);
  }

  @Test
  public void testInstanceHeldAsIsThatChangesBeforeTheCommitIsJudgedAgain() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(0));
    List<Integer> judged = new ArrayList<>();

    // An admin enable clears the held EVACUATE after the request judged it as held.
    Map<String, Outcome> outcomes = apply(instances -> {
      if (judged.isEmpty()) {
        writeOperation(0, InstanceOperation.ENABLE, InstanceOperationSource.ADMIN);
      }
      judged.add(instances.size());
      return ValidationResult.feasible();
    }, InstanceOperation.EVACUATE, instance(0), instance(1));

    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED);
    Assert.assertEquals(judged, Arrays.asList(1, 2));
    Assert.assertTrue(outcomes.get(instance(0)).getMessage()
        .startsWith("EVACUATE is now set by AUTOMATION."), outcomes.toString());
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(),
        InstanceOperation.EVACUATE);
  }

  @Test
  public void testGuardrailMessageNamesAtMostThreePartitions() {
    List<Violation> violations = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      violations.add(Violation.newBuilder("RULE").partition("db_" + i).build());
    }
    Map<String, Outcome> outcomes = apply(instances -> ValidationResult.of(violations),
        InstanceOperation.EVACUATE, instance(0));
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Rejected by guardrail RULE: "
        + "EVACUATE on this instance alone would leave partitions unable to place all their "
        + "replicas, including db_0, db_1, db_2 and more. Add assignable capacity before "
        + "resending it.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        "Applied to 0 of 1 instance (1 GUARDRAIL_REJECTED).");

    // A truncated report ends with a violation that names no partition.
    outcomes = apply(instances -> ValidationResult.of(Arrays.asList(
        Violation.newBuilder("RULE").partition("db_0").build(),
        Violation.newBuilder("RULE").message("Showing the first 1 of 9 partitions ...").build())),
        InstanceOperation.EVACUATE, instance(0));
    Assert.assertTrue(outcomes.get(instance(0)).getMessage().contains("including db_0 and more."));

    outcomes = apply(instances -> ValidationResult.of(violations.subList(0, 3)),
        InstanceOperation.EVACUATE, instance(0));
    Assert.assertTrue(outcomes.get(instance(0)).getMessage()
        .contains("including db_0, db_1 and db_2."), outcomes.toString());

    // Failures name no partition, and their own text points at force=true, which this endpoint
    // does not take.
    String advice = " Add assignable capacity before resending it.";
    Assert.assertEquals(guardrailMessage("Applying EVACUATE to [h] makes the WAGED rebalancer "
        + "unable to compute an assignment for cluster c (boom). ... retry with force=true ..."),
        "Rejected by guardrail RULE: EVACUATE on this instance alone would leave WAGED unable to "
            + "compute any assignment for the cluster." + advice);
    // Text it does not know is quoted, never guessed at, on one bounded line.
    Assert.assertEquals(guardrailMessage("Some\n new   verdict."),
        "Rejected by guardrail RULE: Some new verdict." + advice);
    char[] longText = new char[600];
    Arrays.fill(longText, 'v');
    String clipped = guardrailMessage(new String(longText));
    Assert.assertEquals(clipped, "Rejected by guardrail RULE: " + new String(longText, 0, 497)
        + "..." + advice);
  }

  @Test
  public void testGuardrailThatCannotJudgeWritesNothing() {
    String failed = "Not written: the rebalance-feasibility guardrail could not judge EVACUATE "
        + "on the 1 instance this request changes. Resend it once; if it comes back, the "
        + "cluster's WAGED rebalance must be fixed.";
    Map<String, Outcome> outcomes = apply(instances -> ValidationResult.infeasible(
        Violation.newBuilder("RULE").indeterminate().message("Rule returned no result.").build()),
        InstanceOperation.EVACUATE, instance(0));
    assertStatuses(outcomes, Status.GUARDRAIL_INDETERMINATE);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), failed);
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        "Applied to 0 of 1 instance (1 GUARDRAIL_INDETERMINATE).");

    // Another rule's verdict still stands.
    outcomes = apply(instances -> ValidationResult.of(Arrays.asList(
        Violation.newBuilder("BROKEN").indeterminate().message("boom").build(),
        Violation.newBuilder("RULE").partition("db_0").build())), InstanceOperation.EVACUATE,
        instance(0));
    assertStatuses(outcomes, Status.GUARDRAIL_REJECTED);
    Assert.assertTrue(outcomes.get(instance(0)).getMessage().startsWith(
        "Rejected by guardrail RULE: "), outcomes.toString());

    // Only the instances it could not judge are held back; one that already holds the operation
    // needs no judging and is still applied.
    apply(SAFE, InstanceOperation.EVACUATE, instance(1));
    outcomes = apply(instances -> ValidationResult.infeasible(
        Violation.newBuilder("RULE").indeterminate().message("boom").build()),
        InstanceOperation.EVACUATE, "missing", instance(0), instance(1));
    assertStatuses(outcomes, Status.INSTANCE_NOT_FOUND, Status.GUARDRAIL_INDETERMINATE,
        Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), failed);
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);
    Assert.assertEquals(read(1).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
  }

  @Test
  public void testGuardrailRejectionGivesBackTheCapRoomItTook() {
    // Instance 5 already holds EVACUATE, so it takes a marker without being judged.
    apply(SAFE, InstanceOperation.EVACUATE, instance(5));
    InstanceConfig expired = read(5);
    expired.setInstanceOperationMaintenanceUntilMs(System.currentTimeMillis() - 1);
    _accessor.set(path(5), expired.getRecord(), AccessOption.PERSISTENT);
    setCap(2, -1);
    Function<Collection<String>, ValidationResult> rejectAll = instances ->
        ValidationResult.infeasible(Violation.newBuilder("RULE").partition("db_0").build());

    // Instances 5 and 0 take the room; the guardrail rejects 0, so 1 could have taken its room.
    Map<String, Outcome> outcomes = apply(rejectAll, InstanceOperation.EVACUATE, instance(5),
        instance(0), instance(1), instance(2), instance(3));

    assertStatuses(outcomes, Status.APPLIED, Status.GUARDRAIL_REJECTED, Status.NOT_APPLIED,
        Status.BUDGET_EXHAUSTED, Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(), "Not judged: the marker cap had no "
        + "room for it until instances ahead of it in this request were not written. Resend it.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes), "Applied to 1 of "
        + "5 instances (1 GUARDRAIL_REJECTED, 2 BUDGET_EXHAUSTED, 1 NOT_APPLIED).");
    Assert.assertEquals(read(5).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
    Assert.assertTrue(read(0).isAssignable());
  }

  @Test
  public void testGuardrailRejectionCommitsTheInstancesThatFit() {
    int before = version(instance(5));
    List<Collection<String>> judged = new ArrayList<>();
    Map<String, Outcome> outcomes = apply(capacity(5, 3, judged), InstanceOperation.EVACUATE,
        instance(0), instance(1), instance(2), instance(3), instance(4), instance(5));

    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED, Status.APPLIED, Status.APPLIED,
        Status.APPLIED, Status.GUARDRAIL_REJECTED);
    Assert.assertEquals(outcomes.get(instance(5)).getMessage(), "Rejected by guardrail RULE: "
        + "EVACUATE on this instance together with 5 instances of this request that fit would "
        + "leave partitions unable to place all their replicas, including db_0. Resend it once "
        + "the instances that fit are done.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        "Applied to 5 of 6 instances (1 GUARDRAIL_REJECTED).");
    // The whole request, then halving: 3, 4 and 5 instances.
    Assert.assertEquals(sizes(judged), Arrays.asList(6, 3, 4, 5));
    Assert.assertEquals(read(4).getInstanceOperation().getOperation(), InstanceOperation.EVACUATE);
    Assert.assertEquals(version(instance(5)), before);
  }

  @Test
  public void testInstanceThatDoesNotFitDoesNotBlockTheOnesAfterIt() {
    // Instances 0 to 2 share a zone that can drain only two of them.
    List<Collection<String>> judged = new ArrayList<>();
    Map<String, Outcome> outcomes = apply(capacity(6, 2, judged), InstanceOperation.EVACUATE,
        instance(0), instance(1), instance(2), instance(3), instance(4));

    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED, Status.GUARDRAIL_REJECTED,
        Status.APPLIED, Status.APPLIED);
    Assert.assertTrue(outcomes.get(instance(2)).getMessage().contains("EVACUATE on this instance "
        + "together with 2 instances of this request that fit would leave"));
    Assert.assertEquals(judged.get(judged.size() - 1),
        Arrays.asList(instance(0), instance(1), instance(3), instance(4)));
  }

  @Test
  public void testProbeThatCannotJudgeWritesNoneOfTheChangingInstances() {
    // Even after a probe showed some of them fit, a check that cannot judge ends the request.
    List<Collection<String>> probed = new ArrayList<>();
    Map<String, Outcome> outcomes = new InstanceOperationMaintenanceHandler(_zkClient,
        capacity(0, 3, new ArrayList<>()), instances -> {
          probed.add(new ArrayList<>(instances));
          return probed.size() == 1 ? ValidationResult.feasible() : ValidationResult.infeasible(
              Violation.newBuilder("RULE").indeterminate().message("timed out").build());
        }, null).apply(CLUSTER, Arrays.asList(instance(0), instance(1), instance(2),
        instance(3)), operation(InstanceOperation.EVACUATE), _expiresAt);

    Status status = Status.GUARDRAIL_INDETERMINATE;
    assertStatuses(outcomes, status, status, status, status);
    Assert.assertTrue(outcomes.get(instance(0)).getMessage()
        .contains("EVACUATE on the 4 instances this request changes"));
    Assert.assertTrue(read(0).isAssignable());
    Assert.assertEquals(sizes(probed), Arrays.asList(2, 3));
  }

  @Test
  public void testInstancesThatDoNotFitGiveBackTheirCapRoom() {
    setCap(3, -1);
    Map<String, Outcome> outcomes = apply(capacity(1, 3, new ArrayList<>()),
        InstanceOperation.EVACUATE, instance(0), instance(1), instance(2), instance(3),
        instance(4));

    // Only instance 0 fits, so the two the cap turned away get their room and are NOT_APPLIED.
    assertStatuses(outcomes, Status.APPLIED, Status.GUARDRAIL_REJECTED,
        Status.GUARDRAIL_REJECTED, Status.NOT_APPLIED, Status.NOT_APPLIED);
    Assert.assertTrue(outcomes.get(instance(1)).getMessage().contains("EVACUATE on this instance "
        + "together with 1 instance of this request that fits would leave"));
  }

  @Test
  public void testRequestOutOfTimeWritesNoneItJudgedAndResendsMakeProgress() {
    // Each guardrail call takes 25 s, so a request makes one call before its budget runs out.
    AtomicLong now = new AtomicLong();
    List<Collection<String>> judged = new ArrayList<>();
    Function<Collection<String>, ValidationResult> capacity = capacity(3, 3, judged);
    Function<Collection<String>, ValidationResult> slow = instances -> {
      now.addAndGet(25_000L);
      return capacity.apply(instances);
    };
    InstanceOperationMaintenanceHandler handler =
        new InstanceOperationMaintenanceHandler(_zkClient, slow, slow, null, now::get);
    List<String> pending = Arrays.asList(instance(0), instance(1), instance(2), instance(3),
        instance(4), instance(5));
    int before = version(instance(0));
    List<String> fit = null;
    Map<Status, String> messages = new EnumMap<>(Status.class);
    for (int request = 0; !pending.isEmpty() && request < 10; request++) {
      Map<String, Outcome> outcomes = handler.apply(CLUSTER, pending,
          operation(InstanceOperation.EVACUATE), _expiresAt, fit);
      outcomes.values().forEach(o -> messages.putIfAbsent(o.getStatus(), o.getMessage()));
      fit = pending.stream().filter(instance -> outcomes.get(instance).getStatus() == Status.FITS)
          .collect(Collectors.toList());
      pending = pending.stream().filter(instance -> EnumSet.of(Status.FITS, Status.NOT_CHECKED)
          .contains(outcomes.get(instance).getStatus())).collect(Collectors.toList());
    }

    // Resent with an empty fitInstances, the search resumes at one instance. A FITS run and the
    // next instance that do not fit reject that instance, and the run is written once no
    // instance is left after it.
    Assert.assertEquals(sizes(judged), Arrays.asList(6, 1, 2, 3, 4, 4, 4, 3));
    Assert.assertEquals(messages.get(Status.FITS), "Shown to fit together with the other FITS "
        + "instances, but not written: the request ran out of time before the search for the "
        + "instances that fit ended. Resend it in fitInstances, with the NOT_CHECKED instances.");
    Assert.assertEquals(messages.get(Status.NOT_CHECKED), "Not checked, so not written: the "
        + "request ran out of time before the search for the instances that fit reached it. "
        + "Resend it, with the FITS instances in fitInstances, an empty list if there are none.");
    Assert.assertEquals(messages.get(Status.GUARDRAIL_REJECTED), "Rejected by guardrail RULE: "
        + "EVACUATE on this instance together with the 3 FITS instances would leave partitions "
        + "unable to place all their replicas, including db_0. Resend it once the instances that "
        + "fit are done.");
    Assert.assertEquals(version(instance(0)), before + 1);
    Assert.assertTrue(read(3).isAssignable());

    // An attempt after a lost race does not start a call that would end past the budget.
    before = version(instance(5));
    AtomicInteger calls = new AtomicInteger();
    RealmAwareZkClient beaten = withMulti(ops -> {
      touchClusterConfig();
      _zkClient.multi(ops);
    });
    Map<String, Outcome> outcomes = new InstanceOperationMaintenanceHandler(beaten, instances -> {
      calls.incrementAndGet();
      now.addAndGet(30_000L);
      return ValidationResult.feasible();
    }, SAFE, null, now::get).apply(CLUSTER, Collections.singletonList(instance(5)),
        operation(InstanceOperation.EVACUATE), _expiresAt);

    assertStatuses(outcomes, Status.CONFLICT);
    Assert.assertEquals(outcomes.get(instance(5)).getMessage(), "Concurrent writes to this "
        + "cluster won the race, and too little time was left to run the guardrail again. Resend "
        + "the request.");
    Assert.assertEquals(calls.get(), 1);
    Assert.assertEquals(version(instance(5)), before);
  }

  @Test
  public void testFitInstancesGoFirstAndAreCheckedAgain() {
    // They are checked with the next instance, then alone once that fails.
    List<Collection<String>> judged = new ArrayList<>();
    Map<String, Outcome> outcomes = handler(capacity(3, 3, judged)).apply(CLUSTER,
        Arrays.asList(instance(0), instance(1), instance(2), instance(3), instance(4)),
        operation(InstanceOperation.EVACUATE), _expiresAt,
        Arrays.asList(instance(3), instance(4), instance(0)));
    assertStatuses(outcomes, Status.APPLIED, Status.GUARDRAIL_REJECTED,
        Status.GUARDRAIL_REJECTED, Status.APPLIED, Status.APPLIED);
    Assert.assertEquals(judged.get(0),
        Arrays.asList(instance(0), instance(3), instance(4), instance(1)));
    Assert.assertEquals(sizes(judged), Arrays.asList(4, 3, 4));

    // A hint that no longer fits is dropped, and the search starts over within it.
    beforeMethod();
    judged.clear();
    outcomes = handler(capacity(2, 3, judged)).apply(CLUSTER, Arrays.asList(instance(0),
        instance(1), instance(2), instance(3), instance(4)),
        operation(InstanceOperation.EVACUATE), _expiresAt,
        Arrays.asList(instance(0), instance(1), instance(2)));
    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED, Status.GUARDRAIL_REJECTED,
        Status.GUARDRAIL_REJECTED, Status.GUARDRAIL_REJECTED);
    Assert.assertEquals(sizes(judged), Arrays.asList(4, 3, 1, 2, 4));
    Assert.assertTrue(read(2).isAssignable());
  }

  @Test
  public void testFitSearchMakesAtMostTheConfiguredChecks() {
    setMaxChecks(1);
    List<Collection<String>> judged = new ArrayList<>();
    Map<String, Outcome> outcomes = apply(capacity(1, 3, judged), InstanceOperation.EVACUATE,
        instance(0), instance(1), instance(2), instance(3), instance(4));

    // The whole request, then the last check goes to the first instance alone, as none fit yet.
    assertStatuses(outcomes, Status.APPLIED, Status.GUARDRAIL_REJECTED,
        Status.GUARDRAIL_REJECTED, Status.GUARDRAIL_REJECTED, Status.GUARDRAIL_REJECTED);
    Assert.assertEquals(outcomes.get(instance(4)).getMessage(), "Not admitted: the search did not "
        + "show it fits together with the 1 instance of this request that fits. Resend it once "
        + "the instances that fit are done.");
    Assert.assertEquals(sizes(judged), Arrays.asList(5, 1));

    // 0 turns the search off and the request is judged as a whole, fitInstances or not.
    setMaxChecks(0);
    judged.clear();
    outcomes = handler(capacity(1, 3, judged)).apply(CLUSTER, Arrays.asList(instance(4),
        instance(5)), operation(InstanceOperation.EVACUATE), _expiresAt,
        Collections.singletonList(instance(5)));
    assertStatuses(outcomes, Status.GUARDRAIL_REJECTED, Status.GUARDRAIL_REJECTED);
    Assert.assertEquals(outcomes.get(instance(4)).getMessage(), "Rejected by guardrail RULE: "
        + "EVACUATE on the 2 instances this request changes would leave partitions unable to "
        + "place all their replicas, including db_0. Change fewer instances at once, or add "
        + "assignable capacity.");
    Assert.assertEquals(judged, Collections.singletonList(Arrays.asList(instance(4), instance(5))));
  }

  private void setMaxChecks(int maxChecks) {
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER);
    config.setInstanceOperationMaintenanceFitSearchMaxChecks(maxChecks);
    _configAccessor.setClusterConfig(CLUSTER, config);
  }

  private static List<Integer> sizes(List<Collection<String>> judged) {
    return judged.stream().map(Collection::size).collect(Collectors.toList());
  }

  /** Feasible for at most max instances, at most zoneMax of them among instances 0 to 2. */
  private static Function<Collection<String>, ValidationResult> capacity(int max, int zoneMax,
      List<Collection<String>> judged) {
    List<String> zone = Arrays.asList(instance(0), instance(1), instance(2));
    return instances -> {
      judged.add(new ArrayList<>(instances));
      return instances.size() <= max && instances.stream().filter(zone::contains).count() <= zoneMax
          ? ValidationResult.feasible()
          : ValidationResult.infeasible(Violation.newBuilder("RULE").partition("db_0").build());
    };
  }

  @Test
  public void testQuotedTextIsOneBoundedLine() {
    char[] pad = new char[300];
    Arrays.fill(pad, 'y');
    String reason = "line one\nline two " + new String(pad);
    InstanceConfig config = read(0);
    config.setInstanceOperation(new InstanceConfig.InstanceOperation.Builder()
        .setOperation(InstanceOperation.SWAP_IN).setSource(InstanceOperationSource.USER)
        .setReason(reason).build());
    _accessor.set(path(0), config.getRecord(), AccessOption.PERSISTENT);

    Map<String, Outcome> outcomes = apply(SAFE, InstanceOperation.EVACUATE, instance(0));

    assertStatuses(outcomes, Status.INVALID_TRANSITION);
    String shown = ("line one line two " + new String(pad)).substring(0, 197) + "...";
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Cannot change to EVACUATE from "
        + "its current operation SWAP_IN, set by USER (reason: " + shown + "). Valid transitions "
        + "from SWAP_IN: [SWAP_IN, UNKNOWN]. An operator must resolve this before the instance is "
        + "resent.");
  }

  private String guardrailMessage(String violation) {
    return apply(instances -> ValidationResult.infeasible(Violation.newBuilder("RULE")
        .message(violation).build()), InstanceOperation.EVACUATE, instance(0)).get(instance(0))
        .getMessage();
  }

  @Test
  public void testBadOperationRequests() {
    InstanceOperationMaintenanceHandler handler = handler(SAFE);
    List<String> one = Collections.singletonList(instance(0));
    assertBadRequest(() -> handler.apply(CLUSTER, one, operation(InstanceOperation.EVACUATE),
        InstanceOperationMaintenanceHandler.CLEAR_MARKER), "expiresAtMillis -1 clears the marker, "
        + "so it cannot go with EVACUATE. Give a future expiry, or 0 for the cluster default.");
    assertBadRequest(() -> handler.apply(CLUSTER, one, operation(InstanceOperation.SWAP_IN),
        _expiresAt), "instanceOperation must be one of [ENABLE, DISABLE, EVACUATE] and "
        + "instanceOperationSource one of [USER, AUTOMATION], given together; got SWAP_IN and "
        + "AUTOMATION.");
    assertBadRequest(() -> handler.apply(CLUSTER, one, operation(InstanceOperation.UNKNOWN),
        _expiresAt));
    for (InstanceOperationSource unsupported : Arrays.asList(InstanceOperationSource.ADMIN,
        InstanceOperationSource.DEFAULT)) {
      assertBadRequest(() -> handler.apply(CLUSTER, one, new InstanceConfig.InstanceOperation
          .Builder().setOperation(InstanceOperation.EVACUATE).setSource(unsupported).build(),
          _expiresAt));
    }
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);
  }

  @Test
  public void testUnexpectedFailureWritesNothing() {
    int before = version(instance(0));

    Map<String, Outcome> outcomes = apply(instances -> {
      throw new IllegalStateException("guardrail failed at org.apache.helix.Foo");
    }, InstanceOperation.EVACUATE, instance(0), instance(1));

    assertStatuses(outcomes, Status.ERROR, Status.ERROR);
    // Java exception text stays in the server log.
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(), "Nothing was written: the request "
        + "failed. Resend the request; if it keeps failing, check the Helix REST server log.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        outcomes.get(instance(1)).getMessage());
    Assert.assertEquals(version(instance(0)), before);

    // A ZooKeeper failure is named by its error code, however deep it is wrapped.
    outcomes = apply(instances -> {
      throw new IllegalStateException(
          new ZkException(KeeperException.create(KeeperException.Code.NOAUTH, "/x")));
    }, InstanceOperation.EVACUATE, instance(0));
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Nothing was written: the request "
        + "failed (ZooKeeper error NOAUTH). Resend the request; if it keeps failing, check the "
        + "Helix REST server log.");
    outcomes = apply(instances -> {
      throw new ZkTimeoutException("Unable to connect to zookeeper server within timeout: 30000");
    }, InstanceOperation.EVACUATE, instance(0));
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Nothing was written: the request "
        + "failed (ZooKeeper timed out). Resend the request; if it keeps failing, check the Helix "
        + "REST server log.");
    Assert.assertEquals(version(instance(0)), before);
  }

  @Test
  public void testRecordsBatchAndCommitSizes() {
    MetricRegistry metrics = new MetricRegistry();
    InstanceOperationMaintenanceHandler handler =
        new InstanceOperationMaintenanceHandler(_zkClient, SAFE, SAFE, metrics);
    handler.apply(CLUSTER, Arrays.asList(instance(0), instance(1), instance(1)),
        operation(InstanceOperation.EVACUATE), _expiresAt);
    // A repeat writes nothing, so it records a batch but no commit.
    handler.apply(CLUSTER, Collections.singletonList(instance(0)),
        operation(InstanceOperation.EVACUATE), _expiresAt);

    Histogram batches = metrics.histogram(InstanceOperationMaintenanceHandler.BATCH_SIZE_METRIC);
    Assert.assertEquals(batches.getCount(), 2);
    Assert.assertEquals(batches.getSnapshot().getMax(), 2);
    Histogram commits = metrics.histogram(InstanceOperationMaintenanceHandler.COMMIT_BYTES_METRIC);
    Assert.assertEquals(commits.getCount(), 1);
    Assert.assertEquals(commits.getSnapshot().getMax(), dataLength(0) + dataLength(1));
  }

  @Test
  public void testFailureAfterACommitWasSentIsUnknown() {
    RealmAwareZkClient failingMulti = withMulti(ops -> {
      throw new ZkException(KeeperException.create(KeeperException.Code.CONNECTIONLOSS));
    });
    Map<String, Outcome> outcomes = new InstanceOperationMaintenanceHandler(failingMulti, SAFE,
        SAFE, null).apply(CLUSTER, Collections.singletonList(instance(0)),
            operation(InstanceOperation.EVACUATE), _expiresAt);
    assertStatuses(outcomes, Status.ERROR);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Failed after the commit was sent "
        + "(ZooKeeper error CONNECTIONLOSS), so whether it was written is unknown. Resend the "
        + "request to settle it.");

    // A commit that lost a race may have landed before a connection loss, so a failure on the
    // next attempt is unknown too.
    AtomicInteger calls = new AtomicInteger();
    outcomes = apply(instances -> {
      if (calls.incrementAndGet() == 1) {
        touchClusterConfig();
        return ValidationResult.feasible();
      }
      throw new IllegalStateException("connection lost");
    }, InstanceOperation.EVACUATE, instance(0));
    assertStatuses(outcomes, Status.ERROR);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Failed after the commit was sent, "
        + "so whether it was written is unknown. Resend the request to settle it.");
  }

  @Test
  public void testCommitThatLandedButWasReportedLostIsFoundApplied() {
    // As when the client resends a multi after a connection loss and the resend finds its own
    // write: the multi lands, then reports a version conflict.
    AtomicInteger commits = new AtomicInteger();
    RealmAwareZkClient landsThenConflicts = withMulti(ops -> {
      commits.incrementAndGet();
      _zkClient.multi(ops);
      throw new ZkBadVersionException("resent after a connection loss");
    });
    int before = version(instance(0));

    Map<String, Outcome> outcomes = new InstanceOperationMaintenanceHandler(landsThenConflicts,
        SAFE, SAFE, null).apply(CLUSTER, Collections.singletonList(instance(0)),
        operation(InstanceOperation.EVACUATE), _expiresAt);

    // The next attempt finds the request already applied and writes nothing more.
    assertStatuses(outcomes, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "EVACUATE was already set by "
        + "AUTOMATION. Kept its live maintenance marker, which runs until " + at(_expiresAt) + ".");
    Assert.assertEquals(commits.get(), 1);
    Assert.assertEquals(version(instance(0)), before + 1);
  }

  @Test
  public void testCommitOverTheSizeLimitIsABadRequest() {
    char[] pad = new char[InstanceOperationMaintenanceHandler.MAX_COMMIT_BYTES / 5];
    Arrays.fill(pad, 'x');
    for (int i = 0; i < 6; i++) {
      ZNRecord record = read(i).getRecord();
      record.setSimpleField("PAD", new String(pad));
      _accessor.set(path(i), record, AccessOption.PERSISTENT);
    }
    int before = version(instance(0));

    assertBadRequest(() -> apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1),
        instance(2), instance(3), instance(4), instance(5)));
    Assert.assertEquals(version(instance(0)), before);
    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(0)), Status.APPLIED);
  }

  @Test
  public void testExpiryPassingBeforeTheCommitWritesNothing() {
    int before = version(instance(0));
    long expiresAt = System.currentTimeMillis() + 500L;
    Function<Collection<String>, ValidationResult> slowGuardrail = instances -> {
      while (System.currentTimeMillis() <= expiresAt) {
        try {
          Thread.sleep(10L);
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
      return ValidationResult.feasible();
    };

    Map<String, Outcome> outcomes = handler(slowGuardrail).apply(CLUSTER,
        Collections.singletonList(instance(0)), operation(InstanceOperation.EVACUATE), expiresAt);

    assertStatuses(outcomes, Status.ERROR);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Nothing was written: the marker "
        + "expiry " + at(expiresAt) + " passed before the commit. Resend with a later expiry.");
    Assert.assertEquals(version(instance(0)), before);

    // A cluster default too short to outlast the request says so.
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER);
    config.setDefaultInstanceOperationMaintenanceDurationMs(300L);
    _configAccessor.setClusterConfig(CLUSTER, config);
    outcomes = handler(instances -> {
      try {
        Thread.sleep(600L);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      return ValidationResult.feasible();
    }).apply(CLUSTER, Collections.singletonList(instance(0)),
        operation(InstanceOperation.EVACUATE), InstanceOperationMaintenanceHandler.DEFAULT_EXPIRY);
    assertStatuses(outcomes, Status.ERROR);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "Nothing was written: the cluster "
        + "default marker duration of 300 ms ran out before the commit. Give an expiry, or raise "
        + "DEFAULT_INSTANCE_OPERATION_MAINTENANCE_DURATION_MS for cluster " + CLUSTER + ".");
    Assert.assertEquals(version(instance(0)), before);
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

  private Map<String, Outcome> apply(Function<Collection<String>, ValidationResult> guardrail,
      InstanceOperation operation, String... instances) {
    return handler(guardrail).apply(CLUSTER, Arrays.asList(instances), operation(operation),
        _expiresAt);
  }

  private Map<String, Outcome> marker(long expiresAt, String... instances) {
    return marker(_zkClient, expiresAt, instances);
  }

  private Map<String, Outcome> marker(RealmAwareZkClient zkClient, long expiresAt,
      String... instances) {
    return new InstanceOperationMaintenanceHandler(zkClient, SAFE, SAFE, null).apply(CLUSTER,
        Arrays.asList(instances), null, expiresAt);
  }

  private InstanceOperationMaintenanceHandler handler(
      Function<Collection<String>, ValidationResult> guardrail) {
    return new InstanceOperationMaintenanceHandler(_zkClient, guardrail, guardrail, null);
  }

  private static InstanceConfig.InstanceOperation operation(InstanceOperation operation) {
    return new InstanceConfig.InstanceOperation.Builder().setOperation(operation)
        .setSource(InstanceOperationSource.AUTOMATION).build();
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

  private static void assertBadRequest(Runnable request) {
    try {
      request.run();
      Assert.fail("expected BadRequestException");
    } catch (BadRequestException expected) {
      // expected
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

  private long dataLength(int i) {
    return _accessor.getStat(path(i), AccessOption.PERSISTENT).getDataLength();
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
