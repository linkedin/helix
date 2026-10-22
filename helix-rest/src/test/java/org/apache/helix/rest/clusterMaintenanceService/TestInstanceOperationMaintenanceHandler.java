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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

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
  public void testRenewalTakesNoBudgetAndNeverReordersTheOperationStack() {
    setCap(1, -1);
    apply(SAFE, InstanceOperation.EVACUATE, instance(0));
    long opTimestamp = read(0).getInstanceOperation().getTimestamp();
    // A later operation from another source is now the active one.
    writeOperation(0, InstanceOperation.DISABLE, InstanceOperationSource.USER);
    _expiresAt += HOUR_MS;

    Map<String, Outcome> outcomes =
        apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1));
    assertStatuses(outcomes, Status.APPLIED, Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(), "EVACUATE was already set by "
        + "AUTOMATION, but DISABLE set by USER still applies. Renewed the maintenance marker until "
        + at(_expiresAt) + ".");

    InstanceConfig renewed = read(0);
    Assert.assertEquals(renewed.getInstanceOperationMaintenanceUntilMs(), _expiresAt);
    Assert.assertEquals(renewed.getInstanceOperation().getSource(), InstanceOperationSource.USER);
    Assert.assertEquals(
        renewed.getInstanceOperation(InstanceOperationSource.AUTOMATION).getTimestamp(),
        opTimestamp);
  }

  @Test
  public void testReleaseClearsTheMarkerUnlessAnotherSourceStillHoldsAnOperation() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1), instance(2));
    writeOperation(1, InstanceOperation.DISABLE, InstanceOperationSource.USER);
    writeOperation(2, InstanceOperation.DISABLE, InstanceOperationSource.USER);
    InstanceConfig expired = read(2);
    expired.setInstanceOperationMaintenanceUntilMs(System.currentTimeMillis() - 1);
    _accessor.set(path(2), expired.getRecord(), AccessOption.PERSISTENT);

    // ENABLE needs no expiry and no cluster default.
    Map<String, Outcome> outcomes =
        apply(SAFE, InstanceOperation.ENABLE, instance(0), instance(1), instance(2));
    assertStatuses(outcomes, Status.APPLIED, Status.APPLIED, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getMessage(),
        "ENABLE is now set by AUTOMATION. Cleared the maintenance marker.");
    Assert.assertNull(outcomes.get(instance(0)).getExpiresAtMillis());
    // The operator's DISABLE keeps the instance down, and the outcome says so.
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(), "ENABLE is now set by AUTOMATION, "
        + "but DISABLE set by USER still applies. The maintenance marker stays until "
        + at(_expiresAt) + " because that operation still applies. Send ENABLE again once it is "
        + "lifted to clear the marker sooner.");
    Assert.assertEquals(outcomes.get(instance(1)).getExpiresAtMillis(), Long.valueOf(_expiresAt));
    Assert.assertEquals(outcomes.get(instance(2)).getMessage(), "ENABLE is now set by AUTOMATION, "
        + "but DISABLE set by USER still applies.");
    Assert.assertNull(outcomes.get(instance(2)).getExpiresAtMillis());

    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
    Assert.assertEquals(read(1).getInstanceOperation().getOperation(), InstanceOperation.DISABLE);
    Assert.assertEquals(read(1).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
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
        + "EVACUATE on the 2 instances this request changes would leave partitions unable to "
        + "place all their replicas, including db_0. Change fewer instances at once, or add "
        + "assignable capacity.");
    Assert.assertEquals(outcomes.get(instance(3)).getMessage(), "EVACUATE was already set by "
        + "AUTOMATION. The maintenance marker already runs until " + at(_expiresAt) + ".");
    Assert.assertEquals(outcomes.get("missing").getMessage(),
        "No such instance in cluster " + CLUSTER + ". Remove it from the request.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes), "Applied to 1 of "
        + "4 instances (1 INSTANCE_NOT_FOUND, 2 GUARDRAIL_REJECTED).");
    Assert.assertEquals(judged, Collections.singletonList(Arrays.asList(instance(1), instance(2))));

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
        + "AUTOMATION. The maintenance marker already runs until " + at(_expiresAt) + ".");

    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.DISABLE);
  }

  @Test
  public void testRepeatedRequestOnlyRenews() {
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
        + "AUTOMATION. The maintenance marker already runs until " + at(_expiresAt) + ".");

    Assert.assertEquals(guardrailCalls.get(), 0);
    Assert.assertEquals(read(0).getInstanceOperation().getTimestamp(),
        first.getInstanceOperation().getTimestamp());
    // The same expiry changes nothing, so nothing is written.
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
        + "EVACUATE on the 1 instance this request changes would leave partitions unable to place "
        + "all their replicas, including db_0, db_1, db_2 and more. Change fewer instances at "
        + "once, or add assignable capacity.");
    Assert.assertEquals(InstanceOperationMaintenanceHandler.summarize(outcomes),
        "Applied to 0 of 1 instance (1 GUARDRAIL_REJECTED).");

    // A truncated report ends with a violation that names no partition.
    outcomes = apply(instances -> ValidationResult.of(Arrays.asList(
        Violation.newBuilder("RULE").partition("db_0").build(),
        Violation.newBuilder("RULE").message("Showing the first 1 of 9 partitions ...").build())),
        InstanceOperation.EVACUATE, instance(0));
    Assert.assertTrue(outcomes.get(instance(0)).getMessage().contains("including db_0 and more."));

    // Failures name no partition, and their own text points at force=true, which this endpoint
    // does not take.
    String subject = "EVACUATE on the 1 instance this request changes";
    Assert.assertEquals(guardrailMessage("Could not compute a baseline WAGED assignment for "
        + "cluster c to validate EVACUATE on [h] against (boom). ... retry with force=true ..."),
        "Rejected by guardrail RULE: the cluster's current WAGED assignment cannot be computed, so "
            + subject + " cannot be shown safe. Fix the cluster's WAGED rebalance first.");
    Assert.assertEquals(guardrailMessage("Applying EVACUATE to [h] makes the WAGED rebalancer "
        + "unable to compute an assignment for cluster c (boom). ... retry with force=true ..."),
        "Rejected by guardrail RULE: " + subject + " would leave WAGED unable to compute any "
            + "assignment for the cluster. Change fewer instances at once, or add assignable "
            + "capacity.");
    // Text it does not know is quoted, never guessed at, on one bounded line.
    Assert.assertEquals(guardrailMessage("Some\n new   verdict."),
        "Rejected by guardrail RULE: Some new verdict.");
    char[] longText = new char[600];
    Arrays.fill(longText, 'v');
    String clipped = guardrailMessage(new String(longText));
    Assert.assertEquals(clipped, "Rejected by guardrail RULE: " + new String(longText, 0, 497)
        + "...");
  }

  @Test
  public void testGuardrailThatFailsToEvaluateIsAnError() {
    String failed = "Not written: the rebalance-feasibility guardrail failed to evaluate "
        + "EVACUATE on the 1 instance this request changes. Resend it; if it keeps failing, check "
        + "the Helix REST server log.";
    for (String violation : Arrays.asList("Rule evaluation failed: ZK read timed out",
        "Rule evaluation failed: null", "Rule returned no result.")) {
      Map<String, Outcome> outcomes = apply(instances -> ValidationResult.infeasible(
          Violation.newBuilder("RULE").message(violation).build()), InstanceOperation.EVACUATE,
          instance(0));
      assertStatuses(outcomes, Status.ERROR);
      Assert.assertEquals(outcomes.get(instance(0)).getMessage(), failed);
    }

    // Another rule's verdict still stands.
    Map<String, Outcome> outcomes = apply(instances -> ValidationResult.of(Arrays.asList(
        Violation.newBuilder("BROKEN").message("Rule returned no result.").build(),
        Violation.newBuilder("RULE").partition("db_0").build())), InstanceOperation.EVACUATE,
        instance(0));
    assertStatuses(outcomes, Status.GUARDRAIL_REJECTED);
    Assert.assertTrue(outcomes.get(instance(0)).getMessage().startsWith(
        "Rejected by guardrail RULE: "), outcomes.toString());

    // Only the instances it failed to judge are errors; one that already holds the operation
    // needs no judging and is still applied.
    apply(SAFE, InstanceOperation.EVACUATE, instance(1));
    _expiresAt += HOUR_MS;
    outcomes = apply(instances -> ValidationResult.infeasible(
        Violation.newBuilder("RULE").message("Rule returned no result.").build()),
        InstanceOperation.EVACUATE, "missing", instance(0), instance(1));
    assertStatuses(outcomes, Status.INSTANCE_NOT_FOUND, Status.ERROR, Status.APPLIED);
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
    return new InstanceOperationMaintenanceHandler(zkClient, SAFE).apply(CLUSTER,
        Arrays.asList(instances), null, expiresAt);
  }

  private InstanceOperationMaintenanceHandler handler(
      Function<Collection<String>, ValidationResult> guardrail) {
    return new InstanceOperationMaintenanceHandler(_zkClient, guardrail);
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
