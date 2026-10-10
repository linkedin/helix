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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkServer;
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
  private static final Function<Collection<String>, Optional<String>> SAFE =
      instances -> Optional.empty();

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
  public void testOverTheCapWritesNothingAndTheAdmittedPartCommitsAtOnce() {
    setCap(2, -1);
    int[] before = {version(instance(0)), version(instance(1)), version(instance(2))};
    String fence = InstanceOperationMaintenanceHandler.fencePath(CLUSTER);

    Map<String, Outcome> outcomes = apply(SAFE, InstanceOperation.EVACUATE, instance(0),
        instance(1), instance(2), instance(3));

    assertStatuses(outcomes, Status.NOT_APPLIED, Status.NOT_APPLIED, Status.BUDGET_EXHAUSTED,
        Status.BUDGET_EXHAUSTED);
    Assert.assertTrue(outcomes.get(instance(2)).getMessage().contains("room for 2"));
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(version(instance(i)), before[i]);
    }
    int fenceVersion = _accessor.getStat(fence, AccessOption.PERSISTENT).getVersion();

    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1)),
        Status.APPLIED, Status.APPLIED);
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(read(i).getInstanceOperation().getOperation(),
          InstanceOperation.EVACUATE);
      Assert.assertEquals(read(i).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
      Assert.assertEquals(version(instance(i)), before[i] + 1);
    }
    // One commit for the whole request.
    Assert.assertEquals(_accessor.getStat(fence, AccessOption.PERSISTENT).getVersion(),
        fenceVersion + 1);
  }

  @Test
  public void testPercentageCapAndUnlimitedWhenNoCapIsSet() {
    setCap(-1, 34); // 34% of 6 instances is 2.
    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1), instance(2)),
        Status.NOT_APPLIED, Status.NOT_APPLIED, Status.BUDGET_EXHAUSTED);

    setCap(-1, -1);
    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(2), instance(3)),
        Status.APPLIED, Status.APPLIED);
  }

  @Test
  public void testRenewalTakesNoBudgetAndNeverReordersTheOperationStack() {
    setCap(1, -1);
    apply(SAFE, InstanceOperation.EVACUATE, instance(0));
    long opTimestamp = read(0).getInstanceOperation().getTimestamp();
    // A later operation from another source is now the active one.
    writeOperation(0, InstanceOperation.DISABLE, InstanceOperationSource.USER);
    _expiresAt += HOUR_MS;

    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1)),
        Status.NOT_APPLIED, Status.BUDGET_EXHAUSTED);
    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(0)), Status.APPLIED);

    InstanceConfig renewed = read(0);
    Assert.assertEquals(renewed.getInstanceOperationMaintenanceUntilMs(), _expiresAt);
    Assert.assertEquals(renewed.getInstanceOperation().getSource(), InstanceOperationSource.USER);
    Assert.assertEquals(
        renewed.getInstanceOperation(InstanceOperationSource.AUTOMATION).getTimestamp(),
        opTimestamp);
  }

  @Test
  public void testExpiredMarkerNeedsBudgetAgain() {
    setCap(1, -1);
    InstanceConfig expired = read(0);
    expired.setInstanceOperationMaintenanceUntilMs(System.currentTimeMillis() - 1);
    _accessor.set(path(0), expired.getRecord(), AccessOption.PERSISTENT);

    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(1), instance(0)),
        Status.NOT_APPLIED, Status.BUDGET_EXHAUSTED);
  }

  @Test
  public void testReleaseClearsTheMarkerUnlessAnotherSourceStillHoldsAnOperation() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1));
    writeOperation(1, InstanceOperation.DISABLE, InstanceOperationSource.USER);

    // ENABLE needs no expiry and no cluster default.
    assertStatuses(apply(SAFE, InstanceOperation.ENABLE, instance(0), instance(1)),
        Status.APPLIED, Status.APPLIED);

    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(),
        InstanceConfig.INSTANCE_OPERATION_MAINTENANCE_NOT_SET);
    Assert.assertEquals(read(1).getInstanceOperation().getOperation(), InstanceOperation.DISABLE);
    Assert.assertEquals(read(1).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
  }

  @Test
  public void testRejectionsWriteNothing() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(3));
    // SWAP_IN cannot move to EVACUATE, which is checked before the budget a cap of 0 exhausts.
    writeOperation(0, InstanceOperation.SWAP_IN, InstanceOperationSource.USER);
    setCap(0, -1);
    int[] before = {version(instance(0)), version(instance(1)), version(instance(3))};
    List<Collection<String>> judged = new ArrayList<>();
    Function<Collection<String>, Optional<String>> guardrail = instances -> {
      judged.add(new ArrayList<>(instances));
      return Optional.of("unsafe");
    };

    assertStatuses(apply(guardrail, InstanceOperation.EVACUATE, "missing", instance(0),
        instance(1)), Status.INSTANCE_NOT_FOUND, Status.INVALID_TRANSITION,
        Status.BUDGET_EXHAUSTED);
    Assert.assertTrue(judged.isEmpty());
    setCap(-1, -1);
    // The admitted part is judged despite the missing instance, so NOT_APPLIED passed every check.
    // Instance 3 already holds the operation, so it is not judged, only held back.
    Map<String, Outcome> outcomes = apply(guardrail, InstanceOperation.EVACUATE, "missing",
        instance(1), instance(2), instance(3));
    assertStatuses(outcomes, Status.INSTANCE_NOT_FOUND, Status.GUARDRAIL_REJECTED,
        Status.GUARDRAIL_REJECTED, Status.NOT_APPLIED);
    Assert.assertEquals(outcomes.get(instance(1)).getMessage(), "unsafe");
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

    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(1), instance(2)),
        Status.NOT_APPLIED, Status.INVALID_TRANSITION, Status.NOT_APPLIED);
    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(0), instance(2)),
        Status.APPLIED, Status.APPLIED);
  }

  @Test
  public void testUnexpectedFailureWritesNothing() {
    int before = version(instance(0));

    Map<String, Outcome> outcomes = apply(instances -> {
      throw new IllegalStateException("guardrail failed");
    }, InstanceOperation.EVACUATE, instance(0), instance(1));

    assertStatuses(outcomes, Status.ERROR, Status.ERROR);
    Assert.assertTrue(outcomes.get(instance(1)).getMessage().contains("guardrail failed"));
    Assert.assertEquals(version(instance(0)), before);
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
    Function<Collection<String>, Optional<String>> slowGuardrail = instances -> {
      while (System.currentTimeMillis() <= expiresAt) {
        try {
          Thread.sleep(10L);
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
      return Optional.empty();
    };

    Map<String, Outcome> outcomes = handler(slowGuardrail).apply(CLUSTER,
        Collections.singletonList(instance(0)), operation(InstanceOperation.EVACUATE), expiresAt);

    assertStatuses(outcomes, Status.ERROR);
    Assert.assertEquals(version(instance(0)), before);
  }

  @Test
  public void testRenewalNeverShortensTheMarker() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(0));

    assertStatuses(handler(SAFE).apply(CLUSTER, Collections.singletonList(instance(0)),
        operation(InstanceOperation.EVACUATE), _expiresAt - HOUR_MS / 2), Status.APPLIED);

    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
  }

  @Test
  public void testRepeatedDisableUndoesALegacyEnable() {
    apply(SAFE, InstanceOperation.DISABLE, instance(0));
    InstanceConfig legacyEnabled = read(0);
    legacyEnabled.setInstanceEnabled(true);
    _accessor.set(path(0), legacyEnabled.getRecord(), AccessOption.PERSISTENT);
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);

    assertStatuses(apply(SAFE, InstanceOperation.DISABLE, instance(0)), Status.APPLIED);

    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.DISABLE);
  }

  @Test
  public void testRepeatedRequestOnlyRenews() {
    apply(SAFE, InstanceOperation.EVACUATE, instance(0));
    InstanceConfig first = read(0);
    int version = version(instance(0));
    AtomicInteger guardrailCalls = new AtomicInteger();

    assertStatuses(apply(instances -> {
      guardrailCalls.incrementAndGet();
      return Optional.empty();
    }, InstanceOperation.EVACUATE, instance(0)), Status.APPLIED);

    Assert.assertEquals(guardrailCalls.get(), 0);
    Assert.assertEquals(read(0).getInstanceOperation().getTimestamp(),
        first.getInstanceOperation().getTimestamp());
    // The same expiry changes nothing, so nothing is written.
    Assert.assertEquals(version(instance(0)), version);
  }

  @Test
  public void testMarkerAloneTakesTheSameBudgetAndSetsNoOperation() {
    setCap(1, -1);

    assertStatuses(marker(_expiresAt, instance(0), instance(1)), Status.NOT_APPLIED,
        Status.BUDGET_EXHAUSTED);
    Map<String, Outcome> outcomes = marker(_expiresAt, instance(0));

    assertStatuses(outcomes, Status.APPLIED);
    Assert.assertEquals(outcomes.get(instance(0)).getExpiresAtMillis(), Long.valueOf(_expiresAt));
    Assert.assertEquals(read(0).getInstanceOperationMaintenanceUntilMs(), _expiresAt);
    Assert.assertNull(read(0).getInstanceOperation(InstanceOperationSource.AUTOMATION));
    assertStatuses(apply(SAFE, InstanceOperation.EVACUATE, instance(1)), Status.BUDGET_EXHAUSTED);
    // Renewing a live marker takes no new budget.
    _expiresAt += HOUR_MS;
    assertStatuses(marker(_expiresAt, instance(0)), Status.APPLIED);
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
  public void testAdmissionThatLosesARaceIsJudgedAgain() {
    setCap(1, -1);
    CompetingAdmission competitor = new CompetingAdmission(instance(1));

    // The competitor commits after this admission has read the cap, before it commits.
    Map<String, Outcome> outcomes = apply(competitor, InstanceOperation.EVACUATE, instance(0));

    Assert.assertEquals(competitor._outcome.getStatus(), Status.APPLIED);
    assertStatuses(outcomes, Status.BUDGET_EXHAUSTED);
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);
  }

  @Test
  public void testConflictAfterEveryAttemptLosesWritesNothing() {
    int before = version(instance(0));
    String fence = InstanceOperationMaintenanceHandler.fencePath(CLUSTER);
    AtomicInteger attempts = new AtomicInteger();

    Map<String, Outcome> outcomes = apply(instances -> {
      attempts.incrementAndGet();
      _accessor.set(fence, new ZNRecord("bump"), AccessOption.PERSISTENT);
      return Optional.empty();
    }, InstanceOperation.EVACUATE, instance(0));

    assertStatuses(outcomes, Status.CONFLICT);
    Assert.assertEquals(attempts.get(), InstanceOperationMaintenanceHandler.MAX_ATTEMPTS);
    Assert.assertEquals(version(instance(0)), before);
  }

  @Test
  public void testBadRequests() {
    InstanceOperationMaintenanceHandler handler = handler(SAFE);
    InstanceConfig.InstanceOperation evacuate = operation(InstanceOperation.EVACUATE);
    List<String> one = Collections.singletonList(instance(0));
    assertBadRequest(() -> handler.apply(CLUSTER, Collections.emptyList(), evacuate, _expiresAt));
    assertBadRequest(() -> handler.apply(CLUSTER, Arrays.asList(instance(0), ""), evacuate,
        _expiresAt));
    assertBadRequest(() -> handler.apply("noSuchCluster", one, evacuate, _expiresAt));
    // No expiry and no cluster default.
    assertBadRequest(() -> handler.apply(CLUSTER, one, evacuate, 0L));
    // Past expiries, and clearing only goes with no operation.
    assertBadRequest(() -> handler.apply(CLUSTER, one, null, System.currentTimeMillis() - 1));
    assertBadRequest(() -> handler.apply(CLUSTER, one, evacuate,
        InstanceOperationMaintenanceHandler.CLEAR_MARKER));
    for (InstanceOperation unsupported : Arrays.asList(InstanceOperation.SWAP_IN,
        InstanceOperation.UNKNOWN)) {
      assertBadRequest(() -> handler.apply(CLUSTER, one, operation(unsupported), _expiresAt));
    }
    for (InstanceOperationSource unsupported : Arrays.asList(InstanceOperationSource.ADMIN,
        InstanceOperationSource.DEFAULT)) {
      assertBadRequest(() -> handler.apply(CLUSTER, one, new InstanceConfig.InstanceOperation
          .Builder().setOperation(InstanceOperation.EVACUATE).setSource(unsupported).build(),
          _expiresAt));
    }
    Assert.assertFalse(_accessor.exists("/noSuchCluster", AccessOption.PERSISTENT));
    Assert.assertEquals(read(0).getInstanceOperation().getOperation(), InstanceOperation.ENABLE);
  }

  /** A guardrail that commits a competing admission through a second handler, once. */
  private class CompetingAdmission implements Function<Collection<String>, Optional<String>> {
    private final String _instance;
    private Outcome _outcome;

    CompetingAdmission(String instance) {
      _instance = instance;
    }

    @Override
    public Optional<String> apply(Collection<String> ignored) {
      if (_outcome == null) {
        _outcome = handler(SAFE).apply(CLUSTER, Collections.singletonList(_instance),
            operation(InstanceOperation.EVACUATE), _expiresAt).get(_instance);
      }
      return Optional.empty();
    }
  }

  private Map<String, Outcome> apply(Function<Collection<String>, Optional<String>> guardrail,
      InstanceOperation operation, String... instances) {
    return handler(guardrail).apply(CLUSTER, Arrays.asList(instances), operation(operation),
        _expiresAt);
  }

  private Map<String, Outcome> marker(long expiresAt, String... instances) {
    return handler(SAFE).apply(CLUSTER, Arrays.asList(instances), null, expiresAt);
  }

  private InstanceOperationMaintenanceHandler handler(
      Function<Collection<String>, Optional<String>> guardrail) {
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
