package org.apache.helix.api.instance;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.api.mutation.CheckedMutationConflictReason;
import org.apache.helix.api.mutation.CheckedMutationOutcome;
import org.apache.helix.api.mutation.CheckedMutationResult;
import org.apache.helix.api.mutation.NodeExpectation;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for conditional instance config changes against a real metadata store, so the
 * conditional write, the retries and the version behaviour are the real ones rather than a
 * model of them.
 */
public class TestCheckedInstanceChanges extends ZkTestBase {
  private static final String RESOURCE = "TestResource";
  private static final String OTHER_RESOURCE = "OtherResource";

  private String _clusterName;
  private BaseDataAccessor<ZNRecord> _accessor;
  private final AtomicInteger _instanceCounter = new AtomicInteger();

  @BeforeClass
  public void beforeClass() {
    _clusterName = "CLUSTER_" + TestCheckedInstanceChanges.class.getSimpleName();
    _gSetupTool.addCluster(_clusterName, true);
    _accessor = new ZkBaseDataAccessor<>(_gZkClient);
  }

  @AfterClass
  public void afterClass() {
    deleteCluster(_clusterName);
  }

  @Test
  public void testDisableIsApplied() {
    String instance = addInstance();
    int versionBefore = readStat(instance).getVersion();

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("draining for maintenance"));

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());
    Assert.assertEquals(result.getEffectiveState().getOperation(),
        InstanceConstants.InstanceOperation.DISABLE);
    Assert.assertTrue(result.getEffectiveState().isDesiredStateInEffect());
    Assert.assertEquals(result.getEffectiveState().getRequestedSourceOperation(),
        InstanceConstants.InstanceOperation.DISABLE);
    Assert.assertFalse(result.getEffectiveState().isHelixEnabled());
    Assert.assertEquals(result.getObservedVersion(), versionBefore + 1);

    InstanceConfig config = readConfig(instance);
    Assert.assertEquals(config.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.DISABLE);
    Assert.assertEquals(config.getInstanceOperation().getSource(),
        InstanceConstants.InstanceOperationSource.AUTOMATION);
    // The deprecated fields are written together with the operation, and carry its timestamp,
    // which is what later makes it provable that this source wrote them.
    Assert.assertEquals(config.getInstanceEnabledTime(),
        config.getInstanceOperation().getTimestamp());
    Assert.assertEquals(config.getInstanceDisabledReason(), "draining for maintenance");
  }

  @Test
  public void testRepeatedDisableWritesNothing() {
    String instance = addInstance();
    CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
        disableRequest("same reason"));
    Stat statAfterFirst = readStat(instance);
    ZNRecord recordAfterFirst = readRecord(instance);

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("same reason"));

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.UNCHANGED,
        result.getMessage());
    Assert.assertTrue(result.getEffectiveState().isDesiredStateInEffect());
    Assert.assertEquals(result.getObservedVersion(), statAfterFirst.getVersion());
    // A genuine no-op must not rewrite anything: same version, and the same content, which is
    // what proves no timestamp was refreshed and nothing was reordered.
    Assert.assertEquals(readStat(instance).getVersion(), statAfterFirst.getVersion());
    assertSameRecord(readRecord(instance), recordAfterFirst);
  }

  @Test
  public void testOtherSourcesArePreservedInOrder() {
    String instance = addInstance();
    apply(instance, InstanceOperationChangeRequest
        .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
            InstanceConstants.InstanceOperationSource.USER).setReason("operator disable").build());
    InstanceConfig.InstanceOperation userOperationBefore =
        recordedOperation(readConfig(instance), InstanceConstants.InstanceOperationSource.USER);

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("automation disable"));
    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());

    InstanceConfig config = readConfig(instance);
    Assert.assertEquals(recordedSources(config),
        Arrays.asList(InstanceConstants.InstanceOperationSource.USER,
            InstanceConstants.InstanceOperationSource.AUTOMATION));
    InstanceConfig.InstanceOperation userOperationAfter =
        recordedOperation(config, InstanceConstants.InstanceOperationSource.USER);
    Assert.assertEquals(userOperationAfter.getReason(), userOperationBefore.getReason());
    Assert.assertEquals(userOperationAfter.getTimestamp(), userOperationBefore.getTimestamp());
    // The deprecated annotation still describes the first disable, because this change was not
    // able to prove it owns it and overwriting it would erase why that writer disabled the
    // instance while it stays disabled.
    Assert.assertEquals(config.getInstanceDisabledReason(), "operator disable");

    // Re-asserting the same intent stays a no-op even though another source recorded a disable
    // before this one.
    Stat before = readStat(instance);
    CheckedMutationResult<EffectiveInstanceOperation> repeat =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("automation disable"));
    Assert.assertEquals(repeat.getOutcome(), CheckedMutationOutcome.UNCHANGED,
        repeat.getMessage());
    Assert.assertEquals(readStat(instance).getVersion(), before.getVersion());
    Assert.assertEquals(recordedSources(readConfig(instance)),
        Arrays.asList(InstanceConstants.InstanceOperationSource.USER,
            InstanceConstants.InstanceOperationSource.AUTOMATION));
  }

  @Test
  public void testRequireRequestedSourceActiveRewritesWhenAnotherSourceIsActive() {
    String instance = addInstance();
    apply(instance, disableRequest("automation disable"));
    apply(instance, InstanceOperationChangeRequest
        .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
            InstanceConstants.InstanceOperationSource.USER).setReason("operator disable").build());

    // Without the flag this is a no-op: the operation asked for is already in effect, and this
    // source already has it recorded.
    Stat before = readStat(instance);
    Assert.assertEquals(
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("automation disable")).getOutcome(), CheckedMutationOutcome.UNCHANGED);
    Assert.assertEquals(readStat(instance).getVersion(), before.getVersion());

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setReason("automation disable").setRequireRequestedSourceActive(true).build());

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());
    Assert.assertEquals(recordedSources(readConfig(instance)),
        Arrays.asList(InstanceConstants.InstanceOperationSource.USER,
            InstanceConstants.InstanceOperationSource.AUTOMATION));
  }

  @Test
  public void testExpectedVersionIsEnforced() {
    String instance = addInstance();
    int version = readStat(instance).getVersion();

    CheckedMutationResult<EffectiveInstanceOperation> stale =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setNodeExpectation(NodeExpectation.ofVersion(version + 7)).build());
    Assert.assertEquals(stale.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(stale.getConflictReason().orElse(null),
        CheckedMutationConflictReason.VERSION_MISMATCH);
    Assert.assertEquals(readStat(instance).getVersion(), version, "Nothing may be written");

    CheckedMutationResult<EffectiveInstanceOperation> fresh =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setNodeExpectation(NodeExpectation.ofVersion(version)).build());
    Assert.assertEquals(fresh.getOutcome(), CheckedMutationOutcome.APPLIED, fresh.getMessage());
  }

  @Test
  public void testRecreatedInstanceConfigIsRejectedByCreationId() {
    String instance = addInstance();
    long creationId = readStat(instance).getCzxid();

    // Drop and recreate the instance, which is what makes a version alone unsafe: the new node
    // starts again from version zero under the same path.
    _gSetupTool.getClusterManagementTool().dropInstance(_clusterName, readConfig(instance));
    _gSetupTool.addInstanceToCluster(_clusterName, instance);
    long newCreationId = readStat(instance).getCzxid();
    Assert.assertTrue(newCreationId != creationId,
        "Recreating the instance must produce a new creation id");

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setNodeExpectation(NodeExpectation.ofCreationId(creationId)).build());

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.IDENTITY_MISMATCH);
    Assert.assertEquals(readConfig(instance).getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE, "Nothing may be written");

    // The same request against the incarnation that is actually there is applied, and reports
    // the creation id it wrote against so a caller can chain further conditional changes.
    CheckedMutationResult<EffectiveInstanceOperation> retry =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setNodeExpectation(NodeExpectation.ofCreationId(newCreationId)).build());
    Assert.assertEquals(retry.getOutcome(), CheckedMutationOutcome.APPLIED, retry.getMessage());
    Assert.assertEquals(retry.getObservedCreationId(), newCreationId);
  }

  @Test
  public void testMissingInstanceIsNotCreated() {
    String instance = "neverAdded_12345";
    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("does not matter"));

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.NOT_FOUND);
    Assert.assertNull(result.getEffectiveState());
    Assert.assertFalse(
        _accessor.exists(PropertyPathBuilder.instanceConfig(_clusterName, instance), 0),
        "A checked change must never create the node it guards");
  }

  @Test
  public void testExpectedOperationMismatchIsRejected() {
    String instance = addInstance();
    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setExpectedOperation(InstanceConstants.InstanceOperation.EVACUATE).build());

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.EXPECTED_STATE_MISMATCH);
    Assert.assertEquals(result.getEffectiveState().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertEquals(readConfig(instance).getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE, "Nothing may be written");
  }

  @Test
  public void testConcurrentWriterIsRetriedAndPreserved() {
    String instance = addInstance();
    // Another writer changes the instance config between the read and the conditional write.
    // The first attempt loses the write, and the change is then re-evaluated on the content
    // that writer left behind, instead of overwriting it.
    InterferingAccessor accessor = new InterferingAccessor(_gZkClient, 1,
        () -> _gSetupTool.getClusterManagementTool().addInstanceTag(_clusterName, instance, "t1"));

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(accessor, _clusterName, instance,
            disableRequest("automation disable"));

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());
    Assert.assertEquals(accessor.getReadCount(), 3,
        "Expected a losing attempt, a winning attempt and the read back after the write");
    InstanceConfig config = readConfig(instance);
    Assert.assertEquals(config.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.DISABLE);
    Assert.assertEquals(config.getTags(), Collections.singletonList("t1"),
        "The concurrent writer's change must survive");
  }

  @Test
  public void testConcurrentWriterThatAlwaysWinsEndsInConflict() {
    String instance = addInstance();
    AtomicInteger counter = new AtomicInteger();
    InterferingAccessor accessor = new InterferingAccessor(_gZkClient, Integer.MAX_VALUE,
        () -> _gSetupTool.getClusterManagementTool()
            .addInstanceTag(_clusterName, instance, "t" + counter.incrementAndGet()));

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(accessor, _clusterName, instance,
            disableRequest("automation disable"));

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.CONCURRENT_MODIFICATION);
    Assert.assertEquals(readConfig(instance).getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE, "Nothing may be written");
  }

  @Test
  public void testEnableIsRefusedWhenDeprecatedDisableIsNotOwned() {
    String instance = addInstance();
    writeLegacyOnlyDisable(instance, "disabled by an older client");
    ZNRecord before = readRecord(instance);

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.ENABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION).build());

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.LEGACY_STATE_NOT_OWNED);
    assertSameRecord(readRecord(instance), before);

    // The explicit override exists for a caller that is the authority for the instance, and it
    // behaves like the unchecked path.
    CheckedMutationResult<EffectiveInstanceOperation> overridden =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.ENABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setLegacyFieldPolicy(LegacyFieldPolicy.ALLOW_OVERRIDE).build());
    Assert.assertEquals(overridden.getOutcome(), CheckedMutationOutcome.APPLIED,
        overridden.getMessage());
    Assert.assertTrue(readConfig(instance).getInstanceEnabled());
  }

  @Test
  public void testEnableClearsADeprecatedDisableThisSourceWrote() {
    String instance = addInstance();
    apply(instance, disableRequest("automation disable"));
    Assert.assertFalse(readConfig(instance).getInstanceEnabled());

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.ENABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION).build());

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());
    Assert.assertTrue(result.getEffectiveState().isDesiredStateInEffect());
    InstanceConfig config = readConfig(instance);
    Assert.assertTrue(config.getInstanceEnabled());
    Assert.assertEquals(config.getInstanceDisabledReason(), "");
  }

  @Test
  public void testChangeThatCannotTakeEffectIsRefused() {
    String instance = addInstance();
    apply(instance, InstanceOperationChangeRequest
        .newBuilder(InstanceConstants.InstanceOperation.EVACUATE,
            InstanceConstants.InstanceOperationSource.USER).setReason("operator evacuate").build());
    ZNRecord before = readRecord(instance);

    // Recording ENABLE here would be accepted by the data model but would leave EVACUATE in
    // effect, because an ENABLE is recorded before it. That is refused by default rather than
    // written, so the caller cannot mistake the write for a completed change.
    CheckedMutationResult<EffectiveInstanceOperation> refused =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.ENABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION).build());
    Assert.assertEquals(refused.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(refused.getConflictReason().orElse(null),
        CheckedMutationConflictReason.DESIRED_STATE_BLOCKED);
    assertSameRecord(readRecord(instance), before);

    // A caller that only wants to withdraw its own claim asks for it explicitly, and is told
    // that the desired state is still not in effect.
    CheckedMutationResult<EffectiveInstanceOperation> recorded =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.ENABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setRequireDesiredStateInEffect(false).build());
    Assert.assertEquals(recorded.getOutcome(), CheckedMutationOutcome.APPLIED,
        recorded.getMessage());
    Assert.assertFalse(recorded.getEffectiveState().isDesiredStateInEffect());
    Assert.assertEquals(recorded.getEffectiveState().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertEquals(readConfig(instance).getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);
  }

  @Test
  public void testInvalidTransitionIsReportedAsConflict() {
    String instance = addInstance();
    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.SWAP_IN,
                    InstanceConstants.InstanceOperationSource.AUTOMATION).build());

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.INVALID_TRANSITION);
    Assert.assertEquals(readConfig(instance).getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE, "Nothing may be written");
  }

  @Test
  public void testOperationRecordedByANewerVersionIsPreserved() {
    String instance = addInstance();
    writeUnrecognisedOperation(instance);
    ZNRecord before = readRecord(instance);

    // The unrecognised operation is the active one, so it is read as UNKNOWN. Moving from
    // UNKNOWN to DISABLE is validated against the other instances, and is allowed here because
    // no other instance shares this one's logical id.
    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("automation disable"));
    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());

    List<String> operationsBefore = before.getListField(
        InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name());
    List<String> operationsAfter = readRecord(instance).getListField(
        InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name());
    Assert.assertEquals(operationsAfter.get(0), operationsBefore.get(0),
        "An operation this version does not recognise must be kept exactly as it was");
    Assert.assertEquals(operationsAfter.size(), operationsBefore.size() + 1);
  }

  @Test
  public void testEnableWithNothingRecordedWritesNothing() {
    String instance = addInstance();
    Stat before = readStat(instance);
    ZNRecord recordBefore = readRecord(instance);

    // Nothing recorded for this source and the instance is enabled: an ENABLE entry would say
    // exactly what the absence of an entry already says.
    CheckedMutationResult<EffectiveInstanceOperation> unchanged =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.ENABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION).build());
    Assert.assertEquals(unchanged.getOutcome(), CheckedMutationOutcome.UNCHANGED,
        unchanged.getMessage());
    Assert.assertEquals(readStat(instance).getVersion(), before.getVersion());
    assertSameRecord(readRecord(instance), recordBefore);

    // A caller that wants its reason recorded is asking for a change, and gets one.
    CheckedMutationResult<EffectiveInstanceOperation> applied =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            InstanceOperationChangeRequest
                .newBuilder(InstanceConstants.InstanceOperation.ENABLE,
                    InstanceConstants.InstanceOperationSource.AUTOMATION)
                .setReason("checked in").build());
    Assert.assertEquals(applied.getOutcome(), CheckedMutationOutcome.APPLIED,
        applied.getMessage());
    Assert.assertEquals(applied.getEffectiveState().getRequestedSourceOperation(),
        InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test
  public void testOperationWithAnUnreadableSourceIsRefused() {
    String instance = addInstance();
    writeRawOperations(instance, Collections.singletonList(
        "{\"OPERATION\":\"DISABLE\",\"REASON\":\"newer writer\",\"SOURCE\":\"A_NEW_SOURCE\","
            + "\"TIMESTAMP\":\"1\"}"));
    ZNRecord before = readRecord(instance);

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("automation disable"));

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.UNREADABLE_STATE);
    assertSameRecord(readRecord(instance), before);
  }

  @Test
  public void testOperationThatCannotBeDeserialisedIsRefused() {
    String instance = addInstance();
    // An entry that cannot be read is dropped when the recorded operations are parsed, so
    // writing them back would silently discard it.
    writeRawOperations(instance, Arrays.asList("this is not an operation",
        "{\"OPERATION\":\"DISABLE\",\"REASON\":\"operator\",\"SOURCE\":\"USER\","
            + "\"TIMESTAMP\":\"1\"}"));
    ZNRecord before = readRecord(instance);

    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance,
            disableRequest("automation disable"));

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.UNREADABLE_STATE);
    assertSameRecord(readRecord(instance), before);
  }

  @Test
  public void testAdminSourceIsRejected() {
    try {
      InstanceOperationChangeRequest.newBuilder(InstanceConstants.InstanceOperation.DISABLE,
          InstanceConstants.InstanceOperationSource.ADMIN).build();
      Assert.fail("ADMIN clears every other source and must not be expressible here");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("ADMIN"), e.getMessage());
    }
  }

  @Test
  public void testDisablePartitionsAppliedThenNoOp() {
    String instance = addInstance();
    CheckedMutationResult<EffectiveDisabledPartitions> applied =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p0", "p1"), true).build());

    Assert.assertEquals(applied.getOutcome(), CheckedMutationOutcome.APPLIED,
        applied.getMessage());
    Assert.assertTrue(applied.getEffectiveState().isDesiredStateInEffect());
    Assert.assertEquals(new HashSet<>(applied.getEffectiveState().getDisabledPartitions(RESOURCE)),
        setOf("p0", "p1"));

    Stat before = readStat(instance);
    ZNRecord recordBefore = readRecord(instance);
    CheckedMutationResult<EffectiveDisabledPartitions> repeat =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p0", "p1"), true).build());
    Assert.assertEquals(repeat.getOutcome(), CheckedMutationOutcome.UNCHANGED,
        repeat.getMessage());
    Assert.assertEquals(readStat(instance).getVersion(), before.getVersion());
    assertSameRecord(readRecord(instance), recordBefore);
  }

  @Test
  public void testDisablePartitionsKeepsOtherResources() {
    String instance = addInstance();
    apply(instance,
        DisabledPartitionsChangeRequest.newBuilder(OTHER_RESOURCE, setOf("q0"), true).build());
    apply(instance, DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p0"), true)
        .build());

    CheckedMutationResult<EffectiveDisabledPartitions> enabled =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p0"), false).build());

    Assert.assertEquals(enabled.getOutcome(), CheckedMutationOutcome.APPLIED,
        enabled.getMessage());
    Map<String, List<String>> disabled = readConfig(instance).getDisabledPartitionsMap();
    Assert.assertEquals(new HashSet<>(disabled.get(OTHER_RESOURCE)), setOf("q0"));
    Assert.assertTrue(disabled.get(RESOURCE) == null || disabled.get(RESOURCE).isEmpty(),
        disabled.toString());
  }

  @Test
  public void testDisableAllResourcesSentinelRoundTrip() {
    String instance = addInstance();
    String allResources = InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY;

    CheckedMutationResult<EffectiveDisabledPartitions> disabled =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(allResources, setOf(""), true).build());
    Assert.assertEquals(disabled.getOutcome(), CheckedMutationOutcome.APPLIED,
        disabled.getMessage());
    Assert.assertTrue(readConfig(instance).getDisabledPartitionsMap().containsKey(allResources));

    Assert.assertEquals(
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(allResources, setOf(""), true).build())
            .getOutcome(), CheckedMutationOutcome.UNCHANGED);

    CheckedMutationResult<EffectiveDisabledPartitions> enabled =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(allResources, setOf(""), false).build());
    Assert.assertEquals(enabled.getOutcome(), CheckedMutationOutcome.APPLIED,
        enabled.getMessage());
    Assert.assertFalse(readConfig(instance).getDisabledPartitionsMap().containsKey(allResources));
  }

  @Test
  public void testEnableIsRefusedWhenOnlyTheDeprecatedListDisablesThePartition() {
    String instance = addInstance();
    writeDeprecatedCrossResourceDisable(instance, "p0");
    ZNRecord before = readRecord(instance);

    CheckedMutationResult<EffectiveDisabledPartitions> refused =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p0"), false).build());

    Assert.assertEquals(refused.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(refused.getConflictReason().orElse(null),
        CheckedMutationConflictReason.LEGACY_STATE_NOT_OWNED);
    Assert.assertEquals(refused.getEffectiveState().getCrossResourceDisabledPartitions(),
        setOf("p0"));
    assertSameRecord(readRecord(instance), before);

    CheckedMutationResult<EffectiveDisabledPartitions> overridden =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p0"), false)
                .setLegacyFieldPolicy(LegacyFieldPolicy.ALLOW_OVERRIDE).build());
    Assert.assertEquals(overridden.getOutcome(), CheckedMutationOutcome.APPLIED,
        overridden.getMessage());
    Assert.assertTrue(
        overridden.getEffectiveState().getCrossResourceDisabledPartitions().isEmpty());
  }

  @Test
  public void testExpectedDisabledPartitionsIsEnforced() {
    String instance = addInstance();
    apply(instance, DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p0"), true)
        .build());
    ZNRecord before = readRecord(instance);

    CheckedMutationResult<EffectiveDisabledPartitions> result =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance,
            DisabledPartitionsChangeRequest.newBuilder(RESOURCE, setOf("p1"), true)
                .setExpectedDisabledPartitions(Collections.emptySet()).build());

    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.CONFLICT);
    Assert.assertEquals(result.getConflictReason().orElse(null),
        CheckedMutationConflictReason.EXPECTED_STATE_MISMATCH);
    assertSameRecord(readRecord(instance), before);
  }

  @Test
  public void testEmptyPartitionSetIsRejected() {
    try {
      DisabledPartitionsChangeRequest.newBuilder(RESOURCE, Collections.emptySet(), true).build();
      Assert.fail("A change with no partitions has no meaning and must be rejected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("partitions"), e.getMessage());
    }
  }

  private String addInstance() {
    String instance = "localhost_" + (12918 + _instanceCounter.incrementAndGet());
    _gSetupTool.addInstanceToCluster(_clusterName, instance);
    return instance;
  }

  private InstanceOperationChangeRequest disableRequest(String reason) {
    return InstanceOperationChangeRequest
        .newBuilder(InstanceConstants.InstanceOperation.DISABLE,
            InstanceConstants.InstanceOperationSource.AUTOMATION).setReason(reason).build();
  }

  private void apply(String instance, InstanceOperationChangeRequest request) {
    CheckedMutationResult<EffectiveInstanceOperation> result =
        CheckedInstanceChanges.setInstanceOperation(_accessor, _clusterName, instance, request);
    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());
  }

  private void apply(String instance, DisabledPartitionsChangeRequest request) {
    CheckedMutationResult<EffectiveDisabledPartitions> result =
        CheckedInstanceChanges.setPartitionsDisabled(_accessor, _clusterName, instance, request);
    Assert.assertEquals(result.getOutcome(), CheckedMutationOutcome.APPLIED, result.getMessage());
  }

  /**
   * Write a disable the way a client that predates the per source operation record would: only
   * the deprecated fields, with nothing recorded that could be attributed to a source.
   */
  private void writeLegacyOnlyDisable(String instance, String reason) {
    ZNRecord record = readRecord(instance);
    record.setBooleanField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), false);
    record.setLongField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED_TIMESTAMP.name(),
        System.currentTimeMillis());
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_REASON.name(),
        reason);
    Assert.assertTrue(_accessor.set(PropertyPathBuilder.instanceConfig(_clusterName, instance),
        record, -1, AccessOption.PERSISTENT));
  }

  /**
   * Record an operation this version of the data model does not recognise, which is what a
   * newer writer in a mixed version cluster produces.
   */
  private void writeUnrecognisedOperation(String instance) {
    writeRawOperations(instance, Collections.singletonList(
        "{\"OPERATION\":\"AN_OPERATION_FROM_THE_FUTURE\",\"REASON\":\"newer writer\","
            + "\"SOURCE\":\"USER\",\"TIMESTAMP\":\"1\"}"));
  }

  private void writeRawOperations(String instance, List<String> serializedOperations) {
    ZNRecord record = readRecord(instance);
    record.setListField(InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name(),
        serializedOperations);
    Assert.assertTrue(_accessor.set(PropertyPathBuilder.instanceConfig(_clusterName, instance),
        record, -1, AccessOption.PERSISTENT));
  }

  /**
   * Disable a partition through the deprecated flat list, which applies to every resource.
   */
  private void writeDeprecatedCrossResourceDisable(String instance, String partition) {
    ZNRecord record = readRecord(instance);
    record.setListField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(),
        Collections.singletonList(partition));
    Assert.assertTrue(_accessor.set(PropertyPathBuilder.instanceConfig(_clusterName, instance),
        record, -1, AccessOption.PERSISTENT));
  }

  private ZNRecord readRecord(String instance) {
    return _accessor.get(PropertyPathBuilder.instanceConfig(_clusterName, instance), new Stat(),
        AccessOption.PERSISTENT);
  }

  private InstanceConfig readConfig(String instance) {
    return new InstanceConfig(readRecord(instance));
  }

  private Stat readStat(String instance) {
    Stat stat = new Stat();
    _accessor.get(PropertyPathBuilder.instanceConfig(_clusterName, instance), stat,
        AccessOption.PERSISTENT);
    return stat;
  }

  private static List<InstanceConstants.InstanceOperationSource> recordedSources(
      InstanceConfig config) {
    return config.getAllInstanceOperations().stream()
        .map(InstanceConfig.InstanceOperation::getSource).collect(Collectors.toList());
  }

  private static InstanceConfig.InstanceOperation recordedOperation(InstanceConfig config,
      InstanceConstants.InstanceOperationSource source) {
    return config.getAllInstanceOperations().stream().filter(op -> op.getSource() == source)
        .findFirst().orElse(null);
  }

  private static LinkedHashSet<String> setOf(String... values) {
    return new LinkedHashSet<>(Arrays.asList(values));
  }

  private static void assertSameRecord(ZNRecord actual, ZNRecord expected) {
    Assert.assertEquals(actual.getSimpleFields(), expected.getSimpleFields());
    Assert.assertEquals(actual.getListFields(), expected.getListFields());
    Assert.assertEquals(actual.getMapFields(), expected.getMapFields());
  }

  /**
   * An accessor that lets another writer change the node between a read and the conditional
   * write that follows it, which is the race a checked change has to survive.
   */
  private static class InterferingAccessor extends ZkBaseDataAccessor<ZNRecord> {
    private final int _maxInterferences;
    private final Runnable _interference;
    private int _readCount;
    private int _interferenceCount;

    InterferingAccessor(RealmAwareZkClient zkClient, int maxInterferences, Runnable interference) {
      super(zkClient);
      _maxInterferences = maxInterferences;
      _interference = interference;
    }

    @Override
    public ZNRecord get(String path, Stat stat, int options) {
      ZNRecord record = super.get(path, stat, options);
      _readCount++;
      if (record != null && _interferenceCount < _maxInterferences) {
        _interferenceCount++;
        _interference.run();
      }
      return record;
    }

    int getReadCount() {
      return _readCount;
    }
  }
}
