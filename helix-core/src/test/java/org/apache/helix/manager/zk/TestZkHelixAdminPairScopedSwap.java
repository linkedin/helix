package org.apache.helix.manager.zk;

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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfigIdentity;
import org.apache.helix.model.SwapPairRequest;
import org.apache.helix.model.SwapPairResult;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Behaviour of the pair-scoped swap APIs on {@link ZKHelixAdmin}: which pairs they accept, what
 * each mode writes, and which explicit outcome a caller gets when they refuse.
 */
public class TestZkHelixAdminPairScopedSwap extends ZkUnitTestBase {

  private static final String ZONE_KEY = "zone";
  private static final String LOGICAL_ID_KEY = "logicalId";
  private static final String HOST_KEY = "host";
  private static final String VIRTUAL_ZONE_KEY = "virtualZone";
  private static final String SWAP_OUT = "swapOutInstance_12000";
  private static final String SWAP_IN = "swapInInstance_12001";
  private static final String LOGICAL_ID = "slot_0";
  private static final String ZONE = "zone_a";

  // ===========================================================================================
  // Preparation
  // ===========================================================================================

  @Test
  public void testCoordinatedPrepareMovesOnlyLogicalIdAndMarksSwapIn() {
    String clusterName = newCluster("coordinatedPrepare");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", "swapInVirtualZone"),
        InstanceConstants.InstanceOperation.UNKNOWN);

    SwapPairResult result = admin.prepareSwapPair(clusterName,
        coordinated().setReason("unit test").build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PREPARED, result.toString());
    Assert.assertTrue(result.isSuccessful());
    InstanceConfig swapInConfig = getInstanceConfig(clusterName, SWAP_IN);
    Map<String, String> swapInDomain = swapInConfig.getDomainAsMap();
    Assert.assertEquals(swapInDomain.get(LOGICAL_ID_KEY), LOGICAL_ID);
    // Only the logical id moves. Everything else the swap-in carries stays as it was.
    Assert.assertEquals(swapInDomain.get(HOST_KEY), "swap-in-host");
    Assert.assertEquals(swapInDomain.get(VIRTUAL_ZONE_KEY), "swapInVirtualZone");
    Assert.assertEquals(swapInDomain.get(ZONE_KEY), ZONE);
    Assert.assertEquals(swapInConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);
    // The swap-out is never written by a preparation.
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test
  public void testCoordinatedPrepareReplayWritesNothing() {
    String clusterName = newCluster("coordinatedPrepareReplay");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);

    Assert.assertEquals(
        admin.prepareSwapPair(clusterName, coordinated().build()).getStatus(),
        SwapPairResult.Status.PREPARED);
    InstanceConfigIdentity afterFirst = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);

    SwapPairResult replay = admin.prepareSwapPair(clusterName, coordinated().build());

    Assert.assertEquals(replay.getStatus(), SwapPairResult.Status.ALREADY_PREPARED,
        replay.toString());
    Assert.assertTrue(replay.isSuccessful());
    // Nothing was rewritten, so a repeated preparation cannot re-apply the operation change.
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_IN), afterFirst);
  }

  @Test
  public void testCoordinatedPrepareDoesNotRetargetAnExistingSwap() {
    String clusterName = newCluster("coordinatedPrepareExistingPair");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    String originalSwapOut = "originalSwapOut_12002";
    addInstance(admin, clusterName, originalSwapOut,
        domain(ZONE, "original_slot", "original-out-host", null), null);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "spare_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    Assert.assertEquals(admin.prepareSwapPair(clusterName,
        new SwapPairRequest.Builder(originalSwapOut, SWAP_IN)
            .setSwapMode(SwapPairRequest.SwapMode.COORDINATED).build()).getStatus(),
        SwapPairResult.Status.PREPARED);
    InstanceConfigIdentity originalOutBefore =
        admin.getInstanceConfigIdentity(clusterName, originalSwapOut);
    InstanceConfigIdentity newOutBefore = admin.getInstanceConfigIdentity(clusterName, SWAP_OUT);
    InstanceConfigIdentity swapInBefore = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);

    SwapPairResult result = admin.prepareSwapPair(clusterName, coordinated().build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PAIR_MISMATCH,
        result.toString());
    Assert.assertFalse(result.isSuccessful());
    Assert.assertTrue(result.getBlockers().get(0).contains("original_slot"),
        result.getBlockers().get(0));
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, originalSwapOut),
        originalOutBefore);
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_OUT), newOutBefore);
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_IN), swapInBefore);
  }

  @Test
  public void testDirectPrepareCopiesWholeSlotAndKeepsPreservedKeys() {
    String clusterName = newCluster("directPrepare");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", "swapInVirtualZone"),
        InstanceConstants.InstanceOperation.UNKNOWN);

    SwapPairResult result = admin.prepareSwapPair(clusterName, direct().build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PREPARED, result.toString());
    InstanceConfig swapInConfig = getInstanceConfig(clusterName, SWAP_IN);
    Map<String, String> swapInDomain = swapInConfig.getDomainAsMap();
    Assert.assertEquals(swapInDomain.get(LOGICAL_ID_KEY), LOGICAL_ID);
    Assert.assertEquals(swapInDomain.get(ZONE_KEY), ZONE);
    // The derived topology field of the swap-out has to come across, or the swap-in would land in
    // a different placement group than the slot it is taking over.
    Assert.assertEquals(swapInDomain.get(VIRTUAL_ZONE_KEY), "swapOutVirtualZone");
    // The incoming host is the one key that stays with the swap-in.
    Assert.assertEquals(swapInDomain.get(HOST_KEY), "swap-in-host");
    // A direct preparation sets no instance operation, so the swap-in stays unassignable.
    Assert.assertEquals(swapInConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    InstanceConfigIdentity afterFirst = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);
    SwapPairResult replay = admin.prepareSwapPair(clusterName, direct().build());
    Assert.assertEquals(replay.getStatus(), SwapPairResult.Status.ALREADY_PREPARED,
        replay.toString());
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_IN), afterFirst);
  }

  @Test
  public void testDirectPrepareRefusesWhenPreservedKeyIsMissing() {
    String clusterName = newCluster("directPrepareMissingKey");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    Map<String, String> swapInDomain = new LinkedHashMap<>();
    swapInDomain.put(ZONE_KEY, ZONE);
    swapInDomain.put(LOGICAL_ID_KEY, "other_slot");
    addInstance(admin, clusterName, SWAP_IN, swapInDomain,
        InstanceConstants.InstanceOperation.UNKNOWN);

    SwapPairResult result = admin.prepareSwapPair(clusterName, direct().build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.INVALID_REQUEST,
        result.toString());
    Assert.assertFalse(result.isSuccessful());
    Assert.assertTrue(result.getBlockers().get(0).contains(HOST_KEY), result.getBlockers().get(0));
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getDomainAsMap().get(
        LOGICAL_ID_KEY), "other_slot");
  }

  @DataProvider
  public Object[][] swapInLogicalIds() {
    return new Object[][] {{"other_slot"}, {LOGICAL_ID}};
  }

  @Test(dataProvider = "swapInLogicalIds")
  public void testDirectPrepareRefusesPreservingLogicalId(String swapInLogicalId) {
    String clusterName = newCluster("directPreparePreservedLogicalId_" + swapInLogicalId);
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, swapInLogicalId, "swap-in-host", "swapInVirtualZone"),
        InstanceConstants.InstanceOperation.UNKNOWN);
    InstanceConfigIdentity swapOutBefore = admin.getInstanceConfigIdentity(clusterName, SWAP_OUT);
    InstanceConfigIdentity swapInBefore = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);

    SwapPairResult result = admin.prepareSwapPair(clusterName,
        direct().setPreservedSwapInDomainKeys(Collections.singleton(LOGICAL_ID_KEY)).build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.INVALID_REQUEST,
        result.toString());
    Assert.assertFalse(result.isSuccessful());
    Assert.assertTrue(result.getBlockers().get(0).contains(LOGICAL_ID_KEY),
        result.getBlockers().get(0));
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_OUT), swapOutBefore);
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_IN), swapInBefore);
  }

  @Test
  public void testDirectPrepareRefusesAssignableSwapIn() {
    String clusterName = newCluster("directPrepareAssignable");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    // Left ENABLE, so copying the swap-out slot onto it would put two assignable instances in the
    // same slot.
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null), null);

    SwapPairResult result = admin.prepareSwapPair(clusterName, direct().build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PAIR_MISMATCH,
        result.toString());
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getDomainAsMap().get(
        LOGICAL_ID_KEY), "other_slot");
  }

  @Test
  public void testCoordinatedPrepareDoesNotDiscardAnotherPartysOperation() {
    String clusterName = newCluster("coordinatedPrepareDisabled");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    InstanceConfig heldSwapInConfig = configAccessor.getInstanceConfig(clusterName, SWAP_IN);
    heldSwapInConfig.setInstanceOperation(
        new InstanceConfig.InstanceOperation.Builder().setOperation(
                InstanceConstants.InstanceOperation.DISABLE).setReason("held by an operator")
            .setSource(InstanceConstants.InstanceOperationSource.USER).build());
    configAccessor.setInstanceConfig(clusterName, SWAP_IN, heldSwapInConfig);

    SwapPairResult result = admin.prepareSwapPair(clusterName, coordinated().build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PAIR_MISMATCH,
        result.toString());
    InstanceConfig swapInConfig = getInstanceConfig(clusterName, SWAP_IN);
    Assert.assertEquals(swapInConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.DISABLE);
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(LOGICAL_ID_KEY), "other_slot");
  }

  @Test
  public void testCoordinatedPrepareRefusesAnAssignableSwapIn() {
    String clusterName = newCluster("coordinatedPrepareAssignableSwapIn");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    // ENABLE is assignable, so the controller can hand this instance work at any moment. There is
    // no point at which marking it SWAP_IN is known not to strand that work.
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null), null);

    SwapPairResult result = admin.prepareSwapPair(clusterName, coordinated().build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PAIR_MISMATCH,
        result.toString());
    Assert.assertTrue(result.getBlockers().get(0).contains("still assignable"),
        result.getBlockers().get(0));
    InstanceConfig swapInConfig = getInstanceConfig(clusterName, SWAP_IN);
    Assert.assertEquals(swapInConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(LOGICAL_ID_KEY), "other_slot");
  }

  // ===========================================================================================
  // Pair scoping
  // ===========================================================================================

  @Test
  public void testPrepareRefusesWhenAThirdInstanceTookTheLogicalId() {
    String clusterName = newCluster("ambiguousPeer");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    // Some other actor moved a third instance into the swap-out's slot, so a swap resolved by
    // logical id could act on that instance instead of the one the caller named.
    String intruder = "intruderInstance_12002";
    addInstance(admin, clusterName, intruder, domain(ZONE, "spare_slot", "intruder-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    InstanceConfig intruderConfig = configAccessor.getInstanceConfig(clusterName, intruder);
    intruderConfig.setDomain(domain(ZONE, LOGICAL_ID, "intruder-host", null));
    configAccessor.setInstanceConfig(clusterName, intruder, intruderConfig);

    SwapPairResult result = admin.prepareSwapPair(clusterName, coordinated().build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PAIR_MISMATCH,
        result.toString());
    Assert.assertTrue(result.getBlockers().get(0).contains(intruder), result.getBlockers().get(0));
    // Neither the named swap-in nor the intruder was touched.
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getDomainAsMap()
        .get(LOGICAL_ID_KEY), "other_slot");
    Assert.assertEquals(getInstanceConfig(clusterName, intruder).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testInvalidRequestsAreRefusedWithoutWriting() {
    String clusterName = newCluster("invalidRequests");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);

    SwapPairResult sameInstance = admin.prepareSwapPair(clusterName,
        new SwapPairRequest.Builder(SWAP_OUT, SWAP_OUT).setSwapMode(
            SwapPairRequest.SwapMode.COORDINATED).build());
    Assert.assertEquals(sameInstance.getStatus(), SwapPairResult.Status.INVALID_REQUEST);

    SwapPairResult missingInstance = admin.prepareSwapPair(clusterName,
        new SwapPairRequest.Builder(SWAP_OUT, "neverAddedInstance_1").setSwapMode(
            SwapPairRequest.SwapMode.COORDINATED).build());
    Assert.assertEquals(missingInstance.getStatus(), SwapPairResult.Status.INVALID_REQUEST);

    // A half-specified expectation is rejected rather than enforced in part.
    SwapPairResult partialIdentity = admin.prepareSwapPair(clusterName, coordinated()
        .setExpectedSwapInIdentity(new InstanceConfigIdentity(SWAP_IN, 3,
            InstanceConfigIdentity.UNKNOWN_CREATION_ID)).build());
    Assert.assertEquals(partialIdentity.getStatus(), SwapPairResult.Status.INVALID_REQUEST);

    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getDomainAsMap().get(
        LOGICAL_ID_KEY), "other_slot");
  }

  // ===========================================================================================
  // Expected identities on both sides
  // ===========================================================================================

  @Test
  public void testBothSidesOfTheExpectedIdentityAreEnforced() {
    String clusterName = newCluster("expectedIdentities");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    bumpConfigVersion(clusterName, SWAP_OUT);
    bumpConfigVersion(clusterName, SWAP_IN);
    InstanceConfigIdentity swapOutIdentity = admin.getInstanceConfigIdentity(clusterName, SWAP_OUT);
    InstanceConfigIdentity swapInIdentity = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);

    // Matching expectations on both sides go through.
    SwapPairResult prepared = admin.prepareSwapPair(clusterName,
        coordinated().setExpectedSwapOutIdentity(swapOutIdentity)
            .setExpectedSwapInIdentity(swapInIdentity).build());
    Assert.assertEquals(prepared.getStatus(), SwapPairResult.Status.PREPARED, prepared.toString());
    Assert.assertEquals(prepared.getObservedSwapInIdentity().getConfigVersion(),
        swapInIdentity.getConfigVersion() + 1);
    Assert.assertEquals(prepared.getObservedSwapOutIdentity(), swapOutIdentity);

    // The swap-in expectation is now stale, so the completion is refused on the swap-in side.
    SwapPairResult staleSwapIn = admin.completeSwapPair(clusterName,
        coordinated().setForceComplete(true).setExpectedSwapOutIdentity(swapOutIdentity)
            .setExpectedSwapInIdentity(swapInIdentity).build());
    Assert.assertEquals(staleSwapIn.getStatus(), SwapPairResult.Status.IDENTITY_MISMATCH,
        staleSwapIn.toString());
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.ENABLE);

    // A stale expectation on the swap-out side is refused just the same.
    bumpConfigVersion(clusterName, SWAP_OUT);
    SwapPairResult staleSwapOut = admin.completeSwapPair(clusterName,
        coordinated().setForceComplete(true).setExpectedSwapOutIdentity(swapOutIdentity)
            .setExpectedSwapInIdentity(prepared.getObservedSwapInIdentity()).build());
    Assert.assertEquals(staleSwapOut.getStatus(), SwapPairResult.Status.IDENTITY_MISMATCH,
        staleSwapOut.toString());
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test
  public void testRecreatedConfigIsRefusedEvenAtTheSameVersion() {
    String clusterName = newCluster("recreatedConfig");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    bumpConfigVersion(clusterName, SWAP_IN);
    InstanceConfigIdentity swapInIdentity = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);

    // The instance is removed and added again, then written the same number of times, so its
    // config version is back to what the caller expects while being a different config.
    admin.dropInstance(clusterName, new InstanceConfig(SWAP_IN));
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    bumpConfigVersion(clusterName, SWAP_IN);
    InstanceConfigIdentity recreated = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);
    Assert.assertEquals(recreated.getConfigVersion(), swapInIdentity.getConfigVersion());
    Assert.assertTrue(recreated.getConfigCreationId() != swapInIdentity.getConfigCreationId(),
        "The recreated config should have a different creation id.");

    SwapPairResult result = admin.prepareSwapPair(clusterName,
        coordinated().setExpectedSwapInIdentity(swapInIdentity).build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.IDENTITY_MISMATCH,
        result.toString());
    Assert.assertTrue(result.getBlockers().get(0).contains("replaced"),
        result.getBlockers().get(0));
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getDomainAsMap().get(
        LOGICAL_ID_KEY), "other_slot");
  }

  @Test
  public void testRecreateAndRewriteDuringMutationIsReportedAsFailed() {
    String clusterName = newCluster("recreateAndRewriteDuringMutation");
    HelixAdmin setupAdmin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(setupAdmin, clusterName);
    addInstance(setupAdmin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    bumpConfigVersion(clusterName, SWAP_OUT);
    bumpConfigVersion(clusterName, SWAP_IN);
    InstanceConfigIdentity swapOutBefore =
        setupAdmin.getInstanceConfigIdentity(clusterName, SWAP_OUT);
    InstanceConfigIdentity swapInBefore =
        setupAdmin.getInstanceConfigIdentity(clusterName, SWAP_IN);
    ConcurrentWriterSerializer serializer = new ConcurrentWriterSerializer(SWAP_IN, () -> {
      setupAdmin.dropInstance(clusterName, new InstanceConfig(SWAP_IN));
      addInstance(setupAdmin, clusterName, SWAP_IN,
          domain(ZONE, "replacement_slot", "replacement-host", null),
          InstanceConstants.InstanceOperation.UNKNOWN);
      bumpConfigVersion(clusterName, SWAP_IN);
      Assert.assertEquals(setupAdmin.getInstanceConfigIdentity(clusterName, SWAP_IN)
          .getConfigVersion(), swapInBefore.getConfigVersion());
    });
    HelixZkClient racingZkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR),
            new HelixZkClient.ZkClientConfig().setZkSerializer(serializer));
    try {
      SwapPairResult result = new ZKHelixAdmin(racingZkClient).prepareSwapPair(clusterName,
          coordinated().setExpectedSwapOutIdentity(swapOutBefore)
              .setExpectedSwapInIdentity(swapInBefore).build());

      Assert.assertTrue(serializer.fired());
      Assert.assertEquals(result.getStatus(), SwapPairResult.Status.FAILED, result.toString());
      Assert.assertFalse(result.isSuccessful());
      Assert.assertTrue(result.getObservedSwapInIdentity().getConfigCreationId()
          != swapInBefore.getConfigCreationId());
      // Detection follows the write: FAILED is not proof that the replacement was untouched.
      Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getDomainAsMap()
          .get(LOGICAL_ID_KEY), LOGICAL_ID);
    } finally {
      racingZkClient.close();
    }
  }

  @Test
  public void testIdentityAtVersionZeroIsRefusedRatherThanWeaklyEnforced() {
    String clusterName = newCluster("unverifiableIdentity");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    InstanceConfigIdentity swapInIdentity = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);
    Assert.assertEquals(swapInIdentity.getConfigVersion(), 0);

    SwapPairResult refused = admin.prepareSwapPair(clusterName,
        coordinated().setExpectedSwapInIdentity(swapInIdentity).build());

    Assert.assertEquals(refused.getStatus(), SwapPairResult.Status.IDENTITY_UNVERIFIABLE,
        refused.toString());
    Assert.assertFalse(refused.isSuccessful());
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getDomainAsMap().get(
        LOGICAL_ID_KEY), "other_slot");

    // Unasserted requests do not use the conservative version-zero refusal.
    Assert.assertEquals(admin.prepareSwapPair(clusterName, coordinated().build()).getStatus(),
        SwapPairResult.Status.PREPARED);
  }

  // ===========================================================================================
  // Completion, replay and conflicts
  // ===========================================================================================

  @Test
  public void testCompleteMovesConfigAndRetiresSwapOut() {
    String clusterName = newCluster("completeSwap");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    admin.addInstanceTag(clusterName, SWAP_OUT, "swapOutTag");
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", "swapInVirtualZone"),
        InstanceConstants.InstanceOperation.UNKNOWN);
    Assert.assertEquals(admin.prepareSwapPair(clusterName, coordinated().build()).getStatus(),
        SwapPairResult.Status.PREPARED);

    SwapPairResult result =
        admin.completeSwapPair(clusterName, coordinated().setForceComplete(true).build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.COMPLETED, result.toString());
    InstanceConfig swapInConfig = getInstanceConfig(clusterName, SWAP_IN);
    // Overwritable fields move across.
    Assert.assertTrue(swapInConfig.getTags().contains("swapOutTag"), swapInConfig.toString());
    Assert.assertTrue(swapInConfig.getInstanceOperation().getOperation()
            != InstanceConstants.InstanceOperation.SWAP_IN,
        "The completed swap-in should no longer be marked SWAP_IN.");
    // The swap-in keeps its own domain, so the host it actually runs on is not overwritten.
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(HOST_KEY), "swap-in-host");
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(LOGICAL_ID_KEY), LOGICAL_ID);
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testCompleteReplayAfterCompletionWritesNothing() {
    String clusterName = newCluster("completeReplay");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    admin.prepareSwapPair(clusterName, coordinated().build());
    Assert.assertEquals(
        admin.completeSwapPair(clusterName, coordinated().setForceComplete(true).build())
            .getStatus(), SwapPairResult.Status.COMPLETED);
    InstanceConfigIdentity swapOutAfter = admin.getInstanceConfigIdentity(clusterName, SWAP_OUT);
    InstanceConfigIdentity swapInAfter = admin.getInstanceConfigIdentity(clusterName, SWAP_IN);

    SwapPairResult replay =
        admin.completeSwapPair(clusterName, coordinated().setForceComplete(true).build());

    Assert.assertEquals(replay.getStatus(), SwapPairResult.Status.ALREADY_COMPLETED,
        replay.toString());
    Assert.assertTrue(replay.isSuccessful());
    // A replayed completion must not move a config a second time.
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_OUT), swapOutAfter);
    Assert.assertEquals(admin.getInstanceConfigIdentity(clusterName, SWAP_IN), swapInAfter);
  }

  @Test
  public void testCompleteRefusesAnUnpreparedPair() {
    String clusterName = newCluster("completeUnprepared");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);

    SwapPairResult result =
        admin.completeSwapPair(clusterName, coordinated().setForceComplete(true).build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PAIR_MISMATCH,
        result.toString());
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test
  public void testCompleteRefusesWhenThePreparedModeDoesNotMatch() {
    String clusterName = newCluster("completeModeMismatch");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    admin.prepareSwapPair(clusterName, coordinated().build());

    // The pair was prepared as a coordinated swap, so it is not a direct swap even under force.
    SwapPairResult result =
        admin.completeSwapPair(clusterName, direct().setForceComplete(true).build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.PAIR_MISMATCH,
        result.toString());
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.ENABLE);
  }

  @Test
  public void testDirectCompleteTransfersTheSlotWithoutSwapInMarker() {
    String clusterName = newCluster("directComplete");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", "swapInVirtualZone"),
        InstanceConstants.InstanceOperation.UNKNOWN);
    Assert.assertEquals(admin.prepareSwapPair(clusterName, direct().build()).getStatus(),
        SwapPairResult.Status.PREPARED);

    SwapPairResult result =
        admin.completeSwapPair(clusterName, direct().setForceComplete(true).build());

    Assert.assertEquals(result.getStatus(), SwapPairResult.Status.COMPLETED, result.toString());
    InstanceConfig swapInConfig = getInstanceConfig(clusterName, SWAP_IN);
    Assert.assertEquals(swapInConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE);
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(VIRTUAL_ZONE_KEY), "swapOutVirtualZone");
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(HOST_KEY), "swap-in-host");
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testConcurrentConfigUpdateAbortsTheWholePair() {
    String clusterName = newCluster("concurrentUpdate");
    HelixAdmin setupAdmin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(setupAdmin, clusterName);
    addInstance(setupAdmin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    setupAdmin.prepareSwapPair(clusterName, coordinated().build());

    // A writer changes the swap-out config after the swap has read it and decided what to write,
    // but before the write lands.
    ConcurrentWriterSerializer serializer = new ConcurrentWriterSerializer(SWAP_IN,
        () -> bumpConfigVersion(clusterName, SWAP_OUT));
    HelixZkClient racingZkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR),
            new HelixZkClient.ZkClientConfig().setZkSerializer(serializer));
    try {
      HelixAdmin racingAdmin = new ZKHelixAdmin(racingZkClient);
      SwapPairResult result =
          racingAdmin.completeSwapPair(clusterName, coordinated().setForceComplete(true).build());

      Assert.assertTrue(serializer.fired(), "The concurrent write was never triggered.");
      Assert.assertEquals(result.getStatus(), SwapPairResult.Status.CONFLICT, result.toString());
      Assert.assertFalse(result.isSuccessful());
      // Neither side moved, so the pair was not left half-completed.
      Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
          .getOperation(), InstanceConstants.InstanceOperation.ENABLE);
      Assert.assertEquals(getInstanceConfig(clusterName, SWAP_IN).getInstanceOperation()
          .getOperation(), InstanceConstants.InstanceOperation.SWAP_IN);
    } finally {
      racingZkClient.close();
    }
  }

  // ===========================================================================================
  // Readiness and force
  // ===========================================================================================

  @Test
  public void testReadinessFailureBlocksCompletionAndForceSkipsOnlyReadiness() {
    String clusterName = newCluster("readinessAndForce");
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    addSwapOut(admin, clusterName);
    addInstance(admin, clusterName, SWAP_IN,
        domain(ZONE, "other_slot", "swap-in-host", null),
        InstanceConstants.InstanceOperation.UNKNOWN);
    admin.prepareSwapPair(clusterName, coordinated().build());

    // Neither instance is live, so the readiness check reports the swap-in being offline.
    SwapPairResult notReady = admin.completeSwapPair(clusterName, coordinated().build());
    Assert.assertEquals(notReady.getStatus(), SwapPairResult.Status.NOT_READY, notReady.toString());
    Assert.assertFalse(notReady.getBlockers().isEmpty());
    Assert.assertTrue(notReady.getBlockers().get(0).contains(SWAP_IN),
        notReady.getBlockers().get(0));
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.ENABLE);

    // Force still refuses a stale identity, because force only waives readiness.
    InstanceConfigIdentity staleSwapOut = new InstanceConfigIdentity(SWAP_OUT,
        admin.getInstanceConfigIdentity(clusterName, SWAP_OUT).getConfigVersion() + 5,
        admin.getInstanceConfigIdentity(clusterName, SWAP_OUT).getConfigCreationId());
    SwapPairResult forcedWithStaleIdentity = admin.completeSwapPair(clusterName,
        coordinated().setForceComplete(true).setExpectedSwapOutIdentity(staleSwapOut).build());
    Assert.assertEquals(forcedWithStaleIdentity.getStatus(),
        SwapPairResult.Status.IDENTITY_MISMATCH, forcedWithStaleIdentity.toString());

    // With the same readiness failure, force completes.
    SwapPairResult forced =
        admin.completeSwapPair(clusterName, coordinated().setForceComplete(true).build());
    Assert.assertEquals(forced.getStatus(), SwapPairResult.Status.COMPLETED, forced.toString());
    Assert.assertEquals(getInstanceConfig(clusterName, SWAP_OUT).getInstanceOperation()
        .getOperation(), InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testUnimplementedPairScopedSwapIsReportedAsUnsupported() {
    // An admin implementation that predates these APIs inherits the interface defaults, which say
    // so explicitly rather than reporting a swap that never happened.
    HelixAdmin admin = Mockito.mock(HelixAdmin.class, Mockito.CALLS_REAL_METHODS);
    assertUnsupported(() -> admin.prepareSwapPair("anyCluster", coordinated().build()));
    assertUnsupported(() -> admin.completeSwapPair("anyCluster", coordinated().build()));
    assertUnsupported(() -> admin.getInstanceConfigIdentity("anyCluster", SWAP_OUT));
  }

  private static void assertUnsupported(Runnable call) {
    try {
      call.run();
      Assert.fail("Expected an UnsupportedOperationException.");
    } catch (UnsupportedOperationException expected) {
      Assert.assertTrue(expected.getMessage().contains("is not implemented"),
          expected.getMessage());
    }
  }

  // ===========================================================================================
  // Helpers
  // ===========================================================================================

  private static SwapPairRequest.Builder coordinated() {
    return new SwapPairRequest.Builder(SWAP_OUT, SWAP_IN).setSwapMode(
        SwapPairRequest.SwapMode.COORDINATED);
  }

  private static SwapPairRequest.Builder direct() {
    return new SwapPairRequest.Builder(SWAP_OUT, SWAP_IN).setSwapMode(
        SwapPairRequest.SwapMode.DIRECT)
        .setPreservedSwapInDomainKeys(Collections.singleton(HOST_KEY));
  }

  private String newCluster(String suffix) {
    String clusterName = getShortClassName() + "_" + suffix;
    if (_gZkClient.exists("/" + clusterName)) {
      _gZkClient.deleteRecursively("/" + clusterName);
    }
    _gSetupTool.addCluster(clusterName, true);
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/" + ZONE_KEY + "/" + LOGICAL_ID_KEY);
    clusterConfig.setFaultZoneType(ZONE_KEY);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
    return clusterName;
  }

  private static Map<String, String> domain(String zone, String logicalId, String host,
      String virtualZone) {
    Map<String, String> domain = new LinkedHashMap<>();
    domain.put(ZONE_KEY, zone);
    domain.put(LOGICAL_ID_KEY, logicalId);
    domain.put(HOST_KEY, host);
    if (virtualZone != null) {
      domain.put(VIRTUAL_ZONE_KEY, virtualZone);
    }
    return domain;
  }

  private void addSwapOut(HelixAdmin admin, String clusterName) {
    addInstance(admin, clusterName, SWAP_OUT,
        domain(ZONE, LOGICAL_ID, "swap-out-host", "swapOutVirtualZone"), null);
  }

  private void addInstance(HelixAdmin admin, String clusterName, String instanceName,
      Map<String, String> domain, InstanceConstants.InstanceOperation operation) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(instanceName.substring(0, instanceName.lastIndexOf('_')));
    instanceConfig.setPort(instanceName.substring(instanceName.lastIndexOf('_') + 1));
    instanceConfig.setDomain(domain);
    if (operation != null) {
      instanceConfig.setInstanceOperation(operation);
    }
    admin.addInstance(clusterName, instanceConfig);
    Assert.assertEquals(
        new ConfigAccessor(_gZkClient).getInstanceConfig(clusterName, instanceName)
            .getInstanceOperation().getOperation(),
        operation != null ? operation : InstanceConstants.InstanceOperation.ENABLE,
        "Instance " + instanceName + " was not added in the expected instance operation.");
  }

  private InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    return new ConfigAccessor(_gZkClient).getInstanceConfig(clusterName, instanceName);
  }

  /**
   * Rewrite the instance config without changing what it means, so its config version advances the
   * way any unrelated config write would advance it.
   */
  private void bumpConfigVersion(String clusterName, String instanceName) {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    instanceConfig.getRecord()
        .setSimpleField("TEST_CONFIG_REVISION", String.valueOf(System.nanoTime()));
    configAccessor.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  /**
   * Runs a concurrent write the first time a given instance config is serialized, which is after
   * the swap has read both configs and decided what to write and before the write is submitted.
   */
  private static class ConcurrentWriterSerializer implements PathBasedZkSerializer {
    private final ZNRecordSerializer _delegate = new ZNRecordSerializer();
    private final String _triggerInstanceName;
    private final Runnable _concurrentWrite;
    private final AtomicBoolean _fired = new AtomicBoolean();

    ConcurrentWriterSerializer(String triggerInstanceName, Runnable concurrentWrite) {
      _triggerInstanceName = triggerInstanceName;
      _concurrentWrite = concurrentWrite;
    }

    boolean fired() {
      return _fired.get();
    }

    @Override
    public byte[] serialize(Object data, String path) throws ZkMarshallingError {
      if (path != null && path.endsWith("/" + _triggerInstanceName) && _fired.compareAndSet(false,
          true)) {
        _concurrentWrite.run();
      }
      return _delegate.serialize(data);
    }

    @Override
    public Object deserialize(byte[] bytes, String path) throws ZkMarshallingError {
      return _delegate.deserialize(bytes);
    }
  }
}
