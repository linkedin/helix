package org.apache.helix.controller.rebalancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.util.TestInputLoader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMaintenanceRebalancer {

  private static final String RESOURCE_NAME = "testResource";
  private static final String PARTITION_NAME = "testResourcePartition";

  @Test(dataProvider = "TestComputeIdealStateInput")
  public void testComputeIdealState(String comment, String stateModelName, List<String> liveInstances,
      List<String> preferenceList, Map<String, String> currentStateMap, List<String> expectedPrefList) {
    System.out.println("Test case comment: " + comment);
    MaintenanceRebalancer rebalancer = new MaintenanceRebalancer();

    Partition partition = new Partition(PARTITION_NAME);
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    for (String instance : currentStateMap.keySet()) {
      currentStateOutput.setCurrentState(RESOURCE_NAME, partition, instance, currentStateMap.get(instance));
    }

    IdealState currentIdealState = new IdealState(RESOURCE_NAME);
    currentIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    currentIdealState.setRebalancerClassName("org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
    currentIdealState.setStateModelDefRef(stateModelName);
    currentIdealState.setPreferenceList(PARTITION_NAME, preferenceList);

    ResourceControllerDataProvider dataCache = mock(ResourceControllerDataProvider.class);
    when(dataCache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());

    IdealState updatedIdealState = rebalancer
        .computeNewIdealState(RESOURCE_NAME, currentIdealState, currentStateOutput, dataCache);

    List<String> partitionPrefList = updatedIdealState.getPreferenceList(PARTITION_NAME);
    Assert.assertTrue(partitionPrefList.equals(expectedPrefList));
  }

  @DataProvider(name = "TestComputeIdealStateInput")
  public Object[][] loadTestComputeIdealStateInput() {
    final String[] params = {
        "comment", "stateModel", "liveInstances", "preferenceList", "currentStateMap", "expectedPreferenceList"
    };
    return TestInputLoader.loadTestInputs("MaintenanceRebalancer.ComputeNewIdealState.json", params);
  }

  @DataProvider
  public Object[][] maintenanceEvacuationStates() {
    return new Object[][] {
        {InstanceOperation.EVACUATE, "OFFLINE", "OFFLINE"},
        {InstanceOperation.ENABLE, "OFFLINE", "SLAVE"},
        {InstanceOperation.DISABLE, "OFFLINE", "OFFLINE"},
        {InstanceOperation.EVACUATE, "SLAVE", "SLAVE"},
        {InstanceOperation.EVACUATE, "MASTER", "MASTER"},
        {InstanceOperation.EVACUATE, "ERROR", "ERROR"}
    };
  }

  @DataProvider
  public Object[][] replacementAvailability() {
    return new Object[][] {
        {0, "SLAVE", true, false, null, "MASTER"},
        {2, "SLAVE", true, false, null, "SLAVE"},
        {3, "SLAVE", true, false, null, "OFFLINE"},
        {3, "ERROR", true, false, null, "SLAVE"},
        {3, "OFFLINE", true, false, null, "SLAVE"},
        {3, "SLAVE", false, false, null, "SLAVE"},
        {3, "SLAVE", true, true, null, "SLAVE"},
        {3, "SLAVE", true, false, "OFFLINE", "SLAVE"}
    };
  }

  @Test(dataProvider = "replacementAvailability")
  public void testEvacuationPreservesRecoveryWithoutHealthyReplacements(int peerCount,
      String replacementState, boolean replacementLive, boolean replacementDisabled,
      String pendingState, String expectedState) {
    Partition partition = new Partition(PARTITION_NAME);
    Resource resource = new Resource(RESOURCE_NAME);
    resource.addPartition(PARTITION_NAME);
    resource.setStateModelDefRef("MasterSlave");
    CurrentStateOutput current = new CurrentStateOutput();
    current.setCurrentState(RESOURCE_NAME, partition, "old", "OFFLINE");
    Map<String, LiveInstance> live = new HashMap<>();
    live.put("old", new LiveInstance("old"));
    List<String> preferenceList = new ArrayList<>();
    for (int i = 0; i < peerCount; i++) {
      String peer = "peer" + i;
      String state = i == 0 ? "MASTER" : replacementState;
      current.setCurrentState(RESOURCE_NAME, partition, peer, state);
      preferenceList.add(peer);
      if (i != peerCount - 1 || replacementLive) {
        live.put(peer, new LiveInstance(peer));
      }
    }
    String replacement = "peer" + (peerCount - 1);
    if (pendingState != null) {
      Message pending = new Message(Message.MessageType.STATE_TRANSITION, "pending");
      pending.setFromState(replacementState);
      pending.setToState(pendingState);
      current.setPendingMessage(RESOURCE_NAME, partition, replacement, pending);
    }
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getClusterConfig()).thenReturn(new ClusterConfig("maintenanceTest"));
    when(cache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());
    when(cache.getEvacuatingInstances()).thenReturn(Set.of("old"));
    when(cache.getLiveInstances()).thenReturn(live);
    Map<String, LiveInstance> assignable = new HashMap<>(live);
    assignable.remove("old");
    when(cache.getAssignableLiveInstances()).thenReturn(assignable);
    when(cache.getDisabledInstancesForPartition(RESOURCE_NAME, PARTITION_NAME))
        .thenReturn(replacementDisabled ? Set.of(replacement) : Set.of());
    when(cache.getAbnormalStateResolver("MasterSlave"))
        .thenReturn(MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);
    IdealState target = new IdealState(RESOURCE_NAME);
    target.setStateModelDefRef("MasterSlave");
    target.setReplicas("3");
    target.setPreferenceList(PARTITION_NAME, preferenceList);
    MaintenanceRebalancer rebalancer = new MaintenanceRebalancer();
    IdealState maintenanceTarget =
        rebalancer.computeNewIdealState(RESOURCE_NAME, target, current, cache);
    Map<String, String> assignment = rebalancer
        .computeBestPossiblePartitionState(cache, maintenanceTarget, resource, current)
        .getReplicaMap(partition);
    Assert.assertEquals(assignment.get("old"), expectedState);
  }

  @Test(dataProvider = "maintenanceEvacuationStates")
  public void testMaintenanceRespectsDrainedEvacuatingReplica(InstanceOperation operation,
      String oldState, String expectedState) throws Exception {
    Partition partition = new Partition(PARTITION_NAME);
    Resource resource = new Resource(RESOURCE_NAME);
    resource.addPartition(PARTITION_NAME);
    resource.setStateModelDefRef("MasterSlave");
    CurrentStateOutput current = new CurrentStateOutput();
    Map<String, String> states = Map.of("old", oldState, "replacement", "SLAVE",
        "follower", "SLAVE", "master", oldState.equals("MASTER") ? "SLAVE" : "MASTER");
    Map<String, LiveInstance> live = new HashMap<>();
    states.forEach((host, state) -> {
      current.setCurrentState(RESOURCE_NAME, partition, host, state);
      LiveInstance instance = new LiveInstance(host);
      instance.setSessionId("session");
      live.put(host, instance);
    });
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    ClusterConfig clusterConfig = new ClusterConfig("maintenanceTest");
    clusterConfig.stateTransitionCancelEnabled(true);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(cache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());
    when(cache.getLiveInstances()).thenReturn(live);
    when(cache.getEvacuatingInstances()).thenReturn(
        operation == InstanceOperation.EVACUATE ? Set.of("old") : Collections.emptySet());
    Map<String, LiveInstance> assignable = new HashMap<>(live);
    if (operation == InstanceOperation.EVACUATE) {
      assignable.remove("old");
    }
    when(cache.getAssignableLiveInstances()).thenReturn(assignable);
    Set<String> disabled = operation == InstanceOperation.DISABLE ? Set.of("old") : Set.of();
    when(cache.getDisabledInstancesForPartition(RESOURCE_NAME, PARTITION_NAME)).thenReturn(disabled);
    when(cache.getAbnormalStateResolver("MasterSlave"))
        .thenReturn(MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);
    IdealState target = new IdealState(RESOURCE_NAME);
    target.setStateModelDefRef("MasterSlave");
    target.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    target.setReplicas("3");
    target.setPreferenceList(PARTITION_NAME,
        new ArrayList<>(List.of("master", "follower", "replacement")));
    when(cache.getIdealState(RESOURCE_NAME)).thenReturn(target);

    MaintenanceRebalancer rebalancer = new MaintenanceRebalancer();
    IdealState maintenanceTarget =
        rebalancer.computeNewIdealState(RESOURCE_NAME, target, current, cache);
    Map<String, String> assignment = rebalancer
        .computeBestPossiblePartitionState(cache, maintenanceTarget, resource, current)
        .getReplicaMap(partition);
    Assert.assertEquals(assignment.get("old"), expectedState);
    Assert.assertEquals(assignment.get("replacement"), "SLAVE");
    Assert.assertEquals(assignment.get("follower"), "SLAVE");
    Assert.assertEquals(current.getCurrentState(RESOURCE_NAME, partition, "old"), oldState);

    if (operation == InstanceOperation.EVACUATE && oldState.equals("OFFLINE")) {
      HelixManager manager = mock(HelixManager.class);
      when(manager.getInstanceName()).thenReturn("controller");
      when(manager.getSessionId()).thenReturn("controllerSession");
      ClusterEvent event = new ClusterEvent("maintenanceTest", ClusterEventType.ControllerChange);
      event.addAttribute(AttributeName.helixmanager.name(), manager);
      event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
      event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), Map.of(RESOURCE_NAME, resource));
      event.addAttribute(AttributeName.CURRENT_STATE.name(), current);
      BestPossibleStateOutput bestPossible = new BestPossibleStateOutput();
      bestPossible.setState(RESOURCE_NAME, partition, assignment);
      event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossible);
      new MessageGenerationPhase().process(event);
      MessageOutput messages = event.getAttribute(AttributeName.MESSAGES_ALL.name());
      Assert.assertTrue(messages.getMessages(RESOURCE_NAME, partition).isEmpty(),
          "Maintenance must not reactivate the drained replica");

      Map<String, String> afterMaintenance = new HashMap<>(assignment);
      afterMaintenance.put("old", "DROPPED");
      bestPossible.setState(RESOURCE_NAME, partition, afterMaintenance);
      new MessageGenerationPhase().process(event);
      messages = event.getAttribute(AttributeName.MESSAGES_ALL.name());
      List<Message> drops = messages.getMessages(RESOURCE_NAME, partition);
      Assert.assertEquals(drops.size(), 1);
      Assert.assertEquals(drops.get(0).getMsgType(), Message.MessageType.STATE_TRANSITION.name());
      Assert.assertEquals(drops.get(0).getTgtName(), "old");
      Assert.assertEquals(drops.get(0).getFromState(), "OFFLINE");
      Assert.assertEquals(drops.get(0).getToState(), "DROPPED");
    }
  }

  /**
   * Verifies that under maintenance mode, a partition with a non-empty preferenceList but
   * NO participant CurrentState gets its preferenceList cleared rather than preserved.
   * Preserving such an entry would cause the inherited mapping calculator to dispatch a
   * bootstrap that bypasses the per-pipeline capacity check in DelayedAutoRebalancer.
   * Regression test for the "MM bypass" issue where a WAGED-planned listFields entry for a
   * partition with no CurrentState survived MaintenanceRebalancer and got dispatched.
   */
  @Test
  public void testClearsListFieldsForPartitionWithoutCurrentState() {
    final String partWithCs = "p0_withCs";
    final String partWithoutCs = "p1_withoutCs";
    final String hostA = "hostA";
    final String hostB = "hostB";
    final String hostC = "hostC";
    final String hostD = "hostD";

    // Resource has two partitions:
    //   p0_withCs:    preferenceList=[hostA, hostB], currentState={hostA:MASTER, hostB:SLAVE}
    //   p1_withoutCs: preferenceList=[hostC, hostD], currentState={} (none reported)
    IdealState currentIdealState = new IdealState(RESOURCE_NAME);
    currentIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    currentIdealState.setRebalancerClassName(
        "org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
    currentIdealState.setStateModelDefRef("MasterSlave");
    currentIdealState.setPreferenceList(partWithCs, new ArrayList<>(Arrays.asList(hostA, hostB)));
    currentIdealState.setPreferenceList(partWithoutCs, new ArrayList<>(Arrays.asList(hostC, hostD)));

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Partition p0 = new Partition(partWithCs);
    currentStateOutput.setCurrentState(RESOURCE_NAME, p0, hostA, "MASTER");
    currentStateOutput.setCurrentState(RESOURCE_NAME, p0, hostB, "SLAVE");
    // p1_withoutCs: deliberately no setCurrentState calls

    ResourceControllerDataProvider dataCache = mock(ResourceControllerDataProvider.class);
    when(dataCache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());

    IdealState updated = new MaintenanceRebalancer()
        .computeNewIdealState(RESOURCE_NAME, currentIdealState, currentStateOutput, dataCache);

    // p0 retains its preferenceList (rebuilt from CS).
    List<String> p0List = updated.getPreferenceList(partWithCs);
    Assert.assertNotNull(p0List, "preferenceList for partition with CS should not be null");
    Assert.assertEquals(p0List.size(), 2,
        "preferenceList for partition with CS should have both hosts");
    Assert.assertTrue(p0List.contains(hostA) && p0List.contains(hostB),
        "preferenceList for partition with CS should contain both CS hosts");

    // p1 gets its preferenceList cleared because no participant reports CurrentState for it.
    List<String> p1List = updated.getPreferenceList(partWithoutCs);
    Assert.assertNotNull(p1List, "preferenceList for partition without CS should not be null");
    Assert.assertTrue(p1List.isEmpty(),
        "preferenceList for partition with no CurrentState should be empty under MM, "
            + "but was: " + p1List);
  }

  /**
   * Verifies that the existing Branch A behavior is preserved: if the entire resource has no
   * participant CurrentState reports, every partition's preferenceList is cleared.
   */
  @Test
  public void testClearsAllListFieldsWhenResourceHasNoCurrentState() {
    IdealState currentIdealState = new IdealState(RESOURCE_NAME);
    currentIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    currentIdealState.setRebalancerClassName(
        "org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
    currentIdealState.setStateModelDefRef("MasterSlave");
    currentIdealState.setPreferenceList("p0", new ArrayList<>(Arrays.asList("hostA", "hostB")));
    currentIdealState.setPreferenceList("p1", new ArrayList<>(Arrays.asList("hostC", "hostD")));

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    ResourceControllerDataProvider dataCache = mock(ResourceControllerDataProvider.class);
    when(dataCache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());

    IdealState updated = new MaintenanceRebalancer()
        .computeNewIdealState(RESOURCE_NAME, currentIdealState, currentStateOutput, dataCache);

    Assert.assertEquals(updated.getPreferenceList("p0"), Collections.emptyList(),
        "p0 preferenceList should be cleared (Branch A: no CS for any partition)");
    Assert.assertEquals(updated.getPreferenceList("p1"), Collections.emptyList(),
        "p1 preferenceList should be cleared (Branch A: no CS for any partition)");
  }

  /**
   * Verifies that a partition whose preferenceList is already empty stays empty (no spurious
   * "Clearing preferenceList" log line, no NPE).
   */
  @Test
  public void testEmptyPreferenceListStaysEmpty() {
    final String partWithCs = "p0_withCs";
    final String partWithoutCs = "p1_withoutCs";

    IdealState currentIdealState = new IdealState(RESOURCE_NAME);
    currentIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    currentIdealState.setRebalancerClassName(
        "org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
    currentIdealState.setStateModelDefRef("MasterSlave");
    currentIdealState.setPreferenceList(partWithCs, new ArrayList<>(Arrays.asList("hostA", "hostB")));
    currentIdealState.setPreferenceList(partWithoutCs, new ArrayList<String>());

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Partition p0 = new Partition(partWithCs);
    currentStateOutput.setCurrentState(RESOURCE_NAME, p0, "hostA", "MASTER");
    currentStateOutput.setCurrentState(RESOURCE_NAME, p0, "hostB", "SLAVE");

    ResourceControllerDataProvider dataCache = mock(ResourceControllerDataProvider.class);
    when(dataCache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());

    IdealState updated = new MaintenanceRebalancer()
        .computeNewIdealState(RESOURCE_NAME, currentIdealState, currentStateOutput, dataCache);

    Assert.assertEquals(updated.getPreferenceList(partWithCs).size(), 2);
    Assert.assertTrue(updated.getPreferenceList(partWithoutCs).isEmpty());
  }

}
