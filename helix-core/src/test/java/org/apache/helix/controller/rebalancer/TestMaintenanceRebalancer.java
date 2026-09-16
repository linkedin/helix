package org.apache.helix.controller.rebalancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Partition;
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
