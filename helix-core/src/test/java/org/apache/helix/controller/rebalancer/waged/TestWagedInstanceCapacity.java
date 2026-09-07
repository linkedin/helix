package org.apache.helix.controller.rebalancer.waged;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestWagedInstanceCapacity {

  private static final int INSTANCE_COUNT = 3;
  private static final int RESOURCE_COUNT = 1;
  private static final int PARTITION_COUNT = 3;
  private static final List<String> CAPACITY_KEYS = Lists.newArrayList("CU", "PARTCOUNT", "DISK");
  private static final Map<String, Integer> DEFAULT_INSTANCE_CAPACITY_MAP =
      ImmutableMap.of("CU", 100, "PARTCOUNT", 10, "DISK", 100);

  private static final Map<String, Integer> DEFAULT_PART_CAPACITY_MAP =
      ImmutableMap.of("CU", 40, "PARTCOUNT", 1, "DISK", 1);

  private ResourceControllerDataProvider _clusterData;
  private Map<String, Resource> _resourceMap;
  private CurrentStateOutput _currentStateOutput;
  private WagedInstanceCapacity _wagedInstanceCapacity;

  @BeforeMethod
  public void setUp() {
    // prepare cluster data
    _clusterData = new ResourceControllerDataProvider();
    Map<String, InstanceConfig> instanceConfigMap = generateInstanceCapacityConfigs();
    _clusterData.setInstanceConfigMap(instanceConfigMap);
    _clusterData.setResourceConfigMap(generateResourcePartitionCapacityConfigs());
    _clusterData.setIdealStates(generateIdealStates());

    ClusterConfig clusterConfig = new ClusterConfig("test");
    clusterConfig.setTopologyAwareEnabled(false);
    clusterConfig.setInstanceCapacityKeys(CAPACITY_KEYS);
    _clusterData.setClusterConfig(clusterConfig);

    // prepare current state output
    _resourceMap = generateResourceMap();
    _currentStateOutput = populateCurrentStatesForResources(_resourceMap, instanceConfigMap.keySet());

    // prepare instance of waged-instance capacity
    _wagedInstanceCapacity = new WagedInstanceCapacity(_clusterData);
  }

  @Test
  public void testProcessCurrentState() {
    Map<String, Integer> partCapMap = ImmutableMap.of("CU", 10, "PARTCOUNT", 10, "DISK", 100);

    Assert.assertTrue(_wagedInstanceCapacity.checkAndReduceInstanceCapacity(
        "instance-0", "resource-0", "partition-0", partCapMap));

    Map<String, Integer> instanceAvailableCapacity = _wagedInstanceCapacity.getInstanceAvailableCapacity("instance-0");
    Assert.assertTrue(instanceAvailableCapacity.get("CU").equals(90));
  }

  @Test
  public void testProcessCurrentStateWithUnableToAssignPart() {
    Map<String, Integer> partCapMap = ImmutableMap.of("CU", 110, "PARTCOUNT", 10, "DISK", 100);

    Assert.assertFalse(_wagedInstanceCapacity.checkAndReduceInstanceCapacity(
        "instance-0", "resource-0", "partition-0", partCapMap));

    Map<String, Integer> instanceAvailableCapacity = _wagedInstanceCapacity.getInstanceAvailableCapacity("instance-0");
    Assert.assertTrue(instanceAvailableCapacity.get("CU").equals(100));
  }

  @Test
  public void testProcessCurrentStateWithDoubleCharge() {
    Map<String, Integer> partCapMap = ImmutableMap.of("CU", 10, "PARTCOUNT", 10, "DISK", 100);

    Assert.assertTrue(_wagedInstanceCapacity.checkAndReduceInstanceCapacity(
        "instance-0", "resource-0", "partition-0", partCapMap));

    // charge again
    Assert.assertTrue(_wagedInstanceCapacity.checkAndReduceInstanceCapacity(
        "instance-0", "resource-0", "partition-0", partCapMap));

    Map<String, Integer> instanceAvailableCapacity = _wagedInstanceCapacity.getInstanceAvailableCapacity("instance-0");
    Assert.assertTrue(instanceAvailableCapacity.get("CU").equals(90));
  }

  @Test
  public void testCapacitySnapshotIsAnIndependentDeepCopy() {
    Map<String, Integer> partCapMap = ImmutableMap.of("CU", 10, "PARTCOUNT", 1, "DISK", 1);
    WagedInstanceCapacity snapshot = new WagedInstanceCapacity(_wagedInstanceCapacity);

    // Mutating the original must not disturb the snapshot ...
    Assert.assertTrue(_wagedInstanceCapacity.checkAndReduceInstanceCapacity(
        "instance-0", "resource-0", "partition-0", partCapMap));
    Assert.assertEquals(_wagedInstanceCapacity.getInstanceAvailableCapacity("instance-0").get("CU"),
        Integer.valueOf(90));
    Assert.assertEquals(snapshot.getInstanceAvailableCapacity("instance-0").get("CU"),
        Integer.valueOf(100));

    // ... and the snapshot must not carry the original's allocation record either, otherwise a
    // restored ledger would treat the partition as already charged and skip the capacity check.
    Assert.assertTrue(snapshot.checkAndReduceInstanceCapacity(
        "instance-0", "resource-0", "partition-0", partCapMap));
    Assert.assertEquals(snapshot.getInstanceAvailableCapacity("instance-0").get("CU"),
        Integer.valueOf(90));
  }

  @Test
  public void testCapacityRejectionIsRecordedAndScopedToThePass() {
    _clusterData.setWagedCapacityProviders(_wagedInstanceCapacity,
        new WagedResourceWeightsProvider(_clusterData));

    // Each partition costs 40 CU against an instance capacity of 100, so the third one cannot fit.
    Assert.assertTrue(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-0"));
    Assert.assertTrue(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-1"));
    Assert.assertEquals(_clusterData.getCapacityRejectionCount(), 0);

    Assert.assertFalse(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-2"));

    // The rejection is recorded so the rebalancer can exclude this placement and re-plan.
    Assert.assertEquals(_clusterData.getCapacityRejectionCount(), 1);
    Assert.assertTrue(_clusterData.isCapacityRejected("instance-0", "resource-0", "partition-2"));
    // Only the rejected placement is excluded, not the partition everywhere else.
    Assert.assertFalse(_clusterData.isCapacityRejected("instance-1", "resource-0", "partition-2"));

    // Rejections describe one pass only; they must not leak into the next one.
    _clusterData.clearCapacityRejections();
    Assert.assertEquals(_clusterData.getCapacityRejectionCount(), 0);
    Assert.assertFalse(_clusterData.isCapacityRejected("instance-0", "resource-0", "partition-2"));
  }

  @Test
  public void testRepeatedRejectionOfSamePlacementIsStillCountedAsAnEvent() {
    _clusterData.setWagedCapacityProviders(_wagedInstanceCapacity,
        new WagedResourceWeightsProvider(_clusterData));

    Assert.assertTrue(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-0"));
    Assert.assertTrue(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-1"));
    Assert.assertFalse(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-2"));
    Assert.assertEquals(_clusterData.getCapacityRejectionCount(), 1);
    Assert.assertEquals(_clusterData.getCapacityRejectionEventCount(), 1L);

    // Rejecting the same placement again must not grow the deduplicated set, but must still be
    // observable as an event. A caller that watched only the deduplicated count would see no
    // change and wrongly conclude the placement had been accepted.
    Assert.assertFalse(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-2"));
    Assert.assertEquals(_clusterData.getCapacityRejectionCount(), 1);
    Assert.assertEquals(_clusterData.getCapacityRejectionEventCount(), 2L);

    _clusterData.clearCapacityRejections();
    Assert.assertEquals(_clusterData.getCapacityRejectionEventCount(), 0L);
  }

  @Test
  public void testRestoreCapacityUndoesChargesFromAPreviousAttempt() {
    _clusterData.setWagedCapacityProviders(_wagedInstanceCapacity,
        new WagedResourceWeightsProvider(_clusterData));
    WagedInstanceCapacity snapshot = _clusterData.snapshotWagedInstanceCapacity();

    Assert.assertTrue(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-0"));
    Assert.assertTrue(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-1"));
    Assert.assertFalse(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-2"));

    // A retry has to start from the same ledger the first attempt saw, otherwise it would be
    // charged against capacity the first attempt already consumed.
    _clusterData.restoreWagedInstanceCapacity(snapshot);
    Assert.assertEquals(
        _clusterData.getWagedInstanceCapacity().getInstanceAvailableCapacity("instance-0").get("CU"),
        Integer.valueOf(100));
    Assert.assertTrue(_clusterData.checkAndReduceCapacity("instance-0", "resource-0", "partition-2"));
  }
  private Map<String, InstanceConfig> generateInstanceCapacityConfigs() {
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();

    for (int i = 0; i < INSTANCE_COUNT; i ++) {
      String instanceName = "instance-" + i;
      InstanceConfig config = new InstanceConfig(instanceName);
      config.setInstanceCapacityMap(DEFAULT_INSTANCE_CAPACITY_MAP);
      instanceConfigMap.put(instanceName, config);
    }

    return instanceConfigMap;
  }

  private Map<String, ResourceConfig> generateResourcePartitionCapacityConfigs() {
    Map<String, ResourceConfig> resourceConfigMap = new HashMap<>();

    try {
      Map<String, Map<String, Integer>> partitionsCapacityMap = new HashMap<>();
      partitionsCapacityMap.put("DEFAULT", DEFAULT_PART_CAPACITY_MAP);

      for (String resourceName : getResourceNames()) {
        ResourceConfig config = new ResourceConfig(resourceName);
        config.setPartitionCapacityMap(partitionsCapacityMap);
        resourceConfigMap.put(resourceName, config);
      }
    } catch(IOException e) {
      throw new RuntimeException("error while setting partition capacity map");
    }
    return resourceConfigMap;
  }

  private List<IdealState> generateIdealStates() {
    return getResourceNames().stream()
        .map(resourceName -> {
          IdealState idealState = new IdealState(resourceName);
          idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
          idealState.setRebalancerClassName(WagedRebalancer.class.getName());
          return idealState;
        })
        .collect(Collectors.toList());
  }

  private static CurrentStateOutput populateCurrentStatesForResources(
      Map<String, Resource> resourceMap, Set<String> instanceNames) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();

    resourceMap.forEach((resourceName, resource) ->
      resource.getPartitions().forEach(partition -> {
        int masterPartIdx = RandomUtils.nextInt(0, instanceNames.size());
        int idx = 0;
        for (Iterator<String> it = instanceNames.iterator(); it.hasNext(); idx ++) {
          currentStateOutput.setCurrentState(
              resourceName, partition, it.next(), (idx == masterPartIdx) ? "MASTER" : "SLAVE");
        }
      }));

    return currentStateOutput;
  }

  private static Map<String, Resource> generateResourceMap() {
    return getResourceNames().stream()
        .map(resourceName -> {
          Resource resource = new Resource(resourceName);
          IntStream.range(0, PARTITION_COUNT)
              .mapToObj(i -> "partition-" + i)
              .forEach(resource::addPartition);
          return resource;
        })
        .collect(Collectors.toMap(Resource::getResourceName, Function.identity()));
  }

  private static List<String> getResourceNames() {
    return IntStream.range(0, RESOURCE_COUNT)
        .mapToObj(i -> "resource-" + i)
        .collect(Collectors.toList());
  }
}
