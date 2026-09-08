package org.apache.helix.controller.rebalancer.waged.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedInstanceCapacity;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * The occupancy collection walks every instance in the cluster. Anything it looks up that depends
 * only on the resource, or only on (resource, partition), must therefore be resolved outside that
 * walk -- otherwise the work scales with instances x resources rather than with resources, which on
 * a large cluster is the difference between tens of milliseconds and hundreds per rebalance pass.
 * That regression was measured at roughly +450ms on a 1000-instance topology before the lookups
 * were hoisted, and it is invisible to every correctness test.
 * <p>
 * Timing assertions are too flaky to pin it, so this asserts the invariant that actually matters:
 * growing the cluster by 10x must not grow the number of per-resource lookups by 10x.
 */
public class TestUnallocatedOccupancyCollectionCost {
  private static final int RESOURCES = 5;
  private static final int PARTITIONS = 20;
  private static final int REPLICAS = 3;
  private static final String SESSION = "session0";
  private static final String CAP = "item1";

  private final List<String> _instanceNames = new ArrayList<>();
  private final Map<String, Map<String, CurrentState>> _currentStates = new HashMap<>();
  private final Map<String, IdealState> _idealStates = new HashMap<>();
  private final Map<String, ResourceConfig> _resourceConfigs = new HashMap<>();
  private final Map<String, Resource> _resourceMap = new HashMap<>();

  private int _instanceCount;

  private ResourceControllerDataProvider build(boolean flagOn) {
    ResourceControllerDataProvider cache = Mockito.mock(ResourceControllerDataProvider.class);

    ClusterConfig clusterConfig = new ClusterConfig("scaleCluster");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAP));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(CAP, 200));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(CAP, 1));
    clusterConfig.setWagedCountUnallocatedOccupancyEnabled(flagOn);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(cache.getAbnormalStateResolver(any()))
        .thenReturn(MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER);
    when(cache.getWagedInstanceCapacity()).thenReturn(Mockito.mock(WagedInstanceCapacity.class));

    Map<String, InstanceConfig> configs = new HashMap<>();
    Map<String, LiveInstance> lives = new HashMap<>();
    for (String instance : _instanceNames) {
      InstanceConfig ic = new InstanceConfig(instance);
      ic.setInstanceCapacityMap(Collections.singletonMap(CAP, 200));
      ic.setZoneId("zone" + (instance.hashCode() & 7));
      configs.put(instance, ic);
      LiveInstance li = new LiveInstance(instance);
      li.setSessionId(SESSION);
      lives.put(instance, li);
    }
    when(cache.getAssignableInstanceConfigMap()).thenReturn(configs);
    when(cache.getInstanceConfigMap()).thenReturn(configs);
    when(cache.getAssignableLiveInstances()).thenReturn(lives);

    when(cache.getIdealState(anyString()))
        .thenAnswer((Answer<IdealState>) c -> _idealStates.get(c.getArguments()[0]));
    when(cache.getResourceConfig(anyString()))
        .thenAnswer((Answer<ResourceConfig>) c -> _resourceConfigs.get(c.getArguments()[0]));
    when(cache.getStateModelDef(anyString())).thenReturn(org.apache.helix.model.
        BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition());
    when(cache.getCurrentState(anyString(), anyString())).thenAnswer(
        (Answer<Map<String, CurrentState>>) c -> _currentStates.get(c.getArguments()[0]));
    return cache;
  }

  private void buildTopology(int instanceCount) throws IOException {
    _instanceCount = instanceCount;
    for (int i = 0; i < instanceCount; i++) {
      _instanceNames.add("instance_" + i);
      _currentStates.put("instance_" + i, new HashMap<>());
    }
    for (int r = 0; r < RESOURCES; r++) {
      String resourceName = "Resource" + r;
      IdealState is = new IdealState(resourceName);
      is.setNumPartitions(PARTITIONS);
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setStateModelDefRef("MasterSlave");
      is.setReplicas(String.valueOf(REPLICAS));
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      _idealStates.put(resourceName, is);

      ResourceConfig rc = new ResourceConfig(resourceName);
      rc.setPartitionCapacityMap(Collections.singletonMap(
          ResourceConfig.DEFAULT_PARTITION_KEY, Collections.singletonMap(CAP, 1)));
      _resourceConfigs.put(resourceName, rc);

      Resource resource = new Resource(resourceName);
      Map<String, ZNRecord> perInstance = new HashMap<>();

      for (int p = 0; p < PARTITIONS; p++) {
        String partitionName = resourceName + "_" + p;
        resource.addPartition(partitionName);
        is.setPreferenceList(partitionName, Collections.emptyList());
        for (int k = 0; k < REPLICAS; k++) {
          String instance = _instanceNames.get((p * REPLICAS + k + r * 7) % _instanceCount);
          String state = k == 0 ? "MASTER" : "SLAVE";
          perInstance.computeIfAbsent(instance, x -> new ZNRecord(resourceName))
              .setMapField(partitionName, Collections.singletonMap("CURRENT_STATE", state));
        }
      }
      _resourceMap.put(resourceName, resource);

      perInstance.forEach((instance, record) -> {
        record.setSimpleField("SESSION_ID", SESSION);
        record.setSimpleField("STATE_MODEL_DEF", "MasterSlave");
        _currentStates.get(instance).put(resourceName, new CurrentState(record));
      });
    }
  }

  /** How many times the collection asked the cache for per-resource metadata. */
  private static int perResourceLookups(ResourceControllerDataProvider cache) {
    return (int) Mockito.mockingDetails(cache).getInvocations().stream()
        .filter(inv -> {
          String name = inv.getMethod().getName();
          return name.equals("getIdealState") || name.equals("getResourceConfig");
        }).count();
  }

  private int lookupsForClusterOfSize(int instanceCount) throws IOException {
    reset();
    buildTopology(instanceCount);
    ResourceControllerDataProvider cache = build(true);
    Map<String, ResourceAssignment> empty = new HashMap<>();
    _resourceMap.keySet().forEach(r -> empty.put(r, new ResourceAssignment(r)));
    ClusterModelProvider.generateClusterModelForPartialRebalance(cache, _resourceMap,
        new HashSet<>(_instanceNames), empty, empty);
    return perResourceLookups(cache);
  }

  private void reset() {
    _instanceNames.clear();
    _currentStates.clear();
    _idealStates.clear();
    _resourceConfigs.clear();
    _resourceMap.clear();
  }

  @Test
  public void testPerResourceLookupsDoNotScaleWithInstanceCount() throws IOException {
    int small = lookupsForClusterOfSize(20);
    int large = lookupsForClusterOfSize(200);

    // These lookups depend only on the resource set, which is identical in both runs, so the count
    // must not grow at all. An earlier version of this test allowed a 3x margin and did not fail
    // when the lookups were moved back inside the walk -- the observed growth was 230 to 630, which
    // slipped under the bound. Equality is the property that actually holds.
    Assert.assertTrue(large <= small,
        "per-resource lookups grew from " + small + " to " + large + " when the cluster grew 10x "
            + "with an unchanged resource set, which means something that depends only on the "
            + "resource is being resolved inside the per-instance walk again");
  }
}
