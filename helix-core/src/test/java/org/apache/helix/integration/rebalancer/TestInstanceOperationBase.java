package org.apache.helix.integration.rebalancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixRollbackException;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;


/**
 * Base class for TestInstanceOperation test suites. Provides shared cluster setup,
 * teardown, per-test isolation reset, helper/validation methods, and inner state model classes.
 *
 * Subclasses inherit a fully initialized Helix cluster with participants, controller,
 * verifiers, and three default test databases. The enhanced {@link #beforeMethod()} performs a
 * full isolation reset so that each test method starts from a known-good baseline, eliminating
 * the need for {@code dependsOnMethods} chains.
 */
public abstract class TestInstanceOperationBase extends ZkTestBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestInstanceOperationBase.class);

  public static final int TIMEOUT = 10000;
  protected final int ZONE_COUNT = 4;
  protected final int START_NUM_NODE = 10;
  protected static final int START_PORT = 12918;
  protected static final AtomicInteger _nextStartPort = new AtomicInteger(START_PORT);
  protected static final int PARTITIONS = 20;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected final String TEST_CAPACITY_KEY = "TestCapacityKey";
  protected final int TEST_CAPACITY_VALUE = 100;
  protected static final String ZONE = "zone";
  protected static final String HOST = "host";
  protected static final String LOGICAL_ID = "logicalId";
  protected static final String TOPOLOGY = String.format("%s/%s/%s", ZONE, HOST, LOGICAL_ID);
  protected static final ImmutableSet<String> TOP_STATE_SET = ImmutableSet.of("MASTER");
  protected static final ImmutableSet<String> SECONDARY_STATE_SET =
      ImmutableSet.of("SLAVE", "STANDBY");
  protected static final ImmutableSet<String> ACCEPTABLE_STATE_SET =
      ImmutableSet.of("MASTER", "LEADER", "SLAVE", "STANDBY");

  protected int REPLICA = 3;
  protected ClusterControllerManager _controller;
  protected HelixManager _spectator;
  protected RoutingTableProvider _routingTableProviderDefault;
  protected RoutingTableProvider _routingTableProviderEV;
  protected RoutingTableProvider _routingTableProviderCS;
  protected List<MockParticipantManager> _participants = new ArrayList<>();
  protected List<String> _participantNames = new ArrayList<>();
  protected Set<String> _allDBs = new HashSet<>();
  protected ZkHelixClusterVerifier _clusterVerifier;
  protected BestPossibleExternalViewVerifier _bestPossibleClusterVerifier;
  protected ConfigAccessor _configAccessor;
  protected long _stateModelDelay = 3L;
  protected final long DEFAULT_RESOURCE_DELAY_TIME = 1800000L;
  protected HelixAdmin _admin;
  protected AssignmentMetadataStore _assignmentMetadataStore;
  protected HelixDataAccessor _dataAccessor;

  private static final Set<String> DEFAULT_DBS =
      ImmutableSet.of("TEST_DB0_CRUSHED", "TEST_DB1_CRUSHED", "TEST_DB2_WAGED");

  // -----------------------------------------------------------------------
  // Lifecycle: @BeforeClass / @AfterClass / @BeforeMethod
  // -----------------------------------------------------------------------

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < START_NUM_NODE; i++) {
      String participantName = PARTICIPANT_PREFIX + "_" + _nextStartPort.get();
      addParticipant(participantName);
    }

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME)
        .setZkAddr(ZK_ADDR)
        .setDeactivatedNodeAwareness(true)
        .setResources(_allDBs)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
    _bestPossibleClusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME)
            .setZkAddr(ZK_ADDR)
            .setResources(_allDBs)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    _configAccessor = new ConfigAccessor(_gZkClient);
    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);

    _spectator = HelixManagerFactory.getZKHelixManager(
        CLUSTER_NAME, "spectator", InstanceType.SPECTATOR, ZK_ADDR);
    _spectator.connect();
    _routingTableProviderDefault = new RoutingTableProvider(_spectator);
    _routingTableProviderEV = new RoutingTableProvider(_spectator, PropertyType.EXTERNALVIEW);
    _routingTableProviderCS = new RoutingTableProvider(_spectator, PropertyType.CURRENTSTATES);

    setupClusterConfig();
    createTestDBs(DEFAULT_RESOURCE_DELAY_TIME);
    setUpWagedBaseline();

    _admin = new ZKHelixAdmin(_gZkClient);
  }

  @AfterClass
  public void afterClass() {
    for (String db : _allDBs) {
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    _controller.syncStop();
    _routingTableProviderDefault.shutdown();
    _routingTableProviderEV.shutdown();
    _routingTableProviderCS.shutdown();
    _spectator.disconnect();
  }

  /**
   * Full isolation reset executed before every test method. Ensures each test
   * starts from the same known-good baseline regardless of what previous tests
   * did to cluster state.
   */
  @BeforeMethod
  public void beforeMethod() throws Exception {
    // 1. Remove offline, disabled, or SWAP_IN instances
    removeOfflineOrInactiveInstances();

    // 2. Reset all remaining instance operations to ENABLE and clear disabled partitions
    for (int i = 0; i < _participants.size(); i++) {
      String participantName = _participantNames.get(i);
      InstanceConfig instanceConfig =
          _gSetupTool.getClusterManagementTool()
              .getInstanceConfig(CLUSTER_NAME, participantName);

      if (!instanceConfig.getInstanceOperation().getOperation()
          .equals(InstanceConstants.InstanceOperation.ENABLE)) {
        _gSetupTool.getClusterManagementTool()
            .setInstanceOperation(CLUSTER_NAME, participantName,
                InstanceConstants.InstanceOperation.ENABLE);
      }

      Map<String, List<String>> disabledPartitions = instanceConfig.getDisabledPartitionsMap();
      if (disabledPartitions != null && !disabledPartitions.isEmpty()) {
        for (String resource : new ArrayList<>(disabledPartitions.keySet())) {
          instanceConfig.setInstanceEnabledForPartition(resource, "", true);
        }
        _gSetupTool.getClusterManagementTool()
            .setInstanceConfig(CLUSTER_NAME, participantName, instanceConfig);
      }
    }

    // 3. Drop non-default resources
    for (String db : new ArrayList<>(_allDBs)) {
      if (!DEFAULT_DBS.contains(db)) {
        _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, db);
        _allDBs.remove(db);
      }
    }

    // 4. Recreate default DBs if any are missing
    for (String db : DEFAULT_DBS) {
      if (!_allDBs.contains(db)) {
        createTestDBs(DEFAULT_RESOURCE_DELAY_TIME);
        break;
      }
    }

    // 5. Restore participant count to START_NUM_NODE
    while (_participants.size() < START_NUM_NODE) {
      addParticipant(PARTICIPANT_PREFIX + "_" + _nextStartPort.get());
    }

    // 6. Reset state model delay to default
    _stateModelDelay = 3L;

    // 7. Exit maintenance mode (no-op if already out)
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // 8. Restore baseline cluster config
    setupClusterConfig();

    // 9. Disable topology-aware rebalance (clean baseline)
    disableTopologyAwareRebalance();

    // 10. Verify convergence
    Assert.assertTrue(_bestPossibleClusterVerifier.verifyByPolling());
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  // -----------------------------------------------------------------------
  // Cluster config helpers
  // -----------------------------------------------------------------------

  protected void setupClusterConfig() {
    _stateModelDelay = 3L;
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(1800000L);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  protected void enabledTopologyAwareRebalance() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setFaultZoneType(ZONE);
    clusterConfig.setTopologyAwareEnabled(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  protected void disableTopologyAwareRebalance() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopologyAwareEnabled(false);
    clusterConfig.setTopology(null);
    clusterConfig.setFaultZoneType(null);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  // -----------------------------------------------------------------------
  // Instance management helpers
  // -----------------------------------------------------------------------

  protected void removeOfflineOrInactiveInstances() {
    for (int i = 0; i < _participants.size(); i++) {
      String participantName = _participantNames.get(i);
      InstanceConfig instanceConfig =
          _gSetupTool.getClusterManagementTool()
              .getInstanceConfig(CLUSTER_NAME, participantName);
      if (!_participants.get(i).isConnected() || !instanceConfig.getInstanceEnabled()
          || instanceConfig.getInstanceOperation().getOperation()
          .equals(InstanceConstants.InstanceOperation.SWAP_IN)) {
        if (_participants.get(i).isConnected()) {
          _participants.get(i).syncStop();
        }
        _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME, instanceConfig);
        _participantNames.remove(i);
        _participants.remove(i);
        i--;
      }
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  protected MockParticipantManager createParticipant(String participantName,
      StateModelFactory stateModelFactory) throws Exception {
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName, 10, null);
    StateMachineEngine stateMachine = participant.getStateMachineEngine();
    stateMachine.registerStateModelFactory("MasterSlave",
        stateModelFactory != null ? stateModelFactory : new StDelayMSStateModelFactory());
    return participant;
  }

  protected void addParticipant(String participantName) throws Exception {
    addParticipant(participantName, UUID.randomUUID().toString(),
        "zone_" + _participants.size() % ZONE_COUNT, null, -1);
  }

  protected void addParticipant(String participantName, StateModelFactory stateModelFactory)
      throws Exception {
    addParticipant(participantName, UUID.randomUUID().toString(),
        "zone_" + _participants.size() % ZONE_COUNT, null, -1, null, stateModelFactory);
  }

  protected void addParticipant(String participantName, String logicalId, String zone,
      InstanceConstants.InstanceOperation instanceOperation, int capacity) throws Exception {
    addParticipant(participantName, logicalId, zone, instanceOperation, capacity, null, null);
  }

  protected void addParticipant(String participantName, String logicalId, String zone,
      InstanceConstants.InstanceOperation instanceOperation, int capacity,
      InstanceConfigChangeListener listener, StateModelFactory stateModelFactory)
      throws Exception {
    InstanceConfig config = new InstanceConfig.Builder().setDomain(
            String.format("%s=%s, %s=%s, %s=%s", ZONE, zone, HOST, participantName, LOGICAL_ID,
                logicalId)).setInstanceOperation(instanceOperation)
        .build(participantName);

    if (capacity >= 0) {
      config.setInstanceCapacityMap(ImmutableMap.of(TEST_CAPACITY_KEY, capacity));
    }
    _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, config);

    MockParticipantManager participant = createParticipant(participantName, stateModelFactory);
    participant.syncStart();
    if (listener != null) {
      participant.addListener(listener,
          new PropertyKey.Builder(CLUSTER_NAME).instanceConfig(participantName),
          HelixConstants.ChangeType.INSTANCE_CONFIG,
          new Watcher.Event.EventType[]{Watcher.Event.EventType.NodeDataChanged});
    }
    _participants.add(participant);
    _participantNames.add(participantName);
    _nextStartPort.getAndIncrement();
  }

  // -----------------------------------------------------------------------
  // Resource management helpers
  // -----------------------------------------------------------------------

  protected void createTestDBs(long delayTime) throws InterruptedException {
    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB0_CRUSHED",
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1, -1,
        CrushEdRebalanceStrategy.class.getName());
    _allDBs.add("TEST_DB0_CRUSHED");
    createResourceWithDelayedRebalance(CLUSTER_NAME, "TEST_DB1_CRUSHED",
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1,
        2000000, CrushEdRebalanceStrategy.class.getName());
    _allDBs.add("TEST_DB1_CRUSHED");
    createResourceWithWagedRebalance(CLUSTER_NAME, "TEST_DB2_WAGED",
        BuiltInStateModelDefinitions.LeaderStandby.name(), PARTITIONS, REPLICA, REPLICA - 1);
    _allDBs.add("TEST_DB2_WAGED");

    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  protected void dropTestDBs(Set<String> dbs) throws Exception {
    for (String db : dbs) {
      _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, db);
      _allDBs.remove(db);
    }
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  // -----------------------------------------------------------------------
  // Query helpers
  // -----------------------------------------------------------------------

  protected Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<>();
    for (String db : _allDBs) {
      ExternalView ev = _gSetupTool.getClusterManagementTool()
          .getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  protected Map<String, IdealState> getISs() {
    Map<String, IdealState> idealStates = new HashMap<>();
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, db);
      idealStates.put(db, is);
    }
    return idealStates;
  }

  protected boolean verifyIS(String evacuateInstanceName) {
    for (String db : _allDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, db);
      for (String partition : is.getPartitionSet()) {
        List<String> newPAssignedParticipants = is.getPreferenceList(partition);
        if (newPAssignedParticipants.contains(evacuateInstanceName)) {
          System.out.println("partition " + partition + " assignment "
              + newPAssignedParticipants + " ev " + evacuateInstanceName);
          return false;
        }
      }
    }
    return true;
  }

  protected Set<String> getParticipantsInEv(ExternalView ev) {
    Set<String> assignedParticipants = new HashSet<>();
    for (String partition : ev.getPartitionSet()) {
      ev.getStateMap(partition).keySet().stream()
          .filter(k -> !ev.getStateMap(partition).get(k).equals("OFFLINE"))
          .forEach(assignedParticipants::add);
    }
    return assignedParticipants;
  }

  protected Map<String, String> getPartitionsAndStatesOnInstance(
      Map<String, ExternalView> evs, String instanceName) {
    Map<String, String> instancePartitions = new HashMap<>();
    for (String resourceEV : evs.keySet()) {
      for (String partition : evs.get(resourceEV).getPartitionSet()) {
        if (evs.get(resourceEV).getStateMap(partition).containsKey(instanceName)) {
          instancePartitions.put(partition,
              evs.get(resourceEV).getStateMap(partition).get(instanceName));
        }
      }
    }
    return instancePartitions;
  }

  protected Map<String, Map<String, String>> getResourcePartitionStateOnInstance(
      Map<String, ExternalView> evs, String instanceName) {
    Map<String, Map<String, String>> stateByPartitionByResource = new HashMap<>();
    for (String resourceEV : evs.keySet()) {
      for (String partition : evs.get(resourceEV).getPartitionSet()) {
        if (evs.get(resourceEV).getStateMap(partition).containsKey(instanceName)) {
          if (!stateByPartitionByResource.containsKey(resourceEV)) {
            stateByPartitionByResource.put(resourceEV, new HashMap<>());
          }
          stateByPartitionByResource.get(resourceEV)
              .put(partition, evs.get(resourceEV).getStateMap(partition).get(instanceName));
        }
      }
    }
    return stateByPartitionByResource;
  }

  protected Set<String> getInstanceNames(Collection<InstanceConfig> instanceConfigs) {
    return instanceConfigs.stream().map(InstanceConfig::getInstanceName)
        .collect(Collectors.toSet());
  }

  // -----------------------------------------------------------------------
  // Validation helpers
  // -----------------------------------------------------------------------

  protected void validateRoutingTablesInstance(Map<String, ExternalView> evs,
      String instanceName, boolean shouldContain) {
    RoutingTableProvider[] routingTableProviders =
        new RoutingTableProvider[]{_routingTableProviderDefault, _routingTableProviderEV,
            _routingTableProviderCS};
    getResourcePartitionStateOnInstance(evs, instanceName).forEach((resource, partitions) -> {
      partitions.forEach((partition, state) -> {
        Arrays.stream(routingTableProviders).forEach(rtp -> Assert.assertEquals(
            getInstanceNames(rtp.getInstancesForResource(resource, partition, state))
                .contains(instanceName), shouldContain));
      });
    });
    Arrays.stream(routingTableProviders).forEach(rtp -> Assert.assertEquals(
        getInstanceNames(rtp.getInstanceConfigs()).contains(instanceName), shouldContain));
  }

  protected void validateEVCorrect(ExternalView actual, ExternalView original,
      Map<String, String> swapOutInstancesToSwapInInstances,
      Set<String> inFlightSwapInInstances, Set<String> completedSwapInInstanceNames,
      Set<String> allPartitionsDisabledInstances) {
    Assert.assertEquals(actual.getPartitionSet(), original.getPartitionSet());
    IdealState is = _gSetupTool.getClusterManagementTool()
        .getResourceIdealState(CLUSTER_NAME, original.getResourceName());
    StateModelDefinition stateModelDef = _gSetupTool.getClusterManagementTool()
        .getStateModelDef(CLUSTER_NAME, is.getStateModelDefRef());
    for (String partition : actual.getPartitionSet()) {
      Map<String, String> expectedStateMap = new HashMap<>(original.getStateMap(partition));
      for (String swapOutInstance : swapOutInstancesToSwapInInstances.keySet()) {
        if (expectedStateMap.containsKey(swapOutInstance) && inFlightSwapInInstances.contains(
            swapOutInstancesToSwapInInstances.get(swapOutInstance))) {
          String expectedState =
              expectedStateMap.get(swapOutInstance).equals(stateModelDef.getTopState())
                  ? (String) stateModelDef.getSecondTopStates().toArray()[0]
                  : expectedStateMap.get(swapOutInstance);
          if (allPartitionsDisabledInstances.contains(
              swapOutInstancesToSwapInInstances.get(swapOutInstance))) {
            expectedState = stateModelDef.getInitialState();
          }
          expectedStateMap.put(
              swapOutInstancesToSwapInInstances.get(swapOutInstance), expectedState);
        } else if (expectedStateMap.containsKey(swapOutInstance)
            && completedSwapInInstanceNames.contains(
            swapOutInstancesToSwapInInstances.get(swapOutInstance))) {
          expectedStateMap.put(swapOutInstancesToSwapInInstances.get(swapOutInstance),
              expectedStateMap.get(swapOutInstance));
          expectedStateMap.remove(swapOutInstance);
        }
      }
      Assert.assertEquals(actual.getStateMap(partition), expectedStateMap,
          "Error for partition " + partition + " in resource " + actual.getResourceName());
    }
  }

  protected boolean validateEVsCorrect(Map<String, ExternalView> actuals,
      Map<String, ExternalView> originals,
      Map<String, String> swapOutInstancesToSwapInInstances,
      Set<String> inFlightSwapInInstances, Set<String> completedSwapInInstanceNames) {
    Assert.assertEquals(actuals.keySet(), originals.keySet());
    for (String resource : actuals.keySet()) {
      validateEVCorrect(actuals.get(resource), originals.get(resource),
          swapOutInstancesToSwapInInstances, inFlightSwapInInstances,
          completedSwapInInstanceNames, Collections.emptySet());
    }
    return true;
  }

  protected void validateAssignmentInEv(ExternalView ev) {
    validateAssignmentInEv(ev, REPLICA);
  }

  protected void validateAssignmentInEv(ExternalView ev, int expectedNumber) {
    Set<String> partitionSet = ev.getPartitionSet();
    for (String partition : partitionSet) {
      AtomicInteger activeReplicaCount = new AtomicInteger();
      ev.getStateMap(partition).values().stream().filter(ACCEPTABLE_STATE_SET::contains)
          .forEach(v -> activeReplicaCount.getAndIncrement());
      Assert.assertTrue(activeReplicaCount.get() >= expectedNumber);
    }
  }

  protected void validateSwapCompletedSuccessfully(Map<String, ExternalView> afterEVs,
      String swapOutInstanceName, String swapInInstanceName) {
    for (String resource : _allDBs) {
      ExternalView ev = afterEVs.get(resource);
      boolean swapInHasPartitionsInResource = false;
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        Assert.assertFalse(stateMap.containsKey(swapOutInstanceName),
            "Swap-out instance " + swapOutInstanceName
                + " should not be in partition " + partition + " in resource " + resource);
        if (stateMap.containsKey(swapInInstanceName)) {
          swapInHasPartitionsInResource = true;
        }
      }
      validateAssignmentInEv(ev);
      Assert.assertTrue(swapInHasPartitionsInResource,
          "Swap-in instance " + swapInInstanceName
              + " should have at least one partition in resource " + resource);
    }
  }

  protected static void verifier(TestHelper.Verifier verifier, long timeout) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      try {
        boolean result = verifier.verify();
        if (!result) {
          LOG.error("Verifier returned false, retrying...");
        }
        return result;
      } catch (AssertionError e) {
        LOG.error("Caught AssertionError on verifier attempt: ", e);
        return false;
      }
    }, timeout));
  }

  // -----------------------------------------------------------------------
  // WAGED baseline setup
  // -----------------------------------------------------------------------

  protected void setUpWagedBaseline() {
    _assignmentMetadataStore =
        new AssignmentMetadataStore(new ZkBucketDataAccessor(ZK_ADDR), CLUSTER_NAME) {
          public Map<String, ResourceAssignment> getBaseline() {
            super.reset();
            return super.getBaseline();
          }

          public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
            super.reset();
            return super.getBestPossibleAssignment();
          }
        };

    ClusterConfig clusterConfig =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().clusterConfig());
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(TEST_CAPACITY_KEY));
    clusterConfig.setDefaultInstanceCapacityMap(
        Collections.singletonMap(TEST_CAPACITY_KEY, TEST_CAPACITY_VALUE));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(TEST_CAPACITY_KEY, 1));
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().clusterConfig(), clusterConfig);
  }

  // -----------------------------------------------------------------------
  // Inner classes: listeners and state models
  // -----------------------------------------------------------------------

  protected static class CustomIndividualInstanceConfigChangeListener
      implements InstanceConfigChangeListener {
    private boolean throttlesEnabled;

    public CustomIndividualInstanceConfigChangeListener() {
      throttlesEnabled = true;
    }

    public boolean isThrottlesEnabled() {
      return throttlesEnabled;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfig,
        NotificationContext context) {
      if (instanceConfig.get(0).getInstanceOperation().getOperation()
          .equals(InstanceConstants.InstanceOperation.SWAP_IN)) {
        throttlesEnabled = false;
      } else {
        throttlesEnabled = true;
      }
    }
  }

  public class StDelayMSStateModelFactory extends StateModelFactory<StDelayMSStateModel> {
    @Override
    public StDelayMSStateModel createNewStateModel(String resourceName, String partitionKey) {
      return new StDelayMSStateModel();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public class StDelayMSStateModel extends StateModel {
    public StDelayMSStateModel() {
      _cancelled = false;
    }

    private void sleepWhileNotCanceled(long sleepTime) throws InterruptedException {
      while (sleepTime > 0 && !isCancelled()) {
        Thread.sleep(TIMEOUT);
        sleepTime = sleepTime - TIMEOUT;
      }
      if (isCancelled()) {
        _cancelled = false;
        throw new HelixRollbackException("EX");
      }
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay < 0) {
        sleepWhileNotCanceled(Math.abs(_stateModelDelay));
      }
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay < 0) {
        sleepWhileNotCanceled(Math.abs(_stateModelDelay));
      }
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
        throws InterruptedException {
      if (_stateModelDelay > 0) {
        sleepWhileNotCanceled(_stateModelDelay);
      }
    }
  }

  public class StateTransitionCountStateModelFactory
      extends StateModelFactory<StateTransitionCountStateModel> {
    private final AtomicInteger _upwardStateTransitionCounter = new AtomicInteger(0);
    private final AtomicInteger _downwardStateTransitionCounter = new AtomicInteger(0);

    @Override
    public StateTransitionCountStateModel createNewStateModel(String resourceName,
        String partitionKey) {
      return new StateTransitionCountStateModel(
          _upwardStateTransitionCounter, _downwardStateTransitionCounter);
    }

    public int getUpwardStateTransitionCounter() {
      return _upwardStateTransitionCounter.get();
    }

    public int getDownwardStateTransitionCounter() {
      return _downwardStateTransitionCounter.get();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {"MASTER", "SLAVE", "ERROR"})
  public class StateTransitionCountStateModel extends StateModel {
    AtomicInteger _upwardStateTransitionCounter;
    AtomicInteger _downwardStateTransitionCounter;

    public StateTransitionCountStateModel(AtomicInteger upwardStateTransitionCounter,
        AtomicInteger downwardStateTransitionCounter) {
      _upwardStateTransitionCounter = upwardStateTransitionCounter;
      _downwardStateTransitionCounter = downwardStateTransitionCounter;
    }

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      _upwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      _upwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      _downwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      _downwardStateTransitionCounter.incrementAndGet();
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      _downwardStateTransitionCounter.incrementAndGet();
    }
  }
}
