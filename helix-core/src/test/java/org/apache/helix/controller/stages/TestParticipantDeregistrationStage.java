package org.apache.helix.controller.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ParticipantHistory;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestParticipantDeregistrationStage extends ZkTestBase {
  final static long DEREGISTER_TIMEOUT = 5000;
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private static final int NUM_NODES = 5;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private HelixAdmin _admin;
  private HelixDataAccessor _dataAccessor;
  private ClusterControllerManager _controller;
  private ConfigAccessor _configAccessor;
  private BestPossibleExternalViewVerifier _verifier;

  @BeforeClass
  public void beforeClass() {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + i;
      addParticipant(CLUSTER_NAME, instanceName);
    }

    _configAccessor = new ConfigAccessor(_gZkClient);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _admin = _gSetupTool.getClusterManagementTool();
    _dataAccessor = _controller.getHelixDataAccessor();

    setAutoDeregisterConfigs(CLUSTER_NAME, DEREGISTER_TIMEOUT);

    _verifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
  }

  // Asserts that a node will be removed from the cluster after it exceedsthe deregister timeout set in the cluster config
  @Test
  public void testParticipantAutoLeavesAfterOfflineTimeout() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipantManager participantToDeregister = _participants.get(0);
    participantToDeregister.syncStop();
    boolean result = TestHelper.verify(() -> !_admin.getInstancesInCluster(CLUSTER_NAME)
        .contains(participantToDeregister.getInstanceName()), TestHelper.WAIT_DURATION);
    Assert.assertTrue(result, "Participant should have been deregistered");

    dropParticipant(CLUSTER_NAME, participantToDeregister);
    addParticipant(CLUSTER_NAME, participantToDeregister.getInstanceName());
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  // Asserts that will not be removed from the cluster if it comes back online before the deregister timeout
  // and that the deregister timeout is reset, so the node will not be removed until time
  // of last offline + deregister timeout
  @Test (dependsOnMethods = "testParticipantAutoLeavesAfterOfflineTimeout")
  public void testReconnectedParticipantNotDeregisteredWhenLive() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipantManager participantToDeregister = _participants.get(0);
    // Kill instance so deregister is scheduled
    LiveInstance liveInstance = _dataAccessor.getProperty(
        _dataAccessor.keyBuilder().liveInstance(participantToDeregister.getInstanceName()));
    participantToDeregister.syncStop();

    // Sleep for half the deregister timeout
    Thread.sleep(DEREGISTER_TIMEOUT * 3/5);

    // Manually recreate live instance so controller thinks it's back online
    // This should prevent the node from being deregistered
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().liveInstance(participantToDeregister.getInstanceName()),
        liveInstance);

    // assert that the instance is still in the cluster
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < startTime + DEREGISTER_TIMEOUT) {
      Assert.assertTrue(_admin.getInstancesInCluster(CLUSTER_NAME)
          .contains(participantToDeregister.getInstanceName()), "Participant should not have been deregistered");
    }

    // Re kill and assert that the instance is deregistered
    _dataAccessor.removeProperty(_dataAccessor.keyBuilder().liveInstance(participantToDeregister.getInstanceName()));
    boolean result = TestHelper.verify(() -> !_admin.getInstancesInCluster(CLUSTER_NAME)
        .contains(participantToDeregister.getInstanceName()), TestHelper.WAIT_DURATION);
    Assert.assertTrue(result, "Participants should have been deregistered. Participants to deregister: "
        + participantToDeregister + " Remaining participants: in cluster " + _admin.getInstancesInCluster(CLUSTER_NAME));

    dropParticipant(CLUSTER_NAME, participantToDeregister);
    addParticipant(CLUSTER_NAME, participantToDeregister.getInstanceName());
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  // Same assertions as above but this time the node is re-killed immediately after being added back
  @Test (dependsOnMethods = "testReconnectedParticipantNotDeregisteredWhenLive")
  public void testFlappingParticipantIsNotDeregistered() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipantManager participantToDeregister = _participants.get(0);
    // Kill instance so deregister is scheduled
    LiveInstance liveInstance = _dataAccessor.getProperty(
        _dataAccessor.keyBuilder().liveInstance(participantToDeregister.getInstanceName()));
    participantToDeregister.syncStop();

    // Sleep for more than half the deregister timeout
    Thread.sleep(DEREGISTER_TIMEOUT * 3/5);

    // Manually recreate live instance so controller thinks it's back online, then immediately delete
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().liveInstance(participantToDeregister.getInstanceName()),
        liveInstance);
    ParticipantHistory participantHistory = _dataAccessor.getProperty(_dataAccessor.keyBuilder()
        .participantHistory(participantToDeregister.getInstanceName()));
    participantHistory.reportOnline("foo", "bar");
    _dataAccessor.setProperty(_dataAccessor.keyBuilder().participantHistory(participantToDeregister.getInstanceName()),
        participantHistory);

    _dataAccessor.removeProperty(_dataAccessor.keyBuilder().liveInstance(participantToDeregister.getInstanceName()));

    // assert that the instance is still in the cluster after original deregistration time should have passed
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < startTime + (DEREGISTER_TIMEOUT * 3/5)) {
      Assert.assertTrue(_admin.getInstancesInCluster(CLUSTER_NAME)
          .contains(participantToDeregister.getInstanceName()), "Participant should not have been deregistered");
    }

    // Re kill and assert that the instance is deregistered
    _dataAccessor.removeProperty(_dataAccessor.keyBuilder().liveInstance(participantToDeregister.getInstanceName()));
    boolean result = TestHelper.verify(() -> !_admin.getInstancesInCluster(CLUSTER_NAME)
        .contains(participantToDeregister.getInstanceName()), TestHelper.WAIT_DURATION);
    Assert.assertTrue(result, "Participants should have been deregistered. Participants to deregister: "
        + participantToDeregister + " Remaining participants: in cluster " + _admin.getInstancesInCluster(CLUSTER_NAME));

    dropParticipant(CLUSTER_NAME, participantToDeregister);
    addParticipant(CLUSTER_NAME, participantToDeregister.getInstanceName());
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  // Tests enabling deregister will trigger deregister for participants that were already offline
  @Test (dependsOnMethods = "testFlappingParticipantIsNotDeregistered")
  public void testDeregisterAfterConfigEnabled() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));

    // Set to deregister to disabled
    long testDeregisterTimeout = 1000;
    setAutoDeregisterConfigs(CLUSTER_NAME, -1);

    // Create and immediately kill participants
    List<MockParticipantManager> killedParticipants = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      MockParticipantManager participantToKill = addParticipant(CLUSTER_NAME, "participants_to_kill_" + i);
      participantToKill.syncStop();
      killedParticipants.add(participantToKill);
    }

    // Sleep so that participant offline time exceeds deregister timeout
    Thread.sleep(testDeregisterTimeout);
    // Trigger on disable --> enable deregister
    setAutoDeregisterConfigs(CLUSTER_NAME, DEREGISTER_TIMEOUT);

    // Assert participants have been deregistered
    boolean result = TestHelper.verify(() -> {
    List<String> instances = _admin.getInstancesInCluster(CLUSTER_NAME);
      return killedParticipants.stream().noneMatch(participant -> instances.contains(participant.getInstanceName()));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result, "Participants should have been deregistered. Participants to deregister: "
        + killedParticipants + " Remaining participants: in cluster " + _admin.getInstancesInCluster(CLUSTER_NAME));

    // reset cluster state
    killedParticipants.forEach(participant -> {
      dropParticipant(CLUSTER_NAME, participant);
    });
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  // Tests shortening deregister timeout will trigger deregister and also deregister participants that now exceed
  // the new (shorter) timeout
  @Test (dependsOnMethods = "testDeregisterAfterConfigEnabled")
  public void testDeregisterAfterConfigTimeoutShortened() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
    long longDeregisterTimeout = 1000*60*60*24;
    long shortDeregisterTimeout = 1000;
    setAutoDeregisterConfigs(CLUSTER_NAME, longDeregisterTimeout);

    // Create and immediately kill participants
    List<MockParticipantManager> killedParticipants = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      MockParticipantManager participantToKill = addParticipant(CLUSTER_NAME, "participants_to_kill_" + i);
      participantToKill.syncStop();
      killedParticipants.add(participantToKill);
    }

    // Sleep so that participant offline time exceeds deregister timeout
    Thread.sleep(shortDeregisterTimeout);

    // Trigger on shorten deregister timeout
    setAutoDeregisterConfigs(CLUSTER_NAME, shortDeregisterTimeout);

    // Assert participants have been deregistered
    boolean result = TestHelper.verify(() -> {
      List<String> instances = _admin.getInstancesInCluster(CLUSTER_NAME);
      return killedParticipants.stream().noneMatch(participant -> instances.contains(participant.getInstanceName()));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result, "Participants should have been deregistered. Participants to deregister: "
        + killedParticipants + " Remaining participants: in cluster " + _admin.getInstancesInCluster(CLUSTER_NAME));

    // reset cluster state
    killedParticipants.forEach(participant -> {
      dropParticipant(CLUSTER_NAME, participant);
    });
    setAutoDeregisterConfigs(CLUSTER_NAME, DEREGISTER_TIMEOUT);
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testParticipantDeregisteredAndRejoins() throws Exception {
    String firstDB = "firstDB";
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, firstDB, 3, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO.name(), null);
    IdealState idealStateOne =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, firstDB);
    idealStateOne.setMinActiveReplicas(2);
    idealStateOne.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, firstDB, idealStateOne);
    // Set replica count to be # instances in cluster + 1, so that we can ensure that each participant assigned a replica
    int replicaCount = _gSetupTool.getClusterManagementTool().getInstancesInCluster(CLUSTER_NAME).size();
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, firstDB, replicaCount);

    // Wait for cluster to converge
    Assert.assertTrue(_verifier.verifyByPolling());

    MockParticipantManager participantToDeregister = _participants.get(0);
    participantToDeregister.syncStop();
    boolean result = TestHelper.verify(() -> !_admin.getInstancesInCluster(CLUSTER_NAME)
        .contains(participantToDeregister.getInstanceName()), TestHelper.WAIT_DURATION);
    Assert.assertTrue(result, "Participant should have been deregistered");

    addParticipant(CLUSTER_NAME, participantToDeregister.getInstanceName());

    // Wait for cluster to converge
    Assert.assertTrue(_verifier.verifyByPolling());

    // loop through partitions and ensure that the participant is assigned to at least one partition
    ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, firstDB);
    boolean assigned = false;
    for (String partition : ev.getPartitionSet()) {
      if (ev.getStateMap(partition).containsKey(participantToDeregister.getInstanceName())) {
        assigned = true;
        break;
      }
    }
    Assert.assertTrue(assigned, "Participant should have been assigned to at least one partition");
  }

  // A drop that fails after deleting the InstanceConfig leaves the instance out of the offline time
  // map, so the stage must remember it and finish the drop on a later pass, but only while the
  // instance is still un-registered: a re-added instance must not be dropped.
  @Test
  public void testFailedDeregistrationIsResumedOnLaterPass() throws Exception {
    String clusterName = "TestDeregistrationResume";
    String instanceName = "host_partial";
    HelixAdmin admin = mock(HelixAdmin.class);
    BaseDataAccessor<ZNRecord> zkAccessor = mock(BaseDataAccessor.class);
    HelixManager manager = mockConnectedManager(clusterName, admin, zkAccessor);
    doThrow(new HelixException("simulated failure after the config was deleted")).doNothing()
        .when(admin).dropInstance(eq(clusterName), any(InstanceConfig.class));
    ParticipantDeregistrationStage stage = new ParticipantDeregistrationStage();

    // Pass 1: the instance is offline past the timeout and its drop fails partway.
    stage.execute(deregistrationEvent(manager,
        mockCache(clusterName, Collections.singletonMap(instanceName, 0L),
            Collections.singletonMap(instanceName, new InstanceConfig(instanceName)))));
    // Pass 2: the config is gone, so only the stage's own record leads it back to the instance.
    setRegistrationInZk(zkAccessor, clusterName, instanceName, false, true);
    ResourceControllerDataProvider configGone =
        mockCache(clusterName, Collections.emptyMap(), Collections.emptyMap());
    stage.execute(deregistrationEvent(manager, configGone));
    verify(admin, times(2)).dropInstance(eq(clusterName),
        argThat(config -> config.getInstanceName().equals(instanceName)));

    // Pass 3: the drop finished, so nothing is retried.
    stage.execute(deregistrationEvent(manager, configGone));
    verify(admin, times(2)).dropInstance(eq(clusterName), any(InstanceConfig.class));
  }

  @Test
  public void testFailedDeregistrationIsNotResumedAfterReAdd() throws Exception {
    String clusterName = "TestDeregistrationNoResumeAfterReAdd";
    String instanceName = "host_readded";
    HelixAdmin admin = mock(HelixAdmin.class);
    BaseDataAccessor<ZNRecord> zkAccessor = mock(BaseDataAccessor.class);
    HelixManager manager = mockConnectedManager(clusterName, admin, zkAccessor);
    doThrow(new HelixException("simulated failure after the config was deleted"))
        .when(admin).dropInstance(eq(clusterName), any(InstanceConfig.class));
    ParticipantDeregistrationStage stage = new ParticipantDeregistrationStage();

    stage.execute(deregistrationEvent(manager,
        mockCache(clusterName, Collections.singletonMap(instanceName, 0L),
            Collections.singletonMap(instanceName, new InstanceConfig(instanceName)))));
    // The instance was re-added before the next pass, but the cache doesn't show it yet.
    setRegistrationInZk(zkAccessor, clusterName, instanceName, true, true);
    stage.execute(deregistrationEvent(manager,
        mockCache(clusterName, Collections.emptyMap(), Collections.emptyMap())));

    verify(admin, times(1)).dropInstance(eq(clusterName), any(InstanceConfig.class));
  }

  private static HelixManager mockConnectedManager(String clusterName, HelixAdmin admin,
      BaseDataAccessor<ZNRecord> zkAccessor) {
    HelixManager manager = mock(HelixManager.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.isConnected()).thenReturn(true);
    when(manager.getClusterName()).thenReturn(clusterName);
    when(manager.getClusterManagmentTool()).thenReturn(admin);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.getBaseDataAccessor()).thenReturn(zkAccessor);
    return manager;
  }

  private static ResourceControllerDataProvider mockCache(String clusterName,
      Map<String, Long> offlineTimes, Map<String, InstanceConfig> instanceConfigs) {
    ClusterConfig clusterConfig = new ClusterConfig(clusterName);
    clusterConfig.setParticipantDeregistrationTimeout(0);
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getClusterName()).thenReturn(clusterName);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(cache.getInstanceOfflineTimeMap()).thenReturn(offlineTimes);
    when(cache.getLiveInstances()).thenReturn(Collections.emptyMap());
    when(cache.getInstanceConfigMap()).thenReturn(instanceConfigs);
    return cache;
  }

  private static void setRegistrationInZk(BaseDataAccessor<ZNRecord> zkAccessor,
      String clusterName, String instanceName, boolean hasConfig, boolean hasSubtree) {
    when(zkAccessor.exists(PropertyPathBuilder.instanceConfig(clusterName, instanceName),
        AccessOption.PERSISTENT)).thenReturn(hasConfig);
    when(zkAccessor.exists(PropertyPathBuilder.instance(clusterName, instanceName),
        AccessOption.PERSISTENT)).thenReturn(hasSubtree);
  }

  private static ClusterEvent deregistrationEvent(HelixManager manager,
      ResourceControllerDataProvider cache) {
    ClusterEvent event = new ClusterEvent(manager.getClusterName(), ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    return event;
  }

  @Override
  public void dropParticipant(String clusterName, MockParticipantManager participant) {
    _participants.remove(participant);
    super.dropParticipant(clusterName, participant);
  }

  @Override
  public MockParticipantManager addParticipant(String clusterName, String instanceName) {
    MockParticipantManager toAddParticipant = super.addParticipant(clusterName, instanceName);
    _participants.add(toAddParticipant);
    return toAddParticipant;
  }

  private void setAutoDeregisterConfigs(String clusterName, long timeout) {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    clusterConfig.setParticipantDeregistrationTimeout(timeout);
    _configAccessor.setClusterConfig(clusterName, clusterConfig);
    // Allow participant to ensure compatibility with nodes re-joining when they re-establish connection
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
            CLUSTER_NAME).build();
    _configAccessor.set(scope, ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
  }
}
