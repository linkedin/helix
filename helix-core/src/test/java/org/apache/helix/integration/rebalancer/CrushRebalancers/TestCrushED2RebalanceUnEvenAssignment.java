//package org.apache.helix.integration.rebalancer.CrushRebalancers;
//
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import org.apache.helix.ConfigAccessor;
//import org.apache.helix.TestHelper;
//import org.apache.helix.common.ZkTestBase;
//import org.apache.helix.controller.rebalancer.strategy.CrushEd2RebalanceStrategy;
//import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
//import org.apache.helix.integration.manager.ClusterControllerManager;
//import org.apache.helix.integration.manager.MockParticipantManager;
//import org.apache.helix.model.BuiltInStateModelDefinitions;
//import org.apache.helix.model.ClusterConfig;
//import org.apache.helix.model.ExternalView;
//import org.apache.helix.model.InstanceConfig;
//import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
//import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
//import org.apache.helix.zookeeper.datamodel.ZNRecord;
//import org.testng.Assert;
//import org.testng.annotations.AfterClass;
//import org.testng.annotations.AfterMethod;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.Test;
//
//
//public class TestCrushED2RebalanceUnEvenAssignment extends ZkTestBase {
//  protected static final int START_PORT = 12918;
//
//  protected final String CLASS_NAME = getShortClassName();
//  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
//  private final String TOPOLOGY = "/zone/instance";
//  private final String FAULT_ZONE_TYPE= "zone";
//  protected ClusterControllerManager _controller;
//  private final Integer PARTICIPANT_COUNT = 60;
//  List<MockParticipantManager> _participants = new ArrayList<>();
//  Map<String, Integer> _zoneToInstanceCountMap = new HashMap<>();
//  Map<String, Integer> _instanceToZoneTypeMap = new HashMap<>();
//  private ZkHelixClusterVerifier _clusterVerifier;
//  List<String> _nodes = new ArrayList<>();
//  Set<String> _allDBs = new HashSet<>();
//  int _REPLICA = 3;
//  ConfigAccessor _configAccessor;
//
//
//  @BeforeClass
//  public void beforeClass() throws Exception {
//
//    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
//
//    _gSetupTool.addCluster(CLUSTER_NAME, true);
//    _configAccessor = new ConfigAccessor(_gZkClient);
//    updateClusterConfig();
//
//    // start controller
//    String controllerName = CONTROLLER_PREFIX + "_0";
//    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
//  }
//
//  @AfterMethod
//  public void afterMethod() throws Exception {
//    for (String db : _allDBs) {
//      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, db);
//    }
//    _allDBs.clear();
//    // waiting for all DB be dropped.
//    Thread.sleep(100);
//    for (MockParticipantManager p : _participants) {
//      if (p != null && p.isConnected()) {
//        p.syncStop();
//      }
//    }
//    _participants.clear();
//    for (String instance : _nodes) {
//      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instance, false);
//      _gSetupTool.dropInstanceFromCluster(CLUSTER_NAME, instance);
//    }
//    _nodes.clear();
//    if (_controller != null && _controller.isConnected()) {
//      _controller.syncStop();
//    }
//  }
//  @AfterClass
//  public void afterClass() throws Exception {
//    deleteCluster(CLUSTER_NAME);
//    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
//  }
//
//  @Test
//  public void TestCrushEd2RebalanceUnevenAssignment() throws Exception {
//    updateSkewedZoneToInstanceMap();
//    createParticipants();
//    startController();
//    createResources();
//    Assert.assertTrue(_clusterVerifier.verifyByPolling());
//    validateAssignment(0.3);
//  }
//
//  @Test
//  public void TestCrushEd2RebalanceEvenAssignment() throws Exception {
//    updateEvenZoneToInstanceMap();
//    createParticipants();
//    startController();
//    createResources();
//    Assert.assertTrue(_clusterVerifier.verifyByPolling());
//    validateAssignment(0.1);
//  }
//
//  private void validateAssignment(Double threshold) {
//    Map<String, ExternalView> assignments = getEVs();
//    for (String resource : _allDBs) {
//      Map<String, List<String>> preferenceList = extractPreferenceList(assignments.get(resource).getRecord());
//      Map<Integer, List<Double>> avgPartitionsByZoneType = CrushED2TestUtils.getAvgPartitionsPerZoneType(preferenceList, _instanceToZoneTypeMap);
//      avgPartitionsByZoneType.forEach((zoneType, stats) -> {
//        Double skew = stats.get(1);
//        Assert.assertTrue(skew >= (1.0 - threshold) && skew <= (1.0 + threshold));
//      });
//      System.out.println("resource: " + resource + " " + avgPartitionsByZoneType);
//    }
//  }
//
//  private Map<String, List<String>> extractPreferenceList(ZNRecord record) {
//    Map<String, List<String>> preferenceLists = new HashMap<>();
//    Map<String, Map<String, String>> mapFields = record.getMapFields();
//
//    if (mapFields == null || mapFields.isEmpty()) {
//      return preferenceLists; // return empty map
//    }
//
//    mapFields.forEach((partition, instanceStateMap) -> {
//      List<String> instances = new ArrayList<>(instanceStateMap.keySet());
//      preferenceLists.put(partition, instances);
//    });
//
//    return preferenceLists;
//  }
//
//  private Map<String, ExternalView> getEVs() {
//    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
//    for (String db : _allDBs) {
//      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
//      externalViews.put(db, ev);
//    }
//    return externalViews;
//  }
//
//
//
//  private void createParticipants() {
//    int[] counter = {0};
//    _zoneToInstanceCountMap.forEach((zone, instanceCount) -> {
//      String zoneChar = zone.split("_")[1];
//
//      for (int i = 0; i < instanceCount; i++) {
//        String participantId = PARTICIPANT_PREFIX + "_" + zoneChar + "_" + i;
//        String participantName = PARTICIPANT_PREFIX + "_" + (START_PORT + counter[0]);
//
//        _gSetupTool.addInstanceToCluster(CLUSTER_NAME, participantName);
//        _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, participantName, true);
//        _nodes.add(participantName);
//        _instanceToZoneTypeMap.put(participantName, instanceCount);
//
//        String domain = String.format(
//            "zone=%s,instance=%s,applicationInstanceId=%s,host=%s",
//            zone, participantId, participantId, participantName
//        );
//
//        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, participantName);
//        instanceConfig.setDomain(domain);
//        _configAccessor.setInstanceConfig(CLUSTER_NAME, participantName, instanceConfig);
//        counter[0]++;
//      }
//    });
//    // start dummy participants
//    for (String node : _nodes) {
//      MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, node);
//      participant.syncStart();
//      _participants.add(participant);
//    }
//  }
//
//  private void startController() {
//    _controller.syncStart();
//    _clusterVerifier = new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
//        .setDeactivatedNodeAwareness(true)
//        .setResources(_allDBs)
//        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
//        .build();
//
//    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
//  }
//
//  private void updateSkewedZoneToInstanceMap() {
//    _zoneToInstanceCountMap.put("zone_A", 1);
//    _zoneToInstanceCountMap.put("zone_B", 8);
//    _zoneToInstanceCountMap.put("zone_C", 8);
//    _zoneToInstanceCountMap.put("zone_D", 8);
//    _zoneToInstanceCountMap.put("zone_E", 8);
//    _zoneToInstanceCountMap.put("zone_F", 8);
//    _zoneToInstanceCountMap.put("zone_G", 19);
//  }
//
//  private void updateEvenZoneToInstanceMap() {
//    _zoneToInstanceCountMap.put("zone_A", 8);
//    _zoneToInstanceCountMap.put("zone_B", 8);
//    _zoneToInstanceCountMap.put("zone_C", 8);
//    _zoneToInstanceCountMap.put("zone_D", 8);
//    _zoneToInstanceCountMap.put("zone_E", 9);
//    _zoneToInstanceCountMap.put("zone_F", 9);
//    _zoneToInstanceCountMap.put("zone_G", 9);
//  }
//
//
//}
