package org.apache.helix.util;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the convergence calculation itself. The cluster metadata is populated directly so that
 * every case, including ones a live cluster reaches only briefly, is deterministic.
 */
public class TestExternalViewConvergenceEvaluator {
  private static final String CLUSTER = "convergenceCluster";
  private static final String RESOURCE = "db0";
  private static final String PARTITION = RESOURCE + "_0";
  private static final String NODE_0 = "node0";
  private static final String NODE_1 = "node1";

  private HelixDataAccessor _accessor;
  private ResourceControllerDataProvider _cache;
  private ClusterConfig _clusterConfig;

  @BeforeMethod
  public void setUp() {
    _accessor = mock(HelixDataAccessor.class);
    when(_accessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER));
    setExternalViews(Collections.emptyMap());

    _clusterConfig = new ClusterConfig(CLUSTER);
    _cache = new ResourceControllerDataProvider(CLUSTER);
    _cache.setClusterConfig(_clusterConfig);
    _cache.setStateModelDefMap(
        Collections.singletonMap(BuiltInStateModelDefinitions.MasterSlave.name(),
            BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition()));
    setInstances(Arrays.asList(NODE_0, NODE_1), Arrays.asList(NODE_0, NODE_1));
  }


  @Test
  public void testExternalViewConvergenceResultBindsFromJson() throws Exception {
    String json = "{\"status\":\"PENDING\",\"observedAtMillis\":1,"
        + "\"evaluatedResourceCount\":1,\"pendingResources\":{\"db0\":\"MAPPING_MISMATCH\"},"
        + "\"failedResources\":{},\"unknownResources\":[],\"skippedResources\":[],"
        + "\"futureField\":\"ignored\"}";

    ExternalViewConvergenceResult result =
        new ObjectMapper().readValue(json, ExternalViewConvergenceResult.class);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.PENDING);
    Assert.assertFalse(result.isConverged());
    Assert.assertEquals(result.getPendingResources().get("db0"),
        ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH);
  }

  @Test
  public void testExternalViewConvergenceResultRejectsContradictingStatus() throws Exception {
    String json = "{\"status\":\"CONVERGED\",\"observedAtMillis\":1,"
        + "\"evaluatedResourceCount\":1,\"pendingResources\":{\"db0\":\"MAPPING_MISMATCH\"},"
        + "\"failedResources\":{},\"unknownResources\":[],\"skippedResources\":[]}";

    try {
      new ObjectMapper().readValue(json, ExternalViewConvergenceResult.class);
      Assert.fail("Expected contradicting convergence status to be rejected");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Invalid convergence status"));
    }
  }

  @Test
  public void testConvergedWhenExternalViewMatchesIdealMapping() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = evaluate(false);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.CONVERGED);
    Assert.assertTrue(result.isConverged());
    Assert.assertEquals(result.getEvaluatedResourceCount(), 1);
    Assert.assertTrue(result.getPendingResources().isEmpty());
    Assert.assertTrue(result.getFailedResources().isEmpty());
    Assert.assertNull(result.getClusterReason());
  }

  @Test
  public void testPendingWhenExternalViewDoesNotMatch() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    // The replica that should be MASTER is still SLAVE, which is the ordinary unstable state.
    setExternalViews(externalView(stateMapping(NODE_0, "SLAVE", NODE_1, "SLAVE")));

    ExternalViewConvergenceResult result = evaluate(false);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.PENDING);
    Assert.assertFalse(result.isConverged());
    Assert.assertEquals(result.getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH);
    Assert.assertEquals(result.getEvaluatedResourceCount(), 1);
  }

  @Test
  public void testLenientMatchIgnoresInitialAndDroppedStates() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    Map<String, String> stateMap = masterSlaveMapping(NODE_0, NODE_1).get(PARTITION);
    stateMap.put("leavingNode", "OFFLINE");
    stateMap.put("droppedNode", "DROPPED");
    setExternalViews(externalView(Collections.singletonMap(PARTITION, stateMap)));

    Assert.assertEquals(evaluate(true).getStatus(),
        ExternalViewConvergenceResult.Status.CONVERGED);
    Assert.assertEquals(evaluate(false).getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH);
  }

  @Test
  public void testMissingExternalViewIsPending() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));

    ExternalViewConvergenceResult result = evaluate(true);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.PENDING);
    Assert.assertEquals(result.getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.EXTERNAL_VIEW_MISSING);
  }

  @Test
  public void testResourceWithDisabledExternalViewIsNotEvaluated() {
    IdealState idealState = givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    idealState.setDisableExternalView(true);
    _cache.setIdealStates(Collections.singletonList(idealState));

    ExternalViewConvergenceResult result = evaluate(true);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.CONVERGED);
    Assert.assertEquals(result.getEvaluatedResourceCount(), 0);
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton(RESOURCE));
  }

  @Test
  public void testTaskResourcesAreIgnored() {
    IdealState idealState = new IdealState(RESOURCE);
    idealState.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setPreferenceList(PARTITION, Arrays.asList(NODE_0, NODE_1));
    _cache.setIdealStates(Collections.singletonList(idealState));

    ExternalViewConvergenceResult result = evaluate(true);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.CONVERGED);
    Assert.assertEquals(result.getEvaluatedResourceCount(), 0);
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton(RESOURCE));
  }

  @Test
  public void testRequestedTaskResourceIsSkippedNotUnknown() {
    IdealState taskIdealState = new IdealState(RESOURCE);
    taskIdealState.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    taskIdealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    taskIdealState.setPreferenceList(PARTITION, Arrays.asList(NODE_0, NODE_1));
    _cache.setIdealStates(Collections.singletonList(taskIdealState));

    ExternalViewConvergenceResult result =
        new ExternalViewConvergenceEvaluator.Builder().setLenientMatch(true).build()
            .evaluate(_accessor, _cache, Collections.singleton(RESOURCE), null);

    // The resource exists, so calling it unknown would be wrong; it is simply not compared.
    Assert.assertTrue(result.getUnknownResources().isEmpty());
    Assert.assertEquals(result.getSkippedResources(), Collections.singleton(RESOURCE));
    Assert.assertEquals(result.getEvaluatedResourceCount(), 0);
    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.CONVERGED);
  }

  @Test
  public void testTaskResourceWithExternalViewIsStillCompared() {
    IdealState taskIdealState = new IdealState(RESOURCE);
    taskIdealState.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    taskIdealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    taskIdealState.setPreferenceList(PARTITION, Arrays.asList(NODE_0, NODE_1));
    _cache.setIdealStates(Collections.singletonList(taskIdealState));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = evaluate(true);

    // An external view left behind is compared against an empty ideal state, as it has always
    // been, so it is not reported as skipped.
    Assert.assertTrue(result.getSkippedResources().isEmpty());
    Assert.assertEquals(result.getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH);
  }

  @Test
  public void testExternalViewWithoutIdealStateIsPending() {
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = evaluate(true);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.PENDING);
    Assert.assertEquals(result.getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH);
  }

  @Test
  public void testCustomizedResourceComparesIdealStateMapping() {
    IdealState idealState = new IdealState(RESOURCE);
    idealState.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.getRecord().setMapFields(masterSlaveMapping(NODE_0, NODE_1));
    _cache.setIdealStates(Collections.singletonList(idealState));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    Assert.assertEquals(evaluate(true).getStatus(),
        ExternalViewConvergenceResult.Status.CONVERGED);

    setExternalViews(externalView(stateMapping(NODE_0, "SLAVE", NODE_1, "MASTER")));
    Assert.assertEquals(evaluate(true).getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.MAPPING_MISMATCH);
  }

  @Test
  public void testFullAutoWithoutPersistedAssignmentFails() {
    givenFullAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = evaluate(true);

    // Without a persisted assignment there is nothing to compare against, so this is not a state
    // that resolves by waiting.
    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.FAILED);
    Assert.assertEquals(result.getFailedResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.BEST_POSSIBLE_ASSIGNMENT_NOT_PERSISTED);
  }

  @Test
  public void testFullAutoWithPersistedAssignmentIsEvaluated() {
    _clusterConfig.setPersistBestPossibleAssignment(true);
    _cache.setClusterConfig(_clusterConfig);
    givenFullAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    Assert.assertEquals(evaluate(true).getStatus(),
        ExternalViewConvergenceResult.Status.CONVERGED);
  }

  @Test
  public void testFullAutoWithEmptyPreferenceListIsPending() {
    _clusterConfig.setPersistIntermediateAssignment(true);
    _cache.setClusterConfig(_clusterConfig);
    givenFullAutoResource(Collections.emptyList());
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = evaluate(true);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.PENDING);
    Assert.assertEquals(result.getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.PREFERENCE_LIST_EMPTY);
  }

  @Test
  public void testFullAutoWithoutClusterConfigIsAReadFailure() {
    givenFullAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));
    _cache.setClusterConfig(null);

    try {
      evaluate(true);
      Assert.fail("A cluster config that could not be read must not look like a converged cluster");
    } catch (HelixException expected) {
      Assert.assertTrue(expected.getMessage().contains("Cluster config is unavailable"),
          expected.getMessage());
    }
  }

  @Test
  public void testMissingStateModelDefinitionFails() {
    _cache.setStateModelDefMap(Collections.emptyMap());
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = evaluate(true);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.FAILED);
    Assert.assertEquals(result.getFailedResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.STATE_MODEL_DEFINITION_MISSING);
  }

  @Test
  public void testStateModelInitialStateDecidesWhatLenientMatchIgnores() {
    StateModelDefinition onlineOffline =
        BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition();
    Map<String, StateModelDefinition> stateModelDefs = new HashMap<>();
    stateModelDefs.put(BuiltInStateModelDefinitions.OnlineOffline.name(), onlineOffline);
    _cache.setStateModelDefMap(stateModelDefs);

    IdealState idealState = new IdealState(RESOURCE);
    idealState.setStateModelDefRef(BuiltInStateModelDefinitions.OnlineOffline.name());
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setReplicas("1");
    idealState.setPreferenceList(PARTITION, Collections.singletonList(NODE_0));
    _cache.setIdealStates(Collections.singletonList(idealState));

    Map<String, String> stateMap = new HashMap<>();
    stateMap.put(NODE_0, "ONLINE");
    // OFFLINE is the initial state of this model, so lenient matching ignores it.
    stateMap.put(NODE_1, "OFFLINE");
    setExternalViews(externalView(Collections.singletonMap(PARTITION, stateMap)));

    Assert.assertEquals(evaluate(true).getStatus(),
        ExternalViewConvergenceResult.Status.CONVERGED);
    Assert.assertEquals(evaluate(false).getStatus(),
        ExternalViewConvergenceResult.Status.PENDING);
  }

  @Test
  public void testLiveInstanceExpectationMismatchIsPending() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result =
        new ExternalViewConvergenceEvaluator.Builder().setLenientMatch(true).build()
            .evaluate(_accessor, _cache, null, new HashSet<>(Arrays.asList(NODE_0, "absentNode")));

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.PENDING);
    Assert.assertEquals(result.getClusterReason(),
        ExternalViewConvergenceResult.Reason.LIVE_INSTANCES_MISMATCH);
    Assert.assertEquals(result.getEvaluatedResourceCount(), 0);
  }

  @Test
  public void testResourceScopeEvaluatesOnlyRequestedResources() {
    IdealState converged = semiAutoIdealState(RESOURCE, Arrays.asList(NODE_0, NODE_1));
    IdealState pending = semiAutoIdealState("db1", Arrays.asList(NODE_0, NODE_1));
    _cache.setIdealStates(Arrays.asList(converged, pending));
    Map<String, ExternalView> externalViews = new HashMap<>();
    externalViews.put(RESOURCE, new ExternalView(
        externalViewRecord(RESOURCE, masterSlaveMapping(NODE_0, NODE_1))));
    setExternalViews(externalViews);

    ExternalViewConvergenceResult scoped = new ExternalViewConvergenceEvaluator.Builder()
        .setLenientMatch(true).build()
        .evaluate(_accessor, _cache, Collections.singleton(RESOURCE), null);
    Assert.assertEquals(scoped.getStatus(), ExternalViewConvergenceResult.Status.CONVERGED);
    Assert.assertEquals(scoped.getEvaluatedResourceCount(), 1);

    // The cluster as a whole has not converged, so the scoped answer must not be read as one.
    Assert.assertEquals(evaluate(true).getStatus(),
        ExternalViewConvergenceResult.Status.PENDING);
  }

  @Test
  public void testUnknownRequestedResourceFails() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = new ExternalViewConvergenceEvaluator.Builder()
        .setLenientMatch(true).build()
        .evaluate(_accessor, _cache, new HashSet<>(Arrays.asList(RESOURCE, "noSuchResource")),
            null);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.FAILED);
    Assert.assertEquals(result.getUnknownResources(),
        Collections.singleton("noSuchResource"));
    Assert.assertEquals(result.getEvaluatedResourceCount(), 1);
  }

  @Test
  public void testUnknownRequestedResourceIsIgnoredInVerifierCompatibleMode() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    ExternalViewConvergenceResult result = new ExternalViewConvergenceEvaluator.Builder()
        .setLenientMatch(true).setFailOnUnknownResources(false).build()
        .evaluate(_accessor, _cache, new HashSet<>(Arrays.asList(RESOURCE, "noSuchResource")),
            null);

    Assert.assertEquals(result.getStatus(), ExternalViewConvergenceResult.Status.CONVERGED);
    Assert.assertTrue(result.getUnknownResources().isEmpty());
  }

  @Test
  public void testStopAtFirstIssueReportsOneResourceAndFullScanReportsAll() {
    _cache.setIdealStates(Arrays.asList(semiAutoIdealState(RESOURCE, Arrays.asList(NODE_0, NODE_1)),
        semiAutoIdealState("db1", Arrays.asList(NODE_0, NODE_1))));

    ExternalViewConvergenceResult full = evaluate(true);
    Assert.assertEquals(full.getPendingResources().size(), 2);

    ExternalViewConvergenceResult shortCircuited =
        new ExternalViewConvergenceEvaluator.Builder().setLenientMatch(true)
            .setStopAtFirstIssue(true).build().evaluate(_accessor, _cache, null, null);
    Assert.assertEquals(shortCircuited.getStatus(),
        ExternalViewConvergenceResult.Status.PENDING);
    Assert.assertEquals(shortCircuited.getPendingResources().size(), 1);
  }

  @Test
  public void testFailedExternalViewReadIsNotAnEmptyConvergedCluster() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    doThrow(new HelixException("external view read failed")).when(_accessor)
        .getChildValuesMap(any(PropertyKey.class), eq(true));

    try {
      evaluate(true);
      Assert.fail("A failed external view read must not be reported as a converged cluster");
    } catch (HelixException expected) {
      Assert.assertEquals(expected.getMessage(), "external view read failed");
    }
  }

  @Test
  public void testNullExternalViewReadIsTreatedAsNoExternalViews() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    doReturn(null).when(_accessor).getChildValuesMap(any(PropertyKey.class), eq(true));

    Assert.assertEquals(evaluate(true).getPendingResources().get(RESOURCE),
        ExternalViewConvergenceResult.Reason.EXTERNAL_VIEW_MISSING);
  }

  @Test
  public void testObservationTimeIsTakenAtEvaluation() throws InterruptedException {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    long before = System.currentTimeMillis();
    ExternalViewConvergenceResult first = evaluate(true);
    Thread.sleep(5);
    ExternalViewConvergenceResult second = evaluate(true);
    long after = System.currentTimeMillis();

    Assert.assertTrue(first.getObservedAtMillis() >= before);
    Assert.assertTrue(second.getObservedAtMillis() > first.getObservedAtMillis());
    Assert.assertTrue(second.getObservedAtMillis() <= after);
  }

  @Test
  public void testEvaluationDoesNotWriteClusterMetadata() {
    givenSemiAutoResource(Arrays.asList(NODE_0, NODE_1));
    setExternalViews(externalView(masterSlaveMapping(NODE_0, NODE_1)));

    evaluate(true);

    // Observing convergence must not change what is being observed.
    verify(_accessor, never()).setProperty(any(PropertyKey.class), any());
    verify(_accessor, never()).updateProperty(any(PropertyKey.class), any());
    verify(_accessor, never()).updateProperty(any(PropertyKey.class), any(DataUpdater.class),
        any());
    verify(_accessor, never()).removeProperty(any(PropertyKey.class));
    verify(_accessor, never()).setChildren(anyList(), anyList());
  }

  private ExternalViewConvergenceResult evaluate(boolean lenientMatch) {
    return new ExternalViewConvergenceEvaluator.Builder().setLenientMatch(lenientMatch).build()
        .evaluate(_accessor, _cache, null, null);
  }

  private IdealState givenSemiAutoResource(List<String> preferenceList) {
    IdealState idealState = semiAutoIdealState(RESOURCE, preferenceList);
    _cache.setIdealStates(Collections.singletonList(idealState));
    return idealState;
  }

  private void givenFullAutoResource(List<String> preferenceList) {
    IdealState idealState = semiAutoIdealState(RESOURCE, preferenceList);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    _cache.setIdealStates(Collections.singletonList(idealState));
  }

  private static IdealState semiAutoIdealState(String resource, List<String> preferenceList) {
    IdealState idealState = new IdealState(resource);
    idealState.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    idealState.setReplicas(String.valueOf(Math.max(preferenceList.size(), 1)));
    idealState.setPreferenceList(resource + "_0", preferenceList);
    return idealState;
  }

  private void setInstances(List<String> instanceNames, List<String> liveInstanceNames) {
    List<LiveInstance> liveInstances = new ArrayList<>();
    for (String instanceName : liveInstanceNames) {
      LiveInstance liveInstance = new LiveInstance(instanceName);
      liveInstance.setSessionId("session-" + instanceName);
      liveInstance.setHelixVersion("1.0.0");
      liveInstances.add(liveInstance);
    }
    Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
    for (String instanceName : instanceNames) {
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      instanceConfig.setInstanceEnabled(true);
      instanceConfigs.put(instanceName, instanceConfig);
    }
    // Live instances first: the instance config setter is what derives the assignable and enabled
    // sets from both inputs.
    _cache.setLiveInstances(liveInstances);
    _cache.setInstanceConfigMap(instanceConfigs);
  }

  private void setExternalViews(Map<String, ExternalView> externalViews) {
    doReturn(externalViews).when(_accessor).getChildValuesMap(any(PropertyKey.class), eq(true));
  }

  private static Map<String, ExternalView> externalView(
      Map<String, Map<String, String>> mapFields) {
    return Collections.singletonMap(RESOURCE,
        new ExternalView(externalViewRecord(RESOURCE, mapFields)));
  }

  private static ZNRecord externalViewRecord(String resource,
      Map<String, Map<String, String>> mapFields) {
    ZNRecord record = new ZNRecord(resource);
    record.setMapFields(mapFields);
    return record;
  }

  private static Map<String, Map<String, String>> masterSlaveMapping(String master, String slave) {
    return stateMapping(master, "MASTER", slave, "SLAVE");
  }

  private static Map<String, Map<String, String>> stateMapping(String firstInstance,
      String firstState, String secondInstance, String secondState) {
    Map<String, String> stateMap = new HashMap<>();
    stateMap.put(firstInstance, firstState);
    stateMap.put(secondInstance, secondState);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    mapFields.put(PARTITION, stateMap);
    return mapFields;
  }
}
