package org.apache.helix.rest.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.apache.helix.util.ConfigStringUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * REST behaviour of the pair-scoped swap commands: what a caller has to send, what it gets back,
 * and that the older swap commands still answer in the shape they always did.
 */
public class TestPairScopedSwapAPI extends AbstractTestClass {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CLUSTER_NAME = "TestCluster_1";
  private static final String HOST_KEY = "host";
  private static final String FAULT_ZONE = "restZoneA";
  private static final Entity<String> EMPTY_BODY =
      Entity.entity("", MediaType.APPLICATION_JSON_TYPE);

  @Test
  public void testCoordinatedPrepareAndCompleteOverRest() throws Exception {
    String swapOut = addInstance("pairSwapRestOut_12000", "restSlotA", "swap-out-host");
    String swapIn = addInstance("pairSwapRestIn_12001", "restSpareA", "swap-in-host");
    setInstanceOperationDirectly(swapIn, InstanceConstants.InstanceOperation.UNKNOWN);

    Map<String, Object> identities = post(swapOut, "getSwapPairIdentities", false,
        body(swapOut, swapIn, null, null));
    Map<String, Object> swapInIdentity = (Map<String, Object>) identities.get("swapIn");
    Assert.assertEquals(swapInIdentity.get("exists"), Boolean.TRUE);
    Assert.assertNotNull(swapInIdentity.get("configVersion"));
    Assert.assertNotNull(swapInIdentity.get("configCreationId"));

    Map<String, Object> prepared =
        post(swapOut, "prepareSwapPair", false, body(swapOut, swapIn, "COORDINATED", null));
    Assert.assertEquals(prepared.get("status"), "PREPARED", prepared.toString());
    Assert.assertEquals(prepared.get("successful"), Boolean.TRUE);
    Assert.assertTrue(((List<String>) prepared.get("blockers")).isEmpty());
    Assert.assertEquals(instanceConfig(swapIn).getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.SWAP_IN);
    Map<String, Object> observedSwapIn = (Map<String, Object>) prepared.get("observedSwapIn");
    Assert.assertEquals(observedSwapIn.get("instanceName"), swapIn);

    Map<String, Object> replayedPrepare =
        post(swapOut, "prepareSwapPair", false, body(swapOut, swapIn, "COORDINATED", null));
    Assert.assertEquals(replayedPrepare.get("status"), "ALREADY_PREPARED",
        replayedPrepare.toString());

    // Without force the swap is not ready, because neither instance is live.
    Map<String, Object> notReady =
        post(swapOut, "completeSwapPair", false, body(swapOut, swapIn, "COORDINATED", null));
    Assert.assertEquals(notReady.get("status"), "NOT_READY", notReady.toString());
    Assert.assertEquals(notReady.get("successful"), Boolean.FALSE);
    Assert.assertFalse(((List<String>) notReady.get("blockers")).isEmpty());

    Map<String, Object> completed =
        post(swapOut, "completeSwapPair", true, body(swapOut, swapIn, "COORDINATED", null));
    Assert.assertEquals(completed.get("status"), "COMPLETED", completed.toString());
    Assert.assertEquals(instanceConfig(swapOut).getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);

    Map<String, Object> replayedComplete =
        post(swapOut, "completeSwapPair", true, body(swapOut, swapIn, "COORDINATED", null));
    Assert.assertEquals(replayedComplete.get("status"), "ALREADY_COMPLETED",
        replayedComplete.toString());
    Assert.assertEquals(replayedComplete.get("successful"), Boolean.TRUE);
  }

  @Test
  public void testDirectPrepareOverRestKeepsPreservedDomainKeys() throws Exception {
    String swapOut = addInstance("pairSwapRestDirectOut_12002", "restSlotB", "swap-out-host");
    String swapIn = addInstance("pairSwapRestDirectIn_12003", "restSpareB", "swap-in-host");
    setInstanceOperationDirectly(swapIn, InstanceConstants.InstanceOperation.UNKNOWN);

    String directBody =
        body(swapOut, swapIn, "DIRECT", "\"preservedSwapInDomainKeys\":[\"" + HOST_KEY + "\"]");
    Map<String, Object> prepared = post(swapOut, "prepareSwapPair", false, directBody);

    Assert.assertEquals(prepared.get("status"), "PREPARED", prepared.toString());
    InstanceConfig swapInConfig = instanceConfig(swapIn);
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(HOST_KEY), "swap-in-host");
    Assert.assertEquals(swapInConfig.getDomainAsMap().get(logicalIdKey()), "restSlotB");
    // A direct preparation must not mark the swap-in, or it would become assignable in the slot it
    // has not taken over yet.
    Assert.assertEquals(swapInConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.UNKNOWN);
  }

  @Test
  public void testStaleExpectedIdentityIsReportedNotApplied() throws Exception {
    String swapOut = addInstance("pairSwapRestStaleOut_12004", "restSlotC", "swap-out-host");
    String swapIn = addInstance("pairSwapRestStaleIn_12005", "restSpareC", "swap-in-host");
    setInstanceOperationDirectly(swapIn, InstanceConstants.InstanceOperation.UNKNOWN);

    Map<String, Object> identities = post(swapOut, "getSwapPairIdentities", false,
        body(swapOut, swapIn, null, null));
    Map<String, Object> swapInIdentity = (Map<String, Object>) identities.get("swapIn");
    int staleVersion = ((Number) swapInIdentity.get("configVersion")).intValue() + 7;
    long creationId = ((Number) swapInIdentity.get("configCreationId")).longValue();

    Map<String, Object> refused = post(swapOut, "prepareSwapPair", false,
        body(swapOut, swapIn, "COORDINATED",
            "\"expectedSwapInConfigVersion\":" + staleVersion
                + ",\"expectedSwapInConfigCreationId\":" + creationId));

    Assert.assertEquals(refused.get("status"), "IDENTITY_MISMATCH", refused.toString());
    Assert.assertEquals(refused.get("successful"), Boolean.FALSE);
    Assert.assertEquals(instanceConfig(swapIn).getDomainAsMap().get(logicalIdKey()), "restSpareC");
  }

  @Test
  public void testMalformedSwapPairRequestsAreRejected() throws Exception {
    String swapOut = addInstance("pairSwapRestBadOut_12006", "restSlotD", "swap-out-host");
    String swapIn = addInstance("pairSwapRestBadIn_12007", "restSpareD", "swap-in-host");

    assertBadRequest(swapOut, "prepareSwapPair", "{\"swapOutInstanceName\":\"" + swapOut + "\"}");
    assertBadRequest(swapOut, "prepareSwapPair", body(swapOut, swapIn, null, null));
    assertBadRequest(swapOut, "prepareSwapPair", body(swapOut, swapIn, "NOT_A_MODE", null));
    // The instance the request is addressed to has to be one of the two it would change.
    assertBadRequest(swapIn, "prepareSwapPair",
        body(swapOut, "someOtherInstance_1", "COORDINATED", null));
    // Half an expectation cannot be enforced, so it is refused rather than partly applied.
    assertBadRequest(swapOut, "prepareSwapPair",
        body(swapOut, swapIn, "COORDINATED", "\"expectedSwapInConfigVersion\":2"));
    Assert.assertEquals(instanceConfig(swapIn).getDomainAsMap().get(logicalIdKey()), "restSpareD");
  }

  @Test
  public void testExistingSwapCommandsKeepTheirResponseShape() throws Exception {
    String swapOut = addInstance("pairSwapRestCompatOut_12008", "restSlotE", "swap-out-host");
    addInstance("pairSwapRestCompatIn_12009", "restSpareE", "swap-in-host");

    Response response =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=canCompleteSwap").format(
            CLUSTER_NAME, swapOut).post(this, EMPTY_BODY);

    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Map<String, Object> body = MAPPER.readValue(response.readEntity(String.class), Map.class);
    Assert.assertEquals(body.keySet().size(), 2, body.toString());
    Assert.assertTrue(body.containsKey("successful"), body.toString());
    Assert.assertTrue(body.containsKey("blockers"), body.toString());
  }

  private Map<String, Object> post(String pathInstance, String command, boolean force,
      String requestBody) throws Exception {
    Response response = new JerseyUriRequestBuilder(
        "clusters/{}/instances/{}?command=" + command + "&force=" + force).format(CLUSTER_NAME,
        pathInstance).post(this, Entity.entity(requestBody, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    return MAPPER.readValue(response.readEntity(String.class), Map.class);
  }

  private void assertBadRequest(String pathInstance, String command, String requestBody) {
    Response response =
        new JerseyUriRequestBuilder("clusters/{}/instances/{}?command=" + command).format(
                CLUSTER_NAME, pathInstance)
            .expectedReturnStatusCode(Response.Status.BAD_REQUEST.getStatusCode())
            .post(this, Entity.entity(requestBody, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
  }

  private static String body(String swapOut, String swapIn, String swapMode, String extraFields) {
    StringBuilder builder = new StringBuilder("{\"swapOutInstanceName\":\"").append(swapOut)
        .append("\",\"swapInInstanceName\":\"").append(swapIn).append("\"");
    if (swapMode != null) {
      builder.append(",\"swapMode\":\"").append(swapMode).append("\"");
    }
    if (extraFields != null) {
      builder.append(",").append(extraFields);
    }
    return builder.append("}").toString();
  }

  private String logicalIdKey() {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    return ClusterTopologyConfig.createFromClusterConfig(clusterConfig).getEndNodeType();
  }

  private String faultZoneKey() {
    return _configAccessor.getClusterConfig(CLUSTER_NAME).getFaultZoneType();
  }

  private String addInstance(String instanceName, String logicalId, String host) throws Exception {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    Entity<String> entity =
        Entity.entity(MAPPER.writeValueAsString(instanceConfig.getRecord()),
            MediaType.APPLICATION_JSON_TYPE);
    new JerseyUriRequestBuilder("clusters/{}/instances/{}").format(CLUSTER_NAME, instanceName)
        .put(this, entity);

    InstanceConfig stored = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
    Map<String, String> domain = new HashMap<>(stored.getDomainAsMap());
    domain.put(logicalIdKey(), logicalId);
    domain.put(HOST_KEY, host);
    // Every instance here is placed in one fault zone, which is what a coordinated swap between
    // any two of them requires.
    domain.put(faultZoneKey(), FAULT_ZONE);
    stored.setDomain(ConfigStringUtil.concatenateMapping(domain));
    _configAccessor.setInstanceConfig(CLUSTER_NAME, instanceName, stored);
    return instanceName;
  }

  private void setInstanceOperationDirectly(String instanceName,
      InstanceConstants.InstanceOperation operation) {
    InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
    instanceConfig.setInstanceOperation(operation);
    _configAccessor.setInstanceConfig(CLUSTER_NAME, instanceName, instanceConfig);
  }

  private InstanceConfig instanceConfig(String instanceName) {
    return _configAccessor.getInstanceConfig(CLUSTER_NAME, instanceName);
  }
}
