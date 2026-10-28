package org.apache.helix.rest.server;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.guardrail.rules.InstanceDisableMinActiveReplicaGuardrailRule;
import org.apache.helix.rest.acl.NoopAclRegister;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.authValidator.NoopAuthValidator;
import org.apache.helix.rest.server.guardrail.GuardrailValidationEvent;
import org.apache.helix.rest.server.guardrail.GuardrailValidationListener;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * End-to-end test that a {@link GuardrailValidationListener} registered through the
 * {@link HelixRestServer} constructor is actually notified when a guard rail runs during a real
 * request. This exercises the full wiring that the unit tests cannot: the new constructor, the
 * {@code GUARDRAIL_VALIDATION_LISTENERS} Jersey context property, and
 * {@code AbstractHelixResource#notifyGuardrailValidationListeners}. If any of that plumbing breaks,
 * the resource silently reads a {@code null} listener list and returns, so only an integration test
 * through a live server can catch the regression.
 */
public class TestGuardrailValidationListenerE2E extends AbstractTestClass {

  /** Captures every event it is notified of; thread-safe because helix-rest serves concurrently. */
  private static final class RecordingGuardrailValidationListener
      implements GuardrailValidationListener {
    private final List<GuardrailValidationEvent> _events =
        Collections.synchronizedList(new ArrayList<>());

    @Override
    public void onValidation(GuardrailValidationEvent event) {
      _events.add(event);
    }

    List<GuardrailValidationEvent> getEvents() {
      return _events;
    }
  }

  /** Always throws, proving a faulty listener can never fail the request or block later listeners. */
  private static final class ThrowingGuardrailValidationListener
      implements GuardrailValidationListener {
    private final AtomicInteger _invocations = new AtomicInteger();

    @Override
    public void onValidation(GuardrailValidationEvent event) {
      _invocations.incrementAndGet();
      throw new RuntimeException("intentional listener failure");
    }

    int getInvocations() {
      return _invocations.get();
    }
  }

  @Test
  public void testListenerNotifiedForDisableDryRunThroughLiveServer() throws Exception {
    String clusterName = "TestGuardrailListenerCluster";
    String instanceName = "localhost_guardrail_12918";
    _gSetupTool.addCluster(clusterName, true);
    _gSetupTool.addInstanceToCluster(clusterName, instanceName);

    RecordingGuardrailValidationListener recordingListener =
        new RecordingGuardrailValidationListener();
    ThrowingGuardrailValidationListener throwingListener =
        new ThrowingGuardrailValidationListener();

    int newPort = getBaseUri().getPort() + 7;
    String mockBaseUri =
        HttpConstants.HTTP_PROTOCOL_PREFIX + getBaseUri().getHost() + ":" + newPort;

    List<HelixRestNamespace> namespaces = new ArrayList<>();
    namespaces.add(new HelixRestNamespace(HelixRestNamespace.DEFAULT_NAMESPACE_NAME,
        HelixRestNamespace.HelixMetadataStoreType.ZOOKEEPER, ZK_ADDR, true));

    // The new constructor wires both the auth seam and the guard rail listeners -- exactly the
    // combination an embedded helix-rest needs. The throwing listener is registered first so a
    // single notify failure cannot prevent the recording listener from seeing the event.
    HelixRestServer server = new HelixRestServer(namespaces, newPort, getBaseUri().getPath(),
        Collections.emptyList(), new NoopAuthValidator(), new NoopAuthValidator(),
        new NoopAclRegister(), Arrays.asList(throwingListener, recordingListener));
    server.start();

    CloseableHttpClient httpClient = HttpClients.createDefault();
    try {
      HttpPost request = new HttpPost(mockBaseUri + "/clusters/" + clusterName + "/instances/"
          + instanceName + "?command=disable&dryRun=true");
      request.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));
      HttpResponse response = httpClient.execute(request);

      // A dryRun always returns the verdict with 200, and a throwing listener must not change that.
      Assert.assertEquals(response.getStatusLine().getStatusCode(),
          Response.Status.OK.getStatusCode());

      // The listener path ran through the live server: exactly one event for the single disable
      // rule the preflight evaluates.
      List<GuardrailValidationEvent> events = recordingListener.getEvents();
      Assert.assertEquals(events.size(), 1, "recording listener should be notified exactly once");
      GuardrailValidationEvent event = events.get(0);
      Assert.assertTrue(event.isDryRun());
      Assert.assertFalse(event.isForce());
      Assert.assertEquals(event.getRuleEvaluations().size(), 1);
      GuardrailValidationEvent.RuleEvaluation evaluation = event.getRuleEvaluations().get(0);
      Assert.assertEquals(evaluation.getRuleId(),
          InstanceDisableMinActiveReplicaGuardrailRule.RULE_ID);
      // The empty cluster trips no rule, so a dryRun pass lands in DRY_RUN_PASSED (never PASSED).
      Assert.assertEquals(evaluation.getOutcome(),
          GuardrailValidationEvent.Outcome.DRY_RUN_PASSED);
      Assert.assertTrue(evaluation.getDurationNanos() >= 0, "per-rule latency should be recorded");

      // The faulty listener was invoked, but its exception was swallowed (the request still got 200
      // and the recording listener still ran).
      Assert.assertEquals(throwingListener.getInvocations(), 1);
    } finally {
      httpClient.close();
      server.shutdown();
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }
}
