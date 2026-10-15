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

package org.apache.helix.guardrail.rules;

import java.util.Objects;

import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.guardrail.GuardrailContext;
import org.apache.helix.guardrail.GuardrailRule;
import org.apache.helix.guardrail.ReadOnlyDataAccessor;
import org.apache.helix.guardrail.ValidationResult;
import org.apache.helix.guardrail.Violation;
import org.apache.helix.model.IdealState;
import org.apache.helix.util.HelixUtil;


/**
 * Blocks writing a {@code USER_DEFINED} resource ideal state whose {@code REBALANCER_CLASS_NAME} is
 * unset, cannot be loaded, or does not name a {@link Rebalancer}.
 * <p>
 * For a {@code USER_DEFINED} resource the controller does not supply a built-in rebalancer; it loads
 * the class named by {@code REBALANCER_CLASS_NAME} and casts it to {@link Rebalancer}
 * (see {@code BestPossibleStateCalcStage#getCustomizedRebalancer}). If that name is missing, names a
 * class absent from the classpath, or names a class that is not a {@code Rebalancer}, the controller
 * resolves a {@code null} rebalancer, logs the failure, and computes no assignment for the resource:
 * the ideal state is accepted into ZooKeeper but the resource can never be brought online, and the
 * failure surfaces only later &mdash; on the controller's next pipeline run &mdash; rather than as an
 * error on the write itself. Neither the raw ideal-state update path
 * ({@code setResourceIdealState} / {@code updateIdealState}) nor {@link IdealState#isValid()}
 * validates the rebalancer class, so an unresolvable {@code USER_DEFINED} resource can reach
 * ZooKeeper through those endpoints. This rule closes that gap by applying the same resolution check
 * at pre-write time, turning a silent, deferred failure into an actionable error on the write.
 * <p>
 * The rule concerns only {@code USER_DEFINED} resources. Other rebalance modes
 * ({@code FULL_AUTO}, {@code SEMI_AUTO}, {@code CUSTOMIZED}, {@code TASK}) either use a built-in
 * rebalancer or are managed by a framework that owns the class name, so their class resolution is
 * outside this rule's concern and is certified feasible.
 * <p>
 * Loadability is checked by resolving the class (via {@link HelixUtil#loadClass}) and confirming it
 * is assignable to {@link Rebalancer}; the class is deliberately <em>not</em> instantiated, so no
 * user constructor runs at validation time. This presumes the helix-rest process shares the
 * rebalancer class on its classpath (as it already does for user-supplied maintenance operation
 * classes); the {@code force=true} override on the write endpoint remains available for the rare case
 * where a class is present on the controller but not the REST host.
 * <p>
 * The rule is a <em>delta</em> guard, consistent with the other ideal-state rules: it fires only
 * when the write <em>introduces or changes</em> the resource into an unresolvable {@code USER_DEFINED}
 * configuration. A resource that already references the same unresolvable class &mdash; e.g. because
 * the class was removed after the resource was created &mdash; is grandfathered, so an edit to an
 * unrelated field of such a resource is not blocked; only a switch to {@code USER_DEFINED}, or an
 * edit that points an already-{@code USER_DEFINED} resource's {@code REBALANCER_CLASS_NAME} at a
 * (still) unresolvable class, is rejected.
 * <p>
 * The rule is currently wired only into the ideal-state <em>update</em> path
 * ({@code updateResourceIdealState}, {@code command=update}), which operates on an existing resource
 * ({@code updateIdealState} rejects a resource that does not yet exist), so in practice it never sees
 * a brand-new resource: the write it guards is always a modification of an existing ideal state.
 * Extending the same pre-write check to the plain {@code addResource} path, so a resource cannot be
 * <em>created</em> directly in an unresolvable {@code USER_DEFINED} configuration, is a natural
 * follow-up.
 */
public class UserDefinedRebalancerClassGuardrailRule implements GuardrailRule {

  public static final String RULE_ID = "USER_DEFINED_REBALANCER_CLASS_INVALID";

  @Override
  public String getId() {
    return RULE_ID;
  }

  @Override
  public ValidationResult validate(GuardrailContext context) {
    IdealState proposedIdealState = context.getProposedIdealState();
    if (proposedIdealState == null) {
      // Not an ideal-state mutation; there is nothing for this rule to certify.
      return ValidationResult.feasible();
    }

    if (proposedIdealState.getRebalanceMode() != IdealState.RebalanceMode.USER_DEFINED) {
      // Only USER_DEFINED resources resolve their rebalancer from REBALANCER_CLASS_NAME; every other
      // mode uses a built-in or framework-owned rebalancer and is outside this rule's concern.
      return ValidationResult.feasible();
    }

    String proposedClassName = proposedIdealState.getRebalancerClassName();
    String defectDetail = describeDefect(proposedClassName);
    if (defectDetail == null) {
      // REBALANCER_CLASS_NAME resolves to a Rebalancer; the write is safe to certify.
      return ValidationResult.feasible();
    }

    // The proposed configuration is unresolvable. Grandfather a resource that is already USER_DEFINED
    // with the same (unchanged) class name, so an edit to an unrelated field of an already-broken
    // resource is not blocked; only an add, a switch to USER_DEFINED, or a change of the class name to
    // a (still) unresolvable one is rejected.
    String resourceName = proposedIdealState.getResourceName();
    IdealState existingIdealState = readIdealState(context, resourceName);
    if (existingIdealState != null
        && existingIdealState.getRebalanceMode() == IdealState.RebalanceMode.USER_DEFINED
        && Objects.equals(normalize(existingIdealState.getRebalancerClassName()),
            normalize(proposedClassName))) {
      return ValidationResult.feasible();
    }

    return ValidationResult.infeasible(Violation.newBuilder(RULE_ID)
        .resource(resourceName)
        .message(String.format(
            "Writing the ideal state of resource \"%s\" would leave it in USER_DEFINED mode with %s. "
                + "The controller loads the class named by REBALANCER_CLASS_NAME and uses it as the "
                + "resource's rebalancer; when that class cannot be resolved it computes no assignment, "
                + "so the resource would be accepted but could never be brought online. Set "
                + "REBALANCER_CLASS_NAME to a %s implementation available on the classpath, or choose a "
                + "built-in rebalance mode.",
            resourceName, defectDetail, Rebalancer.class.getName()))
        .build());
  }

  /**
   * Returns a human-readable description of why {@code className} does not resolve to a usable
   * rebalancer, or {@code null} if it does. The class is resolved but never instantiated, so no user
   * constructor runs at validation time.
   */
  private static String describeDefect(String className) {
    if (className == null || className.trim().isEmpty()) {
      return "no REBALANCER_CLASS_NAME set";
    }
    Class<?> clazz;
    try {
      // Resolve the class name exactly as the controller does -- BestPossibleStateCalcStage passes
      // getRebalancerClassName() to loadClass as-is. Do NOT trim here: a padded but otherwise valid
      // name (e.g. from a copy/paste or templated JSON) would pass a trimmed check yet still throw
      // ClassNotFoundException on the controller, which is exactly the silent, deferred failure this
      // rule exists to prevent.
      clazz = HelixUtil.loadClass(UserDefinedRebalancerClassGuardrailRule.class, className);
    } catch (ClassNotFoundException | LinkageError e) {
      return String.format("REBALANCER_CLASS_NAME \"%s\", which cannot be loaded", className);
    }
    if (!Rebalancer.class.isAssignableFrom(clazz)) {
      return String.format("REBALANCER_CLASS_NAME \"%s\", which is not a %s", className,
          Rebalancer.class.getName());
    }
    return null;
  }

  private static String normalize(String className) {
    // Compare the raw class names (null-safe) when deciding whether an edit changed
    // REBALANCER_CLASS_NAME. Surrounding whitespace is significant because the controller loads the
    // name as-is, so an edit that only adds or removes it is a real change and must not be
    // grandfathered.
    return className == null ? "" : className;
  }

  private static IdealState readIdealState(GuardrailContext context, String resourceName) {
    if (resourceName == null) {
      return null;
    }
    ReadOnlyDataAccessor dataAccessor = context.getDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    return dataAccessor.getProperty(keyBuilder.idealStates(resourceName));
  }
}
