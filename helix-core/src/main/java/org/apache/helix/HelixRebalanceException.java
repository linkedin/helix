package org.apache.helix;

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

/**
 * Exception thrown by Helix due to rebalance failures.
 */
public class HelixRebalanceException extends Exception {
  // TODO: Adding static description or other necessary fields into the enum instances for
  // TODO: supporting the rebalance monitor to understand the exception.
  public enum Type {
    INVALID_CLUSTER_STATUS,
    INVALID_REBALANCER_STATUS,
    FAILED_TO_CALCULATE,
    INVALID_INPUT,
    UNKNOWN_FAILURE
  }

  /**
   * Fine-grained classification of a rebalance failure. Independent of {@link Type}: callers
   * surfacing alerts route on category, while internal control-flow (e.g. fallback decisions)
   * still keys on {@link Type}.
   *
   * Each category carries an {@code isCustomerActionable} flag indicating whether the failure
   * stems from cluster/resource configuration the customer controls (true) or from internal
   * Helix infrastructure such as the metadata store or algorithm engine (false).
   */
  public enum FailureCategory {
    // Customer-actionable: customer must adjust cluster/resource configuration.
    CAPACITY_DEFICIT(true),
    NO_CANDIDATE_NODE(true),
    INVALID_RESOURCE_CONFIG(true),
    INVALID_CLUSTER_CONFIG(true),

    // Helix-team-owned: investigate the controller, metadata store, or algorithm.
    METADATA_STORE_IO(false),
    ALGORITHM_INTERNAL(false),
    ASYNC_EXECUTION(false),
    UNKNOWN(false);

    private final boolean _customerActionable;

    FailureCategory(boolean customerActionable) {
      _customerActionable = customerActionable;
    }

    public boolean isCustomerActionable() {
      return _customerActionable;
    }
  }

  private final Type _type;
  private final FailureCategory _category;

  public HelixRebalanceException(String message, Type type, Throwable cause) {
    this(message, type, FailureCategory.UNKNOWN, cause);
  }

  public HelixRebalanceException(String message, Type type) {
    this(message, type, FailureCategory.UNKNOWN);
  }

  public HelixRebalanceException(String message, Type type, FailureCategory category) {
    super(buildMessage(message, type, category));
    _type = type;
    _category = category;
  }

  public HelixRebalanceException(String message, Type type, FailureCategory category,
      Throwable cause) {
    super(buildMessage(message, type, category), cause);
    _type = type;
    _category = category;
  }

  public Type getFailureType() {
    return _type;
  }

  public FailureCategory getFailureCategory() {
    return _category;
  }

  public boolean isCustomerActionable() {
    return _category.isCustomerActionable();
  }

  private static String buildMessage(String message, Type type, FailureCategory category) {
    // Preserve the historical "<msg> Failure Type: X" format when no category is supplied.
    if (category == FailureCategory.UNKNOWN) {
      return String.format("%s Failure Type: %s", message, type.name());
    }
    return String.format("%s Failure Type: %s Category: %s", message, type.name(), category.name());
  }
}
