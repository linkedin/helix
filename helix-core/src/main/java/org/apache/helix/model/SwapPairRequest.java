package org.apache.helix.model;

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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.helix.constants.InstanceConstants;

/**
 * A request to prepare or complete a swap between one named swap-out instance and one named
 * swap-in instance.
 * <p>
 * The pair is always given explicitly. Nothing in the request is discovered from cluster state, so
 * a prepare or complete can never act on a peer other than the one the caller named. The optional
 * expected identities are checked against the config metadata observed before the write. They
 * do not atomically fence config replacement; see
 * {@link org.apache.helix.HelixAdmin#completeSwapPair(String, SwapPairRequest)} for the limits.
 * <p>
 * {@code swapMode} selects how the swap-in is prepared, and the two modes are deliberately
 * different:
 * <ul>
 *   <li>{@link SwapMode#COORDINATED} moves only the logical id field of the swap-in domain onto the
 *       swap-out's value and marks the swap-in with
 *       {@link InstanceConstants.InstanceOperation#SWAP_IN}, so the controller mirrors replicas onto
 *       it before the swap is completed. Every other domain field of the swap-in is left alone.
 *       An existing SWAP_IN is accepted only as a replay for the same logical id.</li>
 *   <li>{@link SwapMode#DIRECT} copies the whole swap-out domain onto the swap-in, except for the
 *       keys named in {@link #getPreservedSwapInDomainKeys()}, which keep the swap-in's own values.
 *       It sets no instance operation at all, so the swap-in stays outside the assignable set until
 *       completion transfers the config. This mode is for placement schemes where occupying the
 *       same topology slot is what transfers the replicas, and it is the mode that has to carry
 *       derived topology fields such as a virtual group assignment across to the swap-in.</li>
 * </ul>
 * <p>
 * Instances of this class are immutable, built through {@link Builder}.
 */
public class SwapPairRequest {

  /**
   * How the swap-in instance is prepared, and therefore what the completion step expects to find.
   */
  public enum SwapMode {
    /**
     * Logical id is aligned and the swap-in is marked SWAP_IN so replicas are mirrored onto it.
     */
    COORDINATED,
    /**
     * The whole topology slot is transferred to the swap-in and no instance operation is set.
     */
    DIRECT
  }

  private final String _swapOutInstanceName;
  private final String _swapInInstanceName;
  private final SwapMode _swapMode;
  private final InstanceConfigIdentity _expectedSwapOutIdentity;
  private final InstanceConfigIdentity _expectedSwapInIdentity;
  private final Set<String> _preservedSwapInDomainKeys;
  private final boolean _forceComplete;
  private final String _reason;
  private final InstanceConstants.InstanceOperationSource _operationSource;

  private SwapPairRequest(Builder builder) {
    _swapOutInstanceName = builder._swapOutInstanceName;
    _swapInInstanceName = builder._swapInInstanceName;
    _swapMode = builder._swapMode;
    _expectedSwapOutIdentity = builder._expectedSwapOutIdentity;
    _expectedSwapInIdentity = builder._expectedSwapInIdentity;
    _preservedSwapInDomainKeys =
        Collections.unmodifiableSet(new LinkedHashSet<>(builder._preservedSwapInDomainKeys));
    _forceComplete = builder._forceComplete;
    _reason = builder._reason;
    _operationSource = builder._operationSource;
  }

  public String getSwapOutInstanceName() {
    return _swapOutInstanceName;
  }

  public String getSwapInInstanceName() {
    return _swapInInstanceName;
  }

  public SwapMode getSwapMode() {
    return _swapMode;
  }

  /**
   * @return the identity the caller expects the swap-out config to still have, or null when the
   *         caller does not assert it.
   */
  public InstanceConfigIdentity getExpectedSwapOutIdentity() {
    return _expectedSwapOutIdentity;
  }

  /**
   * @return the identity the caller expects the swap-in config to still have, or null when the
   *         caller does not assert it.
   */
  public InstanceConfigIdentity getExpectedSwapInIdentity() {
    return _expectedSwapInIdentity;
  }

  /**
   * @return the domain keys whose swap-in values survive a {@link SwapMode#DIRECT} preparation.
   *         The topology's logical-id key cannot be preserved. Every other domain key is taken
   *         from the swap-out. Unused in
   *         {@link SwapMode#COORDINATED}, which only touches the logical id key.
   */
  public Set<String> getPreservedSwapInDomainKeys() {
    return _preservedSwapInDomainKeys;
  }

  /**
   * @return true when completion should skip the replica readiness checks. Force never skips pair
   *         validation, identity assertions or the conditional write.
   */
  public boolean isForceComplete() {
    return _forceComplete;
  }

  /**
   * @return a human readable reason recorded on the instance operations this request writes, or
   *         null.
   */
  public String getReason() {
    return _reason;
  }

  /**
   * @return the source recorded on the instance operations this request writes. It is descriptive
   *         metadata for operators only. It is not authenticated and confers no ownership or
   *         privilege.
   */
  public InstanceConstants.InstanceOperationSource getOperationSource() {
    return _operationSource;
  }

  @Override
  public String toString() {
    return "SwapPairRequest{swapOut=" + _swapOutInstanceName + ", swapIn=" + _swapInInstanceName
        + ", mode=" + _swapMode + ", expectedSwapOutIdentity=" + _expectedSwapOutIdentity
        + ", expectedSwapInIdentity=" + _expectedSwapInIdentity + ", preservedSwapInDomainKeys="
        + _preservedSwapInDomainKeys + ", forceComplete=" + _forceComplete + ", source="
        + _operationSource + "}";
  }

  public static class Builder {
    private final String _swapOutInstanceName;
    private final String _swapInInstanceName;
    private SwapMode _swapMode = SwapMode.COORDINATED;
    private InstanceConfigIdentity _expectedSwapOutIdentity;
    private InstanceConfigIdentity _expectedSwapInIdentity;
    private Set<String> _preservedSwapInDomainKeys = Collections.emptySet();
    private boolean _forceComplete;
    private String _reason;
    private InstanceConstants.InstanceOperationSource _operationSource =
        InstanceConstants.InstanceOperationSource.AUTOMATION;

    /**
     * @param swapOutInstanceName the instance being replaced
     * @param swapInInstanceName  the instance replacing it
     */
    public Builder(String swapOutInstanceName, String swapInInstanceName) {
      _swapOutInstanceName = swapOutInstanceName;
      _swapInInstanceName = swapInInstanceName;
    }

    public Builder setSwapMode(SwapMode swapMode) {
      _swapMode = swapMode;
      return this;
    }

    public Builder setExpectedSwapOutIdentity(InstanceConfigIdentity expectedSwapOutIdentity) {
      _expectedSwapOutIdentity = expectedSwapOutIdentity;
      return this;
    }

    public Builder setExpectedSwapInIdentity(InstanceConfigIdentity expectedSwapInIdentity) {
      _expectedSwapInIdentity = expectedSwapInIdentity;
      return this;
    }

    public Builder setPreservedSwapInDomainKeys(Set<String> preservedSwapInDomainKeys) {
      _preservedSwapInDomainKeys = preservedSwapInDomainKeys != null ? preservedSwapInDomainKeys
          : Collections.emptySet();
      return this;
    }

    public Builder setForceComplete(boolean forceComplete) {
      _forceComplete = forceComplete;
      return this;
    }

    public Builder setReason(String reason) {
      _reason = reason;
      return this;
    }

    public Builder setOperationSource(
        InstanceConstants.InstanceOperationSource operationSource) {
      _operationSource = operationSource != null ? operationSource
          : InstanceConstants.InstanceOperationSource.AUTOMATION;
      return this;
    }

    public SwapPairRequest build() {
      return new SwapPairRequest(this);
    }
  }
}
