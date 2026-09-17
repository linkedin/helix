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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The config identities of both sides of a swap pair, read together.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SwapPairIdentities {
  private final InstanceConfigIdentity _swapOutIdentity;
  private final InstanceConfigIdentity _swapInIdentity;

  @JsonCreator
  public SwapPairIdentities(@JsonProperty("swapOut") InstanceConfigIdentity swapOutIdentity,
      @JsonProperty("swapIn") InstanceConfigIdentity swapInIdentity) {
    _swapOutIdentity = swapOutIdentity;
    _swapInIdentity = swapInIdentity;
  }

  @JsonProperty("swapOut")
  public InstanceConfigIdentity getSwapOutIdentity() {
    return _swapOutIdentity;
  }

  @JsonProperty("swapIn")
  public InstanceConfigIdentity getSwapInIdentity() {
    return _swapInIdentity;
  }

  @JsonIgnore
  public boolean isComplete() {
    return _swapOutIdentity != null && _swapOutIdentity.isFullySpecified()
        && _swapInIdentity != null && _swapInIdentity.isFullySpecified();
  }

  @Override
  public String toString() {
    return "SwapPairIdentities{swapOut=" + _swapOutIdentity + ", swapIn=" + _swapInIdentity + "}";
  }
}
