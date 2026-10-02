package org.apache.helix.api.exceptions;

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

import org.apache.helix.HelixException;

/**
 * Thrown when a connected HelixManager has temporarily lost its ZooKeeper connection and did not
 * get it back within the wait timeout. Unlike a closed manager, the connection may still recover
 * on the same session, so callers can treat this as transient.
 */
public class HelixManagerNotConnectedException extends HelixException {
  private static final long serialVersionUID = 4630954717062253541L;

  public HelixManagerNotConnectedException(String message) {
    super(message);
  }
}
