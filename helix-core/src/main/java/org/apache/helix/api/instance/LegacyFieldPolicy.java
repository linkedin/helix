package org.apache.helix.api.instance;

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
 * How a checked change is allowed to treat the deprecated instance fields, which predate the
 * per source operation record and are still honoured by the rest of the system.
 */
public enum LegacyFieldPolicy {
  /**
   * Only relax deprecated state that the caller's own source can be shown to have written,
   * and never overwrite a deprecated annotation belonging to another writer. When the change
   * would have to relax state that cannot be attributed to the caller, nothing is written and
   * the outcome is a conflict.
   *
   * <p>Ownership is evidence based, not a claim: the deprecated disable fields are written
   * together with the operation that caused them, so a caller's entry owns them when it is
   * the only recorded operation whose timestamp and reason match what the deprecated fields
   * carry. Anything ambiguous counts as not owned. A source value on its own proves nothing,
   * because every writer can use any source.
   */
  OWNED_ONLY,
  /**
   * Write the deprecated fields exactly the way the unchecked setter does, including clearing
   * a disable written by somebody else. Use this only where the caller is the authority for
   * the instance, for example an operator driven override, because it can revoke another
   * writer's disable.
   */
  ALLOW_OVERRIDE
}
