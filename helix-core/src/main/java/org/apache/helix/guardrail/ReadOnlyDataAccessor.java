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

package org.apache.helix.guardrail;

import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;

/**
 * A narrow, read-only view of cluster state handed to {@link GuardrailRule}s.
 * <p>
 * Guard rail rules are contractually pure and read-only (see {@link GuardrailRule}). Handing them a
 * full {@link HelixDataAccessor} would also expose {@code setProperty} / {@code updateProperty} /
 * {@code removeProperty} / {@code createChildren}, letting a "validation" rule mutate the cluster --
 * a footgun the contract forbids but the type would otherwise permit. This interface exposes only
 * the read subset of {@link HelixDataAccessor} that rules need, enforcing the read-only contract at
 * compile time rather than by convention.
 */
public interface ReadOnlyDataAccessor {
  /** @see HelixDataAccessor#getProperty(PropertyKey) */
  <T extends HelixProperty> T getProperty(PropertyKey key);

  /** @see HelixDataAccessor#getChildNames(PropertyKey) */
  List<String> getChildNames(PropertyKey key);

  /** @see HelixDataAccessor#getChildValues(PropertyKey, boolean) */
  <T extends HelixProperty> List<T> getChildValues(PropertyKey key, boolean throwException);

  /** @see HelixDataAccessor#keyBuilder() */
  PropertyKey.Builder keyBuilder();

  /**
   * Wraps a full {@link HelixDataAccessor}, exposing only its read methods to guard rail rules. All
   * mutating methods of the delegate are intentionally left inaccessible.
   *
   * @param delegate the accessor to read cluster state through
   * @return a read-only view backed by {@code delegate}
   */
  static ReadOnlyDataAccessor of(HelixDataAccessor delegate) {
    return new ReadOnlyDataAccessor() {
      @Override
      public <T extends HelixProperty> T getProperty(PropertyKey key) {
        return delegate.getProperty(key);
      }

      @Override
      public List<String> getChildNames(PropertyKey key) {
        return delegate.getChildNames(key);
      }

      @Override
      public <T extends HelixProperty> List<T> getChildValues(PropertyKey key,
          boolean throwException) {
        return delegate.getChildValues(key, throwException);
      }

      @Override
      public PropertyKey.Builder keyBuilder() {
        return delegate.keyBuilder();
      }
    };
  }
}
