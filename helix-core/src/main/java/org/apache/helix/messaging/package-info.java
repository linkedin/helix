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
 * Helix message handling classes for intra-cluster communication.
 * 
 * <p>The messaging system allows sending messages to cluster participants based on flexible criteria
 * such as instance name, resource, partition, or replica state. The key classes are:
 * <ul>
 *   <li>{@link org.apache.helix.ClusterMessagingService} - Main API for sending messages</li>
 *   <li>{@link org.apache.helix.Criteria} - Specifies message recipient criteria</li>
 *   <li>{@link org.apache.helix.messaging.CriteriaEvaluator} - Evaluates criteria to find recipients</li>
 * </ul>
 * 
 * <p><b>Performance Note:</b> When using {@link org.apache.helix.Criteria}, configure
 * {@link org.apache.helix.Criteria.DataSource} carefully. Using EXTERNALVIEW with wildcard resource
 * names can cause severe performance degradation at scale. See {@link org.apache.helix.Criteria}
 * for guidance on choosing the right DataSource.
 */
package org.apache.helix.messaging;