package org.apache.helix.rest.acl;

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

import java.util.Collection;
import java.util.List;
import org.apache.zookeeper.data.ACL;

/**
 * Compatibility facade for the parser shared with non-REST Helix clients.
 * @see org.apache.helix.util.ClusterAclParser
 */
public final class ClusterAclParser {
  public static final String ACL_FORMAT_HINT = org.apache.helix.util.ClusterAclParser.ACL_FORMAT_HINT;

  private ClusterAclParser() {
  }

  public static List<ACL> parse(Collection<String> entries) {
    return org.apache.helix.util.ClusterAclParser.parse(entries);
  }

  public static List<ACL> withWorldRead(List<ACL> acl) {
    return org.apache.helix.util.ClusterAclParser.withWorldRead(acl);
  }
}
