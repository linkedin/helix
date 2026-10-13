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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;


/**
 * Parses ZooKeeper ACL entries written as {@code scheme:id:permissions}.
 * <p>
 * The scheme ends at the first colon and the permissions start after the last colon, so ids that
 * contain colons (for example digest ids) or commas (for example x509 subject DNs) are kept
 * intact. Permissions are any combination of {@code c} (create), {@code d} (delete), {@code r}
 * (read), {@code w} (write) and {@code a} (admin), or {@code all}. Examples:
 * <pre>
 * x509:CN=my-service,OU=my-team,O=my-org,C=US:cdrwa
 * world:anyone:r
 * </pre>
 */
public final class ClusterAclParser {
  private ClusterAclParser() {
  }

  /**
   * @return the parsed ACL list, or an empty list when {@code entries} is null or empty
   * @throws IllegalArgumentException if any entry is malformed
   */
  public static List<ACL> parse(Collection<String> entries) {
    if (entries == null || entries.isEmpty()) {
      return Collections.emptyList();
    }
    List<ACL> acl = new ArrayList<>(entries.size());
    for (String entry : entries) {
      acl.add(parseEntry(entry));
    }
    return Collections.unmodifiableList(acl);
  }

  private static ACL parseEntry(String entry) {
    if (entry == null) {
      throw new IllegalArgumentException("ACL entry must not be null");
    }
    int schemeEnd = entry.indexOf(':');
    int permsStart = entry.lastIndexOf(':');
    if (schemeEnd <= 0 || permsStart <= schemeEnd + 1 || permsStart == entry.length() - 1) {
      throw new IllegalArgumentException(
          "Invalid ACL entry, expected scheme:id:permissions with non-empty parts");
    }
    String scheme = entry.substring(0, schemeEnd);
    String id = entry.substring(schemeEnd + 1, permsStart);
    return new ACL(parsePermissions(entry.substring(permsStart + 1)), new Id(scheme, id));
  }

  private static int parsePermissions(String permissions) {
    if (permissions.equalsIgnoreCase("all")) {
      return ZooDefs.Perms.ALL;
    }
    int perms = 0;
    for (char c : permissions.toLowerCase().toCharArray()) {
      switch (c) {
        case 'c':
          perms |= ZooDefs.Perms.CREATE;
          break;
        case 'd':
          perms |= ZooDefs.Perms.DELETE;
          break;
        case 'r':
          perms |= ZooDefs.Perms.READ;
          break;
        case 'w':
          perms |= ZooDefs.Perms.WRITE;
          break;
        case 'a':
          perms |= ZooDefs.Perms.ADMIN;
          break;
        default:
          throw new IllegalArgumentException(
              "Invalid ACL permissions \"" + permissions + "\", expected letters from cdrwa or all");
      }
    }
    return perms;
  }
}
