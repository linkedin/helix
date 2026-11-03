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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestPropertyPathBuilder {
  @Test
  public void testGetPath() {
    String actual;
    actual = PropertyPathBuilder.idealState("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/IDEALSTATES");
    actual = PropertyPathBuilder.idealState("test_cluster", "resource");
    AssertJUnit.assertEquals(actual, "/test_cluster/IDEALSTATES/resource");

    actual = PropertyPathBuilder.instance("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1");

    actual = PropertyPathBuilder.instanceCurrentState("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES");
    actual = PropertyPathBuilder.instanceCurrentState("test_cluster", "instanceName1", "sessionId");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES/sessionId");

    actual = PropertyPathBuilder.instanceTaskCurrentState("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/TASKCURRENTSTATES");
    actual =
        PropertyPathBuilder.instanceTaskCurrentState("test_cluster", "instanceName1", "sessionId");
    AssertJUnit
        .assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/TASKCURRENTSTATES/sessionId");

    actual = PropertyPathBuilder.instanceCustomizedState("test_cluster", "instanceName1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CUSTOMIZEDSTATES");
    actual = PropertyPathBuilder.instanceCustomizedState("test_cluster", "instanceName1", "customizedState1");
    AssertJUnit.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CUSTOMIZEDSTATES/customizedState1");

    actual = PropertyPathBuilder.controller("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/CONTROLLER");
    actual = PropertyPathBuilder.controllerMessage("test_cluster");
    AssertJUnit.assertEquals(actual, "/test_cluster/CONTROLLER/MESSAGES");

    actual = PropertyPathBuilder.clusterStatus("test_cluster");
    Assert.assertEquals(actual, "/test_cluster/STATUS/CLUSTER/test_cluster");
  }

  /**
   * getPath now joins precompiled literal segments instead of running a regex Matcher and a chain
   * of String.replace calls. This sweeps every registered template and asserts the compiled form
   * produces byte-for-byte the same path as the legacy Matcher + replace implementation.
   */
  @Test
  public void testGetPathMatchesLegacyForAllTemplates() {
    String clusterName = "cluster";
    for (Map.Entry<PropertyType, Map<Integer, String>> typeEntry : PropertyPathBuilder.templateMap
        .entrySet()) {
      PropertyType type = typeEntry.getKey();
      for (Integer numKeys : typeEntry.getValue().keySet()) {
        // numKeys is the number of path segments; clusterName is one of them, so pass numKeys - 1
        // keys. Use distinct values so a repeated token (STATUS) exposes any positional divergence.
        String[] keys = new String[numKeys - 1];
        for (int i = 0; i < keys.length; i++) {
          keys[i] = "key" + i;
        }
        String expected = legacyGetPath(type, clusterName, keys);
        String actual = PropertyPathBuilder.getPath(type, clusterName, keys);
        Assert.assertEquals(actual, expected,
            "Mismatch for type " + type + " with " + numKeys + " segments");
      }
    }
  }

  @Test
  public void testGetPathRepeatedTokenUsesFirstValue() {
    // STATUS v2 template "/{clusterName}/STATUS/{clusterName}" repeats a token. The legacy logic
    // replaced every occurrence with clusterName and discarded the extra key; the compiled form
    // must do the same even when a different key is supplied.
    Assert.assertEquals(PropertyPathBuilder.getPath(PropertyType.STATUS, "cluster", "ignoredKey"),
        "/cluster/STATUS/cluster");
  }

  @Test
  public void testGetPathReturnsNullForUnknownKeyCount() {
    // No CURRENTSTATES template is registered for a single path segment (keys.length + 1 == 1).
    Assert.assertNull(PropertyPathBuilder.getPath(PropertyType.CURRENTSTATES, "cluster"));
  }

  @Test
  public void testGetPathReturnsNullForNullCluster() {
    Assert.assertNull(PropertyPathBuilder.getPath(PropertyType.IDEALSTATES, null, "resource"));
  }

  // Faithful re-implementation of the pre-optimization getPath body (Matcher + String.replace
  // chain), used as the oracle for the equivalence sweep above.
  private static String legacyGetPath(PropertyType type, String clusterName, String... keys) {
    if (clusterName == null) {
      return null;
    }
    if (keys == null) {
      keys = new String[] {};
    }
    String template = null;
    if (PropertyPathBuilder.templateMap.containsKey(type)) {
      template = PropertyPathBuilder.templateMap.get(type).get(keys.length + 1);
    }
    String result = null;
    if (template != null) {
      result = template;
      Matcher matcher = Pattern.compile("(\\{.+?\\})").matcher(template);
      int count = 0;
      while (matcher.find()) {
        count = count + 1;
        String var = matcher.group();
        if (count == 1) {
          result = result.replace(var, clusterName);
        } else {
          result = result.replace(var, keys[count - 2]);
        }
      }
    }
    return result;
  }
}
