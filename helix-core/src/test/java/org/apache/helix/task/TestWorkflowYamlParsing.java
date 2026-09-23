package org.apache.helix.task;

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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.error.YAMLException;


public class TestWorkflowYamlParsing {
  private static final String WORKFLOW =
      "name: workflow\njobs:\n  - name: job\n    command: Dummy\n"
          + "    targetResource: database\n    ignoreDependentJobFailure: true\n";

  @DataProvider
  public Object[][] legacyExternalViewFlags() {
    return new Object[][] {
        {false, null}, {false, "true"}, {false, "false"},
        {true, null}, {true, "true"}, {true, "false"}
    };
  }

  @Test(dataProvider = "legacyExternalViewFlags")
  public void testLegacyJobExternalViewYamlIgnored(boolean targeted, String legacyFlag)
      throws Exception {
    String baseline = targeted ? WORKFLOW
        : WORKFLOW.replace("targetResource: database", "numberOfTasks: 1");
    String yaml = legacyFlag == null ? baseline
        : baseline + "    disableExternalView: " + legacyFlag + "\n";

    Workflow expected = Workflow.parse(baseline);
    Workflow actual = Workflow.parse(yaml);

    Assert.assertEquals(actual.getJobConfigs(), expected.getJobConfigs());
    Assert.assertFalse(
        actual.getJobConfigs().get("workflow_job").containsKey("DisableExternalView"));
    Assert.assertEquals(actual.getWorkflowConfig().getJobDag().getAllNodes(),
        expected.getWorkflowConfig().getJobDag().getAllNodes());
    if (!targeted) {
      Assert.assertEquals(actual.getTaskConfigs().get("workflow_job").size(), 1);
    }
  }

  @DataProvider
  public Object[][] unknownYamlProperties() {
    return new Object[][] {
        {WORKFLOW + "    disableExternalViews: true\n", "disableExternalViews"},
        {"disableExternalView: true\n" + WORKFLOW, "disableExternalView"},
        {WORKFLOW + "    tasks:\n      - command: Dummy\n        disableExternalView: true\n",
            "disableExternalView"}
    };
  }

  @Test(dataProvider = "unknownYamlProperties")
  public void testUnknownPropertiesRemainRejected(String yaml, String property) throws Exception {
    try {
      Workflow.parse(yaml);
      Assert.fail("Unknown property should be rejected: " + property);
    } catch (YAMLException error) {
      Assert.assertTrue(error.getMessage().contains(property), error.getMessage());
    }
  }

  @Test(expectedExceptions = YAMLException.class)
  public void testGlobalTagsRemainRejected() throws Exception {
    Workflow.parse("!!org.apache.helix.task.beans.WorkflowBean\n" + WORKFLOW);
  }
}
