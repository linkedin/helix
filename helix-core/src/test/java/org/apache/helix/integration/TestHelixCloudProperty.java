package org.apache.helix.integration;

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

import org.apache.helix.HelixCloudProperty;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestHelixCloudProperty {
  @DataProvider
  public Object[][] legacyCloudIds() {
    return new Object[][]{{null}, {""}, {"legacy-id"}};
  }

  @Test(dataProvider = "legacyCloudIds")
  public void testHelixCloudPropertyAzure(String legacyCloudId) {
    CloudConfig azureCloudConfig =
        new CloudConfig.Builder().setCloudEnabled(true)
            .setCloudProvider(CloudProvider.AZURE).build();
    if (legacyCloudId != null) {
      azureCloudConfig.getRecord().setSimpleField("CLOUD_ID", legacyCloudId);
    }
    ZNRecord original = new ZNRecord(azureCloudConfig.getRecord());

    HelixCloudProperty azureCloudProperty = new HelixCloudProperty(azureCloudConfig);

    Assert.assertEquals(azureCloudConfig.getRecord(), original);
    Assert.assertTrue(azureCloudProperty.getCloudEnabled());
    Assert.assertEquals(azureCloudProperty.getCloudProvider(), CloudProvider.AZURE.name());
    Assert.assertEquals(azureCloudProperty.getCloudInfoSources(), Collections.singletonList(
        "http://169.254.169.254/metadata/instance?api-version=2019-06-04"));
    Assert.assertEquals(azureCloudProperty.getCloudMaxRetry(), 5);
    Assert.assertEquals(azureCloudProperty.getCloudConnectionTimeout(), 5000);
    Assert.assertEquals(azureCloudProperty.getCloudRequestTimeout(), 5000);
    Assert.assertEquals(azureCloudProperty.getCloudInfoProcessorPackage(),
        "org.apache.helix.cloud.azure");
    Assert.assertEquals(azureCloudProperty.getCloudInfoProcessorName(),
        "AzureCloudInstanceInformationProcessor");
    Assert.assertEquals(azureCloudProperty.getCloudInfoProcessorFullyQualifiedClassName(),
        "org.apache.helix.cloud.azure.AzureCloudInstanceInformationProcessor");
  }

  @Test(dataProvider = "legacyCloudIds")
  public void testHelixCloudPropertyCustomizedFullyQualified(String legacyCloudId) {
    CloudConfig customCloudConfig =
        new CloudConfig.Builder().setCloudEnabled(true).setCloudProvider(CloudProvider.CUSTOMIZED)
            .setCloudInfoProcessorPackageName("org.apache.foo.bar")
            .setCloudInfoProcessorName("CustomCloudInstanceInfoProcessor")
            .setCloudInfoSources(Collections.singletonList("https://custom-cloud.com")).build();
    if (legacyCloudId != null) {
      customCloudConfig.getRecord().setSimpleField("CLOUD_ID", legacyCloudId);
    }
    ZNRecord original = new ZNRecord(customCloudConfig.getRecord());

    HelixCloudProperty customCloudProperty = new HelixCloudProperty(customCloudConfig);

    Assert.assertEquals(customCloudConfig.getRecord(), original);
    Assert.assertTrue(customCloudProperty.getCloudEnabled());
    Assert.assertEquals(customCloudProperty.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    Assert.assertEquals(customCloudProperty.getCloudInfoSources(),
        Collections.singletonList("https://custom-cloud.com"));
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorPackage(), "org.apache.foo.bar");
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorName(),
        "CustomCloudInstanceInfoProcessor");
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorFullyQualifiedClassName(),
        "org.apache.foo.bar.CustomCloudInstanceInfoProcessor");
  }

  @Test
  public void testHelixCloudPropertyClassNameOnly() {
    CloudConfig customCloudConfig = new CloudConfig(CloudConfig.CLOUD_CONFIG_KW, true,
        CloudProvider.CUSTOMIZED, Collections.singletonList("https://custom-cloud.com"),
        "CustomCloudInstanceInfoProcessor");

    HelixCloudProperty customCloudProperty = new HelixCloudProperty(customCloudConfig);

    Assert.assertTrue(customCloudProperty.getCloudEnabled());
    Assert.assertEquals(customCloudProperty.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    Assert.assertEquals(customCloudProperty.getCloudInfoSources(),
        Collections.singletonList("https://custom-cloud.com"));
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorPackage(),
        "org.apache.helix.cloud.customized");
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorName(),
        "CustomCloudInstanceInfoProcessor");
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorFullyQualifiedClassName(),
        "org.apache.helix.cloud.customized.CustomCloudInstanceInfoProcessor");
  }

  @Test
  public void testHelixCloudPropertyDoesNotDefaultCustomizedProcessor() {
    ZNRecord record = new ZNRecord(CloudConfig.CLOUD_CONFIG_KW);
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setSimpleField(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER.name(),
        CloudProvider.CUSTOMIZED.name());
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(),
        Collections.singletonList("https://custom-cloud.com"));

    HelixCloudProperty customCloudProperty = new HelixCloudProperty(new CloudConfig(record));

    Assert.assertTrue(customCloudProperty.getCloudEnabled());
    Assert.assertEquals(customCloudProperty.getCloudProvider(), CloudProvider.CUSTOMIZED.name());
    Assert.assertEquals(customCloudProperty.getCloudInfoProcessorPackage(),
        "org.apache.helix.cloud.customized");
    Assert.assertNull(customCloudProperty.getCloudInfoProcessorName());
  }

  @Test
  public void testHelixCloudPropertyDoesNotDefaultMissingProvider() {
    ZNRecord record = new ZNRecord("CloudConfig");
    record.setBooleanField(CloudConfig.CloudConfigProperty.CLOUD_ENABLED.name(), true);
    record.setListField(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE.name(),
        Collections.singletonList("https://custom-cloud.com"));
    CloudConfig cloudConfig = new CloudConfig(record);

    HelixCloudProperty cloudProperty = new HelixCloudProperty(cloudConfig);

    Assert.assertTrue(cloudProperty.getCloudEnabled());
    Assert.assertNull(cloudProperty.getCloudProvider());
    Assert.assertNull(cloudProperty.getCloudInfoProcessorName());
    Assert.assertNull(cloudProperty.getCloudInfoProcessorPackage());
  }
}
