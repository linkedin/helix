package org.apache.helix.zookeeper.datamodel.serializer;

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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZNRecordStringInterner {

  @Test
  public void testInternStringReturnsSharedInstance() {
    // Distinct instances with equal content must canonicalize to one instance.
    String a = new String("STANDBY");
    String b = new String("STANDBY");
    Assert.assertNotSame(a, b);

    String internedA = ZNRecordStringInterner.intern(a);
    String internedB = ZNRecordStringInterner.intern(b);

    Assert.assertSame(internedA, internedB);
    Assert.assertEquals(internedA, "STANDBY");
  }

  @Test
  public void testInternStringSkipsNullAndLongStrings() {
    Assert.assertNull(ZNRecordStringInterner.intern((String) null));

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i <= ZNRecordStringInterner.MAX_INTERNED_LENGTH; i++) {
      sb.append('x');
    }
    String large = sb.toString();
    // Over the length cap: returned unchanged (same instance), never interned.
    Assert.assertSame(ZNRecordStringInterner.intern(large), large);
  }

  @Test
  public void testInternZNRecordSharesStringsAcrossRecordsAndPreservesContent() {
    ZNRecord r1 = internedRecord("r1");
    ZNRecord r2 = internedRecord("r2");

    // Values are canonicalized and shared across independently-built records.
    Assert.assertSame(r1.getSimpleFields().get("CURRENT_STATE"),
        r2.getSimpleFields().get("CURRENT_STATE"));
    Assert.assertSame(r1.getMapFields().get("resource_0").get("partition_0"),
        r2.getMapFields().get("resource_0").get("partition_0"));
    Assert.assertSame(r1.getListFields().get("resource_0").get(0),
        r2.getListFields().get("resource_0").get(0));

    // Map keys are canonicalized and shared too.
    Assert.assertSame(r1.getMapFields().keySet().iterator().next(),
        r2.getMapFields().keySet().iterator().next());

    // The same value appearing in a simpleField and a mapField shares one instance.
    Assert.assertSame(r1.getSimpleFields().get("CURRENT_STATE"),
        r1.getMapFields().get("resource_0").get("partition_0"));

    // Content is fully preserved.
    Assert.assertEquals(r1.getSimpleFields().get("CURRENT_STATE"), "STANDBY");
    Assert.assertEquals(r1.getMapFields().get("resource_0").get("partition_0"), "STANDBY");
    Assert.assertEquals(r1.getListFields().get("resource_0"),
        Arrays.asList("instance_0", "instance_1"));
  }

  @Test
  public void testInternZNRecordHandlesNullAndEmpty() {
    Assert.assertNull(ZNRecordStringInterner.intern((ZNRecord) null));

    ZNRecord empty = new ZNRecord("empty");
    Assert.assertSame(ZNRecordStringInterner.intern(empty), empty);
  }

  @Test
  public void testInternZNRecordPreservesNullFieldValues() {
    ZNRecord record = new ZNRecord("testId");
    record.setMapField("K1", null);
    record.setListField("k2", null);
    record.setSimpleField("k3", null);

    ZNRecordStringInterner.intern(record);

    Assert.assertNull(record.getMapField("K1"));
    Assert.assertNull(record.getListField("k2"));
    Assert.assertNull(record.getSimpleField("k3"));
  }

  @Test
  public void testZNRecordSerializerDeserializeProducesSharedStrings() {
    ZNRecord record = new ZNRecord("test");
    Map<String, String> simpleFields = new TreeMap<>();
    simpleFields.put("STATE_A", "STANDBY");
    simpleFields.put("STATE_B", "STANDBY");
    record.setSimpleFields(simpleFields);

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    byte[] bytes = serializer.serialize(record);
    ZNRecord out = (ZNRecord) serializer.deserialize(bytes);

    Assert.assertSame(out.getSimpleFields().get("STATE_A"),
        out.getSimpleFields().get("STATE_B"));
    Assert.assertEquals(out.getSimpleFields().get("STATE_A"), "STANDBY");
    Assert.assertEquals(out, record);
  }

  @Test
  public void testZNRecordStreamingSerializerDeserializeProducesSharedStrings() {
    ZNRecord record = new ZNRecord("test");
    Map<String, String> simpleFields = new TreeMap<>();
    simpleFields.put("STATE_A", "STANDBY");
    simpleFields.put("STATE_B", "STANDBY");
    record.setSimpleFields(simpleFields);
    Map<String, Map<String, String>> mapFields = new TreeMap<>();
    Map<String, String> inner = new TreeMap<>();
    inner.put("partition_0", "STANDBY");
    mapFields.put("resource_0", inner);
    record.setMapFields(mapFields);

    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    byte[] bytes = serializer.serialize(record);
    ZNRecord out = (ZNRecord) serializer.deserialize(bytes);

    Assert.assertSame(out.getSimpleFields().get("STATE_A"),
        out.getSimpleFields().get("STATE_B"));
    Assert.assertSame(out.getSimpleFields().get("STATE_A"),
        out.getMapFields().get("resource_0").get("partition_0"));
    Assert.assertEquals(out, record);
  }

  private static ZNRecord internedRecord(String id) {
    ZNRecord record = new ZNRecord(id);

    Map<String, String> simpleFields = new LinkedHashMap<>();
    simpleFields.put(new String("CURRENT_STATE"), new String("STANDBY"));
    record.setSimpleFields(simpleFields);

    Map<String, Map<String, String>> mapFields = new LinkedHashMap<>();
    Map<String, String> inner = new LinkedHashMap<>();
    inner.put(new String("partition_0"), new String("STANDBY"));
    mapFields.put(new String("resource_0"), inner);
    record.setMapFields(mapFields);

    List<String> instances = new ArrayList<>();
    instances.add(new String("instance_0"));
    instances.add(new String("instance_1"));
    Map<String, List<String>> listFields = new LinkedHashMap<>();
    listFields.put(new String("resource_0"), instances);
    record.setListFields(listFields);

    return ZNRecordStringInterner.intern(record);
  }
}
