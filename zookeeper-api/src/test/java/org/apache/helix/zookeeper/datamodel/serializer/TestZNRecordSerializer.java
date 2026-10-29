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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZNRecordSerializer {

  /*
   * Field-name canonicalization is disabled on the serializer's Jackson factory
   * to avoid ByteQuadsCanonicalizer allocation churn on ZNRecord's dynamic map
   * keys. These tests verify that disabling it does not change what is parsed:
   * ZNRecords round-trip exactly, including high-cardinality dynamic keys, long
   * (> 12 byte) field names and multi-byte UTF-8 field names and values.
   */
  @Test
  public void testRoundTripWithDynamicKeys() {
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ZNRecord record = new ZNRecord("VeniceStore_v12_CurrentState");

    record.setSimpleField("SESSION_ID", "1234abcd5678");
    record.setSimpleField("STATE_MODEL_DEF", "OnlineOffline");

    // High-cardinality dynamic map keys (partition names) with standard inner keys.
    for (int p = 0; p < 500; p++) {
      Map<String, String> partitionState = new TreeMap<>();
      partitionState.put("CURRENT_STATE", (p % 2 == 0) ? "ONLINE" : "OFFLINE");
      partitionState.put("PREVIOUS_STATE", "OFFLINE");
      partitionState.put("INFO", "");
      record.setMapField("VeniceStore_v12_partition_" + p, partitionState);
    }

    // Dynamic list-field keys.
    for (int i = 0; i < 50; i++) {
      List<String> instances = new ArrayList<>();
      instances.add("lor1-app" + (90000 + i) + ".prod.linkedin.com_1234");
      record.setListField("preferenceList_" + i, instances);
    }

    byte[] bytes = serializer.serialize(record);
    ZNRecord deserialized = (ZNRecord) serializer.deserialize(bytes);

    Assert.assertNotNull(deserialized);
    Assert.assertEquals(deserialized.getId(), record.getId());
    // ZNRecord.equals() compares simpleFields, mapFields and listFields.
    Assert.assertEquals(deserialized, record);
    Assert.assertEquals(deserialized.getMapFields().size(), 500);
    Assert.assertEquals(deserialized.getMapField("VeniceStore_v12_partition_0").get("CURRENT_STATE"),
        "ONLINE");
  }

  @Test
  public void testLongAndMultiByteFieldNames() {
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ZNRecord record = new ZNRecord("record-with-tricky-names");

    // Field name longer than 12 bytes exercises the parseLongName parser path
    // that drives the canonicalizer; value and key both include characters that
    // must survive the non-canonicalizing decode.
    String longKey = "a_very_long_partition_field_name_exceeding_twelve_bytes_0";
    String unicodeKey = "p\u00e9partition_\u00fc_\u4e2d\u6587";
    record.setSimpleField(longKey, "value_with_\"quote\"_and_\u00e9\u4e2d\u6587");
    record.setSimpleField(unicodeKey, "unicode-value");

    Map<String, String> inner = new TreeMap<>();
    inner.put(longKey, "nested");
    inner.put(unicodeKey, "\u00fcber");
    record.setMapField(unicodeKey, inner);

    byte[] bytes = serializer.serialize(record);
    ZNRecord deserialized = (ZNRecord) serializer.deserialize(bytes);

    Assert.assertNotNull(deserialized);
    Assert.assertEquals(deserialized, record);
    Assert.assertEquals(deserialized.getSimpleField(longKey),
        "value_with_\"quote\"_and_\u00e9\u4e2d\u6587");
    Assert.assertEquals(deserialized.getSimpleField(unicodeKey), "unicode-value");
    Assert.assertEquals(deserialized.getMapField(unicodeKey).get(longKey), "nested");
  }

  /*
   * The serializer's ObjectMapper is a shared static instance used concurrently
   * by many ZkClient callback threads. This verifies that concurrent
   * serialize/deserialize of distinct records through that shared mapper always
   * round-trips correctly (i.e. the mapper is configured once and not mutated
   * per call, so there is no data race that could corrupt results).
   */
  @Test
  public void testConcurrentSerializeDeserialize() throws InterruptedException {
    final ZNRecordSerializer serializer = new ZNRecordSerializer();
    final int threads = 16;
    final int iterationsPerThread = 200;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch startGate = new CountDownLatch(1);
    final CountDownLatch doneGate = new CountDownLatch(threads);
    final AtomicInteger failures = new AtomicInteger();

    try {
      for (int t = 0; t < threads; t++) {
        final int threadId = t;
        pool.execute(() -> {
          try {
            startGate.await();
            for (int i = 0; i < iterationsPerThread; i++) {
              // Distinct, high-cardinality keys per thread/iteration to maximize
              // symbol-table churn and surface any shared-state corruption.
              ZNRecord record = new ZNRecord("record_" + threadId + "_" + i);
              record.setSimpleField("SESSION_ID", "s_" + threadId + "_" + i);
              for (int p = 0; p < 20; p++) {
                Map<String, String> state = new TreeMap<>();
                state.put("CURRENT_STATE", (p % 2 == 0) ? "ONLINE" : "OFFLINE");
                record.setMapField("t" + threadId + "_i" + i + "_partition_" + p, state);
              }
              byte[] bytes = serializer.serialize(record);
              ZNRecord back = (ZNRecord) serializer.deserialize(bytes);
              if (back == null || !back.equals(record)
                  || !back.getId().equals(record.getId())) {
                failures.incrementAndGet();
              }
            }
          } catch (Throwable th) {
            failures.incrementAndGet();
          } finally {
            doneGate.countDown();
          }
        });
      }
      startGate.countDown();
      Assert.assertTrue(doneGate.await(60, TimeUnit.SECONDS), "Concurrent workers timed out");
      Assert.assertEquals(failures.get(), 0,
          "Concurrent serialize/deserialize produced incorrect results");
    } finally {
      pool.shutdownNow();
    }
  }
}
