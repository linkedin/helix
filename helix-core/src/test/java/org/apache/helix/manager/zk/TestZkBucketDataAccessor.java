package org.apache.helix.manager.zk;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordJacksonSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkBucketDataAccessor extends ZkTestBase {
  private static final String PATH = "/" + TestHelper.getTestClassName();
  private static final String NAME_KEY = TestHelper.getTestClassName();
  private static final String LAST_SUCCESSFUL_WRITE_KEY = "LAST_SUCCESSFUL_WRITE";
  private static final String LAST_WRITE_KEY = "LAST_WRITE";
  private static final long VERSION_TTL_MS = 1000L;

  // Populate list and map fields for content comparison
  private static final List<String> LIST_FIELD = ImmutableList.of("1", "2");
  private static final Map<String, String> MAP_FIELD = ImmutableMap.of("1", "2");

  private final ZNRecord record = new ZNRecord(NAME_KEY);

  private HelixZkClient _zkClient;
  private BucketDataAccessor _bucketDataAccessor;
  private BaseDataAccessor<byte[]> _zkBaseDataAccessor;
  private BucketDataAccessor _fastGCBucketDataAccessor;

  @BeforeClass
  public void beforeClass() {
    // Initialize ZK accessors for testing
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR));
    _zkClient.setZkSerializer(new ZkSerializer() {
      @Override
      public byte[] serialize(Object data) throws ZkMarshallingError {
        if (data instanceof byte[]) {
          return (byte[]) data;
        }
        throw new HelixException("ZkBucketDataAccesor only supports a byte array as an argument!");
      }

      @Override
      public Object deserialize(byte[] data) throws ZkMarshallingError {
        return data;
      }
    });
    _zkBaseDataAccessor = new ZkBaseDataAccessor<>(_zkClient);
    _bucketDataAccessor = new ZkBucketDataAccessor(_zkClient, 50 * 1024, VERSION_TTL_MS);

    // Fill in some data for the record
    record.setSimpleField(NAME_KEY, NAME_KEY);
    record.setListField(NAME_KEY, LIST_FIELD);
    record.setMapField(NAME_KEY, MAP_FIELD);
  }

  @AfterClass
  public void afterClass() {
    _bucketDataAccessor.disconnect();
  }

  /**
   * Attempt writing a simple HelixProperty using compressedBucketWrite.
   * @throws IOException
   */
  @Test
  public void testCompressedBucketWrite() throws IOException {
    Assert.assertTrue(_bucketDataAccessor.compressedBucketWrite(PATH, new HelixProperty(record)));
  }

  @Test(dependsOnMethods = "testCompressedBucketWrite")
  public void testMultipleWrites() throws Exception {
    // Note to use a count number < 10 for testing.
    // Otherwise the nodes named with version number will be ordered in a different alphabet order.
    // This might hide some bugs in the GC code。
    int count = 5;
    int pathCount = 2;

    Assert.assertTrue(VERSION_TTL_MS > 100,
        "This test should be executed with the TTL more than 100ms.");

    try {
      // Write "count + 1" times, so the latest version will be "count"
      for (int i = 0; i < count + 1; i++) {
        for (int j = 0; j < pathCount; j++) {
          _bucketDataAccessor.compressedBucketWrite(PATH + j, new HelixProperty(record));
        }
      }

      for (int j = 0; j < pathCount; j++) {
        String path = PATH + j;
        // Last known good version number should be "count"
        byte[] binarySuccessfulWriteVer = _zkBaseDataAccessor.get(path + "/" + LAST_SUCCESSFUL_WRITE_KEY, null, AccessOption.PERSISTENT);
        long lastSuccessfulWriteVer = Long.parseLong(new String(binarySuccessfulWriteVer));
        Assert.assertEquals(lastSuccessfulWriteVer, count);

        // Last write version should be "count"
        byte[] binaryWriteVer = _zkBaseDataAccessor.get(path + "/" + LAST_WRITE_KEY, null, AccessOption.PERSISTENT);
        long writeVer = Long.parseLong(new String(binaryWriteVer));
        Assert.assertEquals(writeVer, count);

        // Test that all previous versions have been deleted
        // Use Verifier because GC can take ZK delay
        Assert.assertTrue(TestHelper.verify(() -> {
          List<String> children = _zkBaseDataAccessor.getChildNames(path, AccessOption.PERSISTENT);
          return children.size() == 3 && children.containsAll(ImmutableList
              .of(LAST_SUCCESSFUL_WRITE_KEY, LAST_WRITE_KEY, new Long(lastSuccessfulWriteVer).toString()));
        }, TestHelper.WAIT_DURATION));

      }
    } finally {
      for (int j = 0; j < pathCount; j++) {
        _bucketDataAccessor.compressedBucketDelete(PATH + j);
      }
    }
  }

  /**
   * The record written in {@link #testCompressedBucketWrite()} is the same record that was written.
   */
  @Test(dependsOnMethods = "testMultipleWrites")
  public void testCompressedBucketRead() throws IOException {
    String path = PATH + "_" + TestHelper.getTestMethodName();
    _bucketDataAccessor.compressedBucketWrite(path, new HelixProperty(record));
    HelixProperty readRecord = _bucketDataAccessor.compressedBucketRead(path, HelixProperty.class);
    Assert.assertEquals(readRecord.getRecord().getSimpleField(NAME_KEY), NAME_KEY);
    Assert.assertEquals(readRecord.getRecord().getListField(NAME_KEY), LIST_FIELD);
    Assert.assertEquals(readRecord.getRecord().getMapField(NAME_KEY), MAP_FIELD);
    _bucketDataAccessor.compressedBucketDelete(path);
  }

  /**
   * Write a HelixProperty with large number of entries using BucketDataAccessor and read it back.
   */
  @Test(dependsOnMethods = "testCompressedBucketRead")
  public void testLargeWriteAndRead() throws IOException {
    String name = "largeResourceAssignment";
    HelixProperty property = createLargeHelixProperty(name, 100000);

    // Perform large write
    long before = System.currentTimeMillis();
    _bucketDataAccessor.compressedBucketWrite("/" + name, property);
    long after = System.currentTimeMillis();
    System.out.println("Write took " + (after - before) + " ms");

    // Read it back
    before = System.currentTimeMillis();
    HelixProperty readRecord =
        _bucketDataAccessor.compressedBucketRead("/" + name, HelixProperty.class);
    after = System.currentTimeMillis();
    System.out.println("Read took " + (after - before) + " ms");

    // Check against the original HelixProperty
    Assert.assertEquals(readRecord, property);
  }

  /**
   * Test to ensure bucket GC still occurs in high frequency write scenarios.
   */
  @Test(dependsOnMethods = "testLargeWriteAndRead")
  public void testGCCompletesUnderHighFrequency() throws Exception {
    String path = PATH + "_" + TestHelper.getTestMethodName();
    long gcTTL = 1000; // GC schedule for 1 second after write
    ZkBucketDataAccessor fastGCBucketDataAccessor = new ZkBucketDataAccessor(_zkClient, 50 * 1024, gcTTL);

    AtomicInteger writeCount = new AtomicInteger(0);
    // Below verifier continuously writes to the same path and then checks if the # of children is less than the # of
    // times we have written. This will only be true once the GC has cleaned up old versions, which will occur once the
    // GC time for the first write has passed.
    Assert.assertTrue(TestHelper.verify(() -> {
      Assert.assertTrue(fastGCBucketDataAccessor.compressedBucketWrite(path, new HelixProperty(record)));
      Thread.sleep(gcTTL/4);
      List<String> children = _zkBaseDataAccessor.getChildNames(path, AccessOption.PERSISTENT);
      // remove from list if name cant be parsed into long (aka not a version count node)
      children.removeIf(name -> {
        try {
          Long.parseLong(name);
          return false;
        } catch (NumberFormatException e) {
          return true;
        }
      });
      boolean result = children.size() < writeCount.incrementAndGet();
      if (!result) {
        System.out.println("Expecting stale versions to have been cleaned up. Write count was: " + writeCount +
            ", children were: " + children);
      }
      return result;
    }, TestHelper.WAIT_DURATION));
  }

  /**
   * CICP-5405 regression test.
   * When the compressed payload length is an exact multiple of the bucket size, the last bucket
   * must still be written and read at its full length. The previous implementation derived the
   * final bucket's length from a modulo, which evaluates to 0 in this case, so the final bucket
   * was written empty and the reconstructed payload kept a zeroed tail that failed to decompress.
   */
  @Test
  public void testExactMultipleOfBucketSizeRoundTrip() throws IOException {
    int bucketSize = 1024;
    ZNRecord exactMultipleRecord = findRecordWithCompressedSizeMultipleOf(bucketSize);
    BucketDataAccessor accessor =
        new ZkBucketDataAccessor(_zkClient, bucketSize, VERSION_TTL_MS);
    String path = PATH + "_" + TestHelper.getTestMethodName();
    try {
      HelixProperty property = new HelixProperty(exactMultipleRecord);
      Assert.assertTrue(accessor.compressedBucketWrite(path, property));
      HelixProperty readBack = accessor.compressedBucketRead(path, HelixProperty.class);
      Assert.assertEquals(readBack, property,
          "A payload whose compressed size is an exact multiple of the bucket size must round trip");
    } finally {
      accessor.compressedBucketDelete(path);
      accessor.disconnect();
    }
  }

  /**
   * CICP-5405: sweep the sizes immediately around each bucket boundary so an off-by-one in the
   * final bucket's length is caught regardless of which side of the boundary it falls on.
   */
  @Test
  public void testBucketBoundarySizes() throws IOException {
    int bucketSize = 1024;
    BucketDataAccessor accessor =
        new ZkBucketDataAccessor(_zkClient, bucketSize, VERSION_TTL_MS);
    try {
      for (int multiple = 1; multiple <= 3; multiple++) {
        for (int delta : new int[] {-1, 0, 1}) {
          int targetCompressedSize = multiple * bucketSize + delta;
          ZNRecord candidate = findRecordWithCompressedSize(targetCompressedSize);
          if (candidate == null) {
            // GZip output is not guaranteed to hit every exact length; skip the ones it cannot
            // produce rather than failing on an unreachable target.
            continue;
          }
          String path = PATH + "_boundary_" + multiple + "_" + delta;
          HelixProperty property = new HelixProperty(candidate);
          Assert.assertTrue(accessor.compressedBucketWrite(path, property));
          Assert.assertEquals(accessor.compressedBucketRead(path, HelixProperty.class), property,
              "Round trip failed for compressed size " + targetCompressedSize + " with bucket size "
                  + bucketSize);
          accessor.compressedBucketDelete(path);
        }
      }
    } finally {
      accessor.disconnect();
    }
  }

  /**
   * CICP-5405: the reader must size its bucket loop using the bucket size the data was WRITTEN
   * with (recorded in the metadata znode), not its own configured bucket size. Otherwise an
   * accessor configured differently from the writer reads the wrong number of buckets.
   */
  @Test
  public void testReadWithDifferentBucketSizeThanWrite() throws IOException {
    int writeBucketSize = 1024;
    int readBucketSize = 4 * 1024;
    BucketDataAccessor writeAccessor =
        new ZkBucketDataAccessor(_zkClient, writeBucketSize, VERSION_TTL_MS);
    BucketDataAccessor readAccessor =
        new ZkBucketDataAccessor(_zkClient, readBucketSize, VERSION_TTL_MS);
    String path = PATH + "_" + TestHelper.getTestMethodName();
    try {
      // Large enough to span several buckets at the write size.
      HelixProperty property = createLargeHelixProperty("differentBucketSize", 5000);
      Assert.assertTrue(writeAccessor.compressedBucketWrite(path, property));
      Assert.assertEquals(readAccessor.compressedBucketRead(path, HelixProperty.class), property,
          "A reader configured with a different bucket size must still honor the written layout");
    } finally {
      writeAccessor.compressedBucketDelete(path);
      writeAccessor.disconnect();
      readAccessor.disconnect();
    }
  }

  /**
   * CICP-5405: a record small enough to fit in a single bucket must still round trip, covering the
   * numBuckets == 1 case where the first bucket is also the last.
   */
  @Test
  public void testSingleBucketRoundTrip() throws IOException {
    int bucketSize = 50 * 1024;
    BucketDataAccessor accessor =
        new ZkBucketDataAccessor(_zkClient, bucketSize, VERSION_TTL_MS);
    String path = PATH + "_" + TestHelper.getTestMethodName();
    try {
      HelixProperty property = new HelixProperty(record);
      Assert.assertTrue(accessor.compressedBucketWrite(path, property));
      Assert.assertEquals(accessor.compressedBucketRead(path, HelixProperty.class), property);
    } finally {
      accessor.compressedBucketDelete(path);
      accessor.disconnect();
    }
  }

  /**
   * Grows a padding field until the compressed payload lands exactly on a bucket boundary.
   * The compressed size is not a predictable function of the input size, so it is searched for
   * rather than assumed. Incompressible padding is used so the compressed length advances roughly
   * one byte per iteration and the search terminates quickly.
   */
  private ZNRecord findRecordWithCompressedSizeMultipleOf(int bucketSize) throws IOException {
    ZNRecord found = findCompressedSize(length -> length > 0 && length % bucketSize == 0);
    Assert.assertNotNull(found,
        "Could not construct a record whose compressed size is a multiple of " + bucketSize);
    return found;
  }

  private ZNRecord findRecordWithCompressedSize(int targetCompressedSize) throws IOException {
    return findCompressedSize(length -> length == targetCompressedSize);
  }

  private ZNRecord findCompressedSize(java.util.function.IntPredicate matcher) throws IOException {
    ZkSerializer serializer = new ZNRecordJacksonSerializer();
    Random random = new Random(0); // Deterministic so failures reproduce.
    StringBuilder padding = new StringBuilder();
    for (int i = 0; i < 40000; i++) {
      ZNRecord candidate = new ZNRecord(NAME_KEY);
      candidate.setSimpleField("PADDING", padding.toString());
      byte[] compressed = GZipCompressionUtil.compress(serializer.serialize(candidate));
      if (matcher.test(compressed.length)) {
        return candidate;
      }
      // Base64-ish random content resists compression, so each appended character advances the
      // compressed length by about one byte.
      padding.append((char) ('!' + random.nextInt(90)));
    }
    return null;
  }

  private HelixProperty createLargeHelixProperty(String name, int numEntries) {
    HelixProperty property = new HelixProperty(name);
    for (int i = 0; i < numEntries; i++) {
      // Create a random string every time
      byte[] arrayKey = new byte[20];
      byte[] arrayVal = new byte[20];
      new Random().nextBytes(arrayKey);
      new Random().nextBytes(arrayVal);
      String randomStrKey = new String(arrayKey, StandardCharsets.UTF_8);
      String randomStrVal = new String(arrayVal, StandardCharsets.UTF_8);

      // Dummy mapField
      Map<String, String> mapField = new HashMap<>();
      mapField.put(randomStrKey, randomStrVal);

      property.getRecord().setMapField(randomStrKey, mapField);
    }
    return property;
  }
}
