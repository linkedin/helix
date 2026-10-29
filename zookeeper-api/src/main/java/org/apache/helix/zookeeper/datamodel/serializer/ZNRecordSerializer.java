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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.introspect.CodehausJacksonIntrospector;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.util.ZNRecordUtil;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZNRecordSerializer implements ZkSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(ZNRecordSerializer.class);

  /*
   * ZNRecord JSON field names are dominated by high-cardinality, dynamic map
   * keys (partition, instance and session names in simpleFields, listFields and
   * mapFields) that rarely repeat across records. Jackson's field-name
   * canonicalizer (ByteQuadsCanonicalizer) is built for recurring field names:
   * with these dynamic keys it continually grows and copies its backing int[]
   * and String[] arrays on every parse (ByteQuadsCanonicalizer._verifySharing
   * -> Arrays.copyOf), which was the single largest source of allocation and GC
   * pressure on Helix controllers. Canonicalization provides no benefit here, so
   * disable it on the shared factory to skip the symbol table entirely.
   */
  private static final JsonFactory JSON_FACTORY =
      new JsonFactory().disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);

  protected static ObjectMapper mapper = new ObjectMapper(JSON_FACTORY)
      // TODO: remove it after upgrading ZNRecord's annotations to Jackson 2
      .setAnnotationIntrospector(new CodehausJacksonIntrospector())
      // The mapper is shared across all (concurrent) serialize/deserialize
      // calls, so configure it once, here, before first use. Mutating these
      // features per call (as this class used to) is a data race on the shared
      // mapper's configuration and allocates a new config object on every call.
      .enable(MapperFeature.AUTO_DETECT_FIELDS)
      .enable(MapperFeature.AUTO_DETECT_SETTERS)
      .enable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private static int getListFieldBound(ZNRecord record) {
    int max = Integer.MAX_VALUE;
    if (record.getSimpleFields().containsKey(ZNRecord.LIST_FIELD_BOUND)) {
      String maxStr = record.getSimpleField(ZNRecord.LIST_FIELD_BOUND);
      try {
        max = Integer.parseInt(maxStr);
      } catch (Exception e) {
        LOG.error("IllegalNumberFormat for list field bound: " + maxStr);
      }
    }
    return max;
  }

  @Override
  public byte[] serialize(Object data) {
    if (!(data instanceof ZNRecord)) {
      // null is NOT an instance of any class
      LOG.error("Input object must be of type ZNRecord but it is " + data
          + ". Will not write to zk");
      throw new ZkMarshallingError("Input object is not of type ZNRecord (was " + data + ")");
    }

    ZNRecord record = (ZNRecord) data;

    // apply retention policy
    int max = getListFieldBound(record);
    if (max < Integer.MAX_VALUE) {
      Map<String, List<String>> listMap = record.getListFields();
      for (String key : listMap.keySet()) {
        List<String> list = listMap.get(key);
        if (list.size() > max) {
          listMap.put(key, list.subList(0, max));
        }
      }
    }

    // do serialization
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] serializedBytes;
    boolean isCompressed = false;

    try {
      mapper.writeValue(baos, data);
      serializedBytes = baos.toByteArray();
      // apply compression if needed
      if (ZNRecordUtil.shouldCompress(record, serializedBytes.length)) {
        serializedBytes = GZipCompressionUtil.compress(serializedBytes);
        isCompressed = true;
      }
    } catch (Exception e) {
      LOG.error(
          "Exception during data serialization. ZNRecord ID: {} will not be written to zk.",
          record.getId(), e);
      throw new ZkMarshallingError(e);
    }

    int writeSizeLimit = ZNRecordUtil.getSerializerWriteSizeLimit();
    if (serializedBytes.length > writeSizeLimit) {
      LOG.error("Data size: {} is greater than {} bytes, is compressed: {}, ZNRecord.id: {}."
              + " Data will not be written to Zookeeper.", serializedBytes.length, writeSizeLimit,
          isCompressed, record.getId());
      throw new ZkMarshallingError(
          "Data size: " + serializedBytes.length + " is greater than " + writeSizeLimit
              + " bytes, is compressed: " + isCompressed + ", ZNRecord.id: " + record.getId());
    }

    return serializedBytes;
  }

  @Override
  public Object deserialize(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      // reading a parent/null node
      return null;
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    try {
      //decompress the data if its already compressed
      if (GZipCompressionUtil.isCompressed(bytes)) {
        byte[] uncompressedBytes = GZipCompressionUtil.uncompress(bais);
        bais = new ByteArrayInputStream(uncompressedBytes);
      }

      return mapper.readValue(bais, ZNRecord.class);
    } catch (Exception e) {
      LOG.error("Exception during deserialization of bytes: {}", new String(bytes), e);
      return null;
    }
  }
}
