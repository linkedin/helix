package org.apache.helix.wagedsim.util;

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
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Shared JSON helpers. ZNRecords use the same JSON shape as ZooKeeper and helix-rest. */
public final class Json {
  public static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
      .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

  private Json() {
  }

  public static ZNRecord toRecord(JsonNode node) throws IOException {
    ZNRecord record = MAPPER.treeToValue(node, ZNRecord.class);
    if (record == null || record.getId() == null) {
      throw new IOException("Not a ZNRecord: " + abbreviate(node.toString()));
    }
    return record;
  }

  public static ZNRecord readRecord(Path file) throws IOException {
    return toRecord(MAPPER.readTree(file.toFile()));
  }

  public static void writeRecord(Path file, ZNRecord record) throws IOException {
    Files.createDirectories(file.getParent());
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), record);
  }

  public static void write(Path file, Object value) throws IOException {
    Files.createDirectories(file.toAbsolutePath().getParent());
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), value);
  }

  public static String compact(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static ZNRecord copy(ZNRecord record) {
    return new ZNRecord(record);
  }

  public static String abbreviate(String text) {
    return text.length() > 200 ? text.substring(0, 200) + "..." : text;
  }
}
