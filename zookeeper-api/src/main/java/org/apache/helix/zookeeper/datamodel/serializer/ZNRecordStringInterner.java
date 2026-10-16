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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Canonicalizes the short, highly-repeated Strings that dominate deserialized
 * {@link ZNRecord}s so that all logically-equal copies share a single String instance.
 * <p>
 * A heap analysis of a production Helix controller found that ~93% of the short Strings on
 * the heap were redundant duplicates (for example the state "STANDBY" alone had ~14.9M
 * copies, and individual instance names several million copies each), accounting for
 * multiple GB of heap. The offenders are a small, low-cardinality vocabulary: state-model
 * states (LEADER/STANDBY/OFFLINE/...), instance names, partition names, and field keys.
 * Because a ZNRecord is deserialized once per ZooKeeper update but then cached for a long
 * time, canonicalizing on the deserialize path lowers both the live footprint and the
 * allocation rate.
 * <p>
 * Canonicalization is always safe: Strings are immutable, so replacing equal copies with a
 * shared instance never changes {@code equals()}/{@code hashCode()} behavior; it only makes
 * reference ({@code ==}) equality more likely to hold.
 * <p>
 * The backing interner holds its canonical instances <i>weakly</i>, so a value is retained
 * only while something else references it and is reclaimed by GC once the last referencing
 * ZNRecord is evicted -- the interner is self-bounding and needs no capacity tuning. Strings
 * longer than {@link #MAX_INTERNED_LENGTH} (such as serialized payloads) are left untouched,
 * since they are rarely duplicated and would only add interner overhead.
 */
public final class ZNRecordStringInterner {
  // Identifiers, state names, and field keys are short; longer strings are almost never
  // duplicated and are skipped to keep the interner small.
  static final int MAX_INTERNED_LENGTH = 256;

  private static final Interner<String> INTERNER = Interners.newWeakInterner();

  private ZNRecordStringInterner() {
  }

  /**
   * Returns a canonical instance equal to {@code s}, or {@code s} unchanged when it is null
   * or longer than {@link #MAX_INTERNED_LENGTH}.
   */
  public static String intern(String s) {
    if (s == null || s.length() > MAX_INTERNED_LENGTH) {
      return s;
    }
    return INTERNER.intern(s);
  }

  /**
   * Rewrites the simple, list, and map fields of {@code record} so that every field key and
   * value uses a canonical String instance. The field containers are rebuilt (preserving
   * iteration order) because map keys cannot be canonicalized in place; the rebuilt
   * containers are also right-sized to the number of entries they actually hold. The
   * record's id and raw payload are left untouched.
   *
   * @return {@code record} (for call-site convenience), or {@code null} if {@code record} is null
   */
  public static ZNRecord intern(ZNRecord record) {
    if (record == null) {
      return null;
    }

    Map<String, String> simpleFields = record.getSimpleFields();
    if (simpleFields != null && !simpleFields.isEmpty()) {
      Map<String, String> interned = Maps.newLinkedHashMapWithExpectedSize(simpleFields.size());
      for (Map.Entry<String, String> e : simpleFields.entrySet()) {
        interned.put(intern(e.getKey()), intern(e.getValue()));
      }
      record.setSimpleFields(interned);
    }

    Map<String, List<String>> listFields = record.getListFields();
    if (listFields != null && !listFields.isEmpty()) {
      Map<String, List<String>> interned = Maps.newLinkedHashMapWithExpectedSize(listFields.size());
      for (Map.Entry<String, List<String>> e : listFields.entrySet()) {
        List<String> values = e.getValue();
        List<String> internedValues = null;
        if (values != null) {
          internedValues = new ArrayList<>(values.size());
          for (String v : values) {
            internedValues.add(intern(v));
          }
        }
        interned.put(intern(e.getKey()), internedValues);
      }
      record.setListFields(interned);
    }

    Map<String, Map<String, String>> mapFields = record.getMapFields();
    if (mapFields != null && !mapFields.isEmpty()) {
      Map<String, Map<String, String>> interned =
          Maps.newLinkedHashMapWithExpectedSize(mapFields.size());
      for (Map.Entry<String, Map<String, String>> e : mapFields.entrySet()) {
        Map<String, String> inner = e.getValue();
        Map<String, String> internedInner = null;
        if (inner != null) {
          internedInner = Maps.newLinkedHashMapWithExpectedSize(inner.size());
          for (Map.Entry<String, String> ie : inner.entrySet()) {
            internedInner.put(intern(ie.getKey()), intern(ie.getValue()));
          }
        }
        interned.put(intern(e.getKey()), internedInner);
      }
      record.setMapFields(interned);
    }

    return record;
  }
}
