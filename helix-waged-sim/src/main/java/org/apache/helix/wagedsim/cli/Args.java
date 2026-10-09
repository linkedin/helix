package org.apache.helix.wagedsim.cli;

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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Minimal command-line parsing: positionals, {@code --name value}, {@code --name=value} and flags. */
final class Args {
  private static final Map<String, String> SHORT = new LinkedHashMap<>();
  private static final Set<String> FLAGS = new HashSet<>(Arrays.asList("current-states", "help",
      "keep-running", "no-report", "force", "verbose"));

  static {
    SHORT.put("-o", "out");
    SHORT.put("-j", "parallel");
    SHORT.put("-s", "scenario");
    SHORT.put("-h", "help");
  }

  final List<String> positional = new ArrayList<>();
  private final Map<String, List<String>> _options = new LinkedHashMap<>();

  Args(String[] args, int from) {
    for (int i = from; i < args.length; i++) {
      String arg = args[i];
      String name = null;
      String value = null;
      if (SHORT.containsKey(arg)) {
        name = SHORT.get(arg);
      } else if (arg.startsWith("--")) {
        name = arg.substring(2);
        int eq = name.indexOf('=');
        if (eq >= 0) {
          value = name.substring(eq + 1);
          name = name.substring(0, eq);
        }
      }
      if (name == null) {
        positional.add(arg);
        continue;
      }
      if (value == null) {
        if (FLAGS.contains(name)) {
          value = "true";
        } else if (i + 1 < args.length) {
          value = args[++i];
        } else {
          throw new IllegalArgumentException("Option --" + name + " needs a value");
        }
      }
      _options.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
    }
  }

  String get(String name) {
    List<String> values = _options.get(name);
    return values == null ? null : values.get(values.size() - 1);
  }

  String get(String name, String defaultValue) {
    String value = get(name);
    return value == null ? defaultValue : value;
  }

  List<String> all(String name) {
    return _options.getOrDefault(name, new ArrayList<>());
  }

  boolean flag(String name) {
    return "true".equalsIgnoreCase(get(name));
  }

  String require(String name) {
    String value = get(name);
    if (value == null) {
      throw new IllegalArgumentException("Missing --" + name);
    }
    return value;
  }

  Set<String> names() {
    return _options.keySet();
  }
}
