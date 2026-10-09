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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses and formats durations such as {@code 500ms}, {@code 90s}, {@code 30m}, {@code 16h}, {@code 2d}. */
public final class Durations {
  private static final Pattern PART = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*(ms|s|m|h|d)");

  private Durations() {
  }

  public static long parseMillis(Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Missing duration");
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    String text = value.toString().trim().toLowerCase(Locale.ROOT);
    if (text.matches("\\d+")) {
      return Long.parseLong(text);
    }
    Matcher matcher = PART.matcher(text);
    long total = 0;
    int consumed = 0;
    while (matcher.find()) {
      if (!text.substring(consumed, matcher.start()).trim().isEmpty()) {
        throw new IllegalArgumentException("Invalid duration: " + value);
      }
      double amount = Double.parseDouble(matcher.group(1));
      total += Math.round(amount * unitMillis(matcher.group(2)));
      consumed = matcher.end();
    }
    if (consumed == 0 || !text.substring(consumed).trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid duration: " + value);
    }
    return total;
  }

  private static long unitMillis(String unit) {
    switch (unit) {
      case "ms":
        return 1L;
      case "s":
        return 1000L;
      case "m":
        return 60_000L;
      case "h":
        return 3_600_000L;
      case "d":
        return 86_400_000L;
      default:
        throw new IllegalArgumentException("Unknown unit " + unit);
    }
  }

  public static String format(long millis) {
    if (millis < 0) {
      return "-" + format(-millis);
    }
    if (millis < 1000) {
      return millis + "ms";
    }
    long seconds = millis / 1000;
    if (seconds < 60) {
      return String.format(Locale.ROOT, "%.1fs", millis / 1000.0);
    }
    long minutes = seconds / 60;
    if (minutes < 60) {
      return minutes + "m" + (seconds % 60 == 0 ? "" : (seconds % 60) + "s");
    }
    long hours = minutes / 60;
    if (hours < 48) {
      return hours + "h" + (minutes % 60 == 0 ? "" : (minutes % 60) + "m");
    }
    return (hours / 24) + "d" + (hours % 24 == 0 ? "" : (hours % 24) + "h");
  }
}
