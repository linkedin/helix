package org.apache.helix.wagedsim.source;

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
import java.util.Locale;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Partition weight distributions: a number, {@code constant(v)}, {@code uniform(min, max)},
 * {@code normal(mean, sd)}, {@code lognormal(median, sigma)}, or an explicit list (cycled over
 * partitions). Samples are rounded to non-negative integers, as WAGED weights are integers.
 */
public abstract class WeightDistribution {
  private static final Pattern CALL = Pattern.compile("\\s*([a-zA-Z]+)\\s*\\(([^)]*)\\)\\s*");

  public abstract int sample(Random random, int index);

  public abstract String describe();

  public static WeightDistribution parse(Object spec) {
    if (spec instanceof Number) {
      return constant(((Number) spec).doubleValue());
    }
    if (spec instanceof List) {
      List<Integer> values = new ArrayList<>();
      for (Object item : (List<?>) spec) {
        values.add((int) Math.round(Double.parseDouble(item.toString())));
      }
      return new WeightDistribution() {
        @Override
        public int sample(Random random, int index) {
          return values.get(index % values.size());
        }

        @Override
        public String describe() {
          return "list" + values;
        }
      };
    }
    String text = String.valueOf(spec).trim();
    if (text.matches("-?\\d+(\\.\\d+)?")) {
      return constant(Double.parseDouble(text));
    }
    Matcher matcher = CALL.matcher(text);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid weight distribution: " + spec);
    }
    String name = matcher.group(1).toLowerCase(Locale.ROOT);
    String[] args = matcher.group(2).split(",");
    double[] values = new double[args.length];
    for (int i = 0; i < args.length; i++) {
      values[i] = Double.parseDouble(args[i].trim());
    }
    switch (name) {
      case "constant":
        return constant(values[0]);
      case "uniform":
        require(name, values, 2);
        return new WeightDistribution() {
          @Override
          public int sample(Random random, int index) {
            return clamp(values[0] + random.nextDouble() * (values[1] - values[0]));
          }

          @Override
          public String describe() {
            return text;
          }
        };
      case "normal":
        require(name, values, 2);
        return new WeightDistribution() {
          @Override
          public int sample(Random random, int index) {
            return clamp(values[0] + random.nextGaussian() * values[1]);
          }

          @Override
          public String describe() {
            return text;
          }
        };
      case "lognormal":
        require(name, values, 2);
        return new WeightDistribution() {
          @Override
          public int sample(Random random, int index) {
            return clamp(values[0] * Math.exp(random.nextGaussian() * values[1]));
          }

          @Override
          public String describe() {
            return text;
          }
        };
      default:
        throw new IllegalArgumentException("Unknown weight distribution '" + name
            + "'; use constant, uniform, normal, lognormal or a list");
    }
  }

  private static void require(String name, double[] values, int count) {
    if (values.length != count) {
      throw new IllegalArgumentException(name + " needs " + count + " arguments");
    }
  }

  private static WeightDistribution constant(double value) {
    int weight = clamp(value);
    return new WeightDistribution() {
      @Override
      public int sample(Random random, int index) {
        return weight;
      }

      @Override
      public String describe() {
        return String.valueOf(weight);
      }

      @Override
      public boolean isConstant() {
        return true;
      }
    };
  }

  public boolean isConstant() {
    return false;
  }

  static int clamp(double value) {
    return (int) Math.max(0, Math.round(value));
  }
}
