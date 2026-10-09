package org.apache.helix.wagedsim.report;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The content of a report as plain data (sections of rows), rendered as Markdown or HTML. Keeping
 * one model means both formats always show the same numbers.
 */
public class ReportContent {
  /** A table: header and rows of cells. */
  public static class Table {
    public final List<String> header = new ArrayList<>();
    public final List<List<String>> rows = new ArrayList<>();
    /** Optional per-column kind used for colouring: skew, util, verdict or null. */
    public final List<String> kinds = new ArrayList<>();

    public Table(String... columns) {
      for (String column : columns) {
        header.add(column);
        kinds.add(null);
      }
    }

    public Table kind(int column, String kind) {
      kinds.set(column, kind);
      return this;
    }

    public void row(List<String> cells) {
      rows.add(cells);
    }
  }

  /** A section: a title, paragraphs and bullet lists, tables, and charts (HTML only). */
  public static class Section {
    public final String title;
    public final List<Object> blocks = new ArrayList<>();

    public Section(String title) {
      this.title = title;
    }

    public Section text(String text) {
      blocks.add(text);
      return this;
    }

    public Section bullets(List<String> items) {
      blocks.add(new Bullets(items));
      return this;
    }

    public Section table(Table table) {
      blocks.add(table);
      return this;
    }

    public Section chart(Chart chart) {
      blocks.add(chart);
      return this;
    }
  }

  public static class Bullets {
    public final List<String> items;

    Bullets(List<String> items) {
      this.items = items;
    }
  }

  /** A line or bar chart; rendered only in HTML. */
  public static class Chart {
    public String title;
    public String type = "line";
    public String yLabel;
    /** series name -> points (x, y). */
    public Map<String, List<double[]>> series = new LinkedHashMap<>();
    /** x positions to mark (event rounds). */
    public List<Double> markers = new ArrayList<>();
    /** Optional reference line, for example 1.0 for perfectly even. */
    public Double reference;
    /** Label of the x axis of a line chart. */
    public String xLabel = "round";
  }

  public String title;
  public String subtitle;
  public final List<Section> sections = new ArrayList<>();

  public Section section(String title) {
    Section section = new Section(title);
    sections.add(section);
    return section;
  }

  public static String number(Object value) {
    if (value == null) {
      return "";
    }
    if (value instanceof Double || value instanceof Float) {
      double d = ((Number) value).doubleValue();
      if (Double.isNaN(d)) {
        return "";
      }
      if (Math.abs(d) >= 1000) {
        return String.format(Locale.ROOT, "%,.0f", d);
      }
      String text = String.format(Locale.ROOT, "%.3f", d);
      return text.contains(".") ? text.replaceAll("0+$", "").replaceAll("\\.$", "") : text;
    }
    if (value instanceof Number) {
      long l = ((Number) value).longValue();
      return Math.abs(l) >= 10000 ? String.format(Locale.ROOT, "%,d", l) : String.valueOf(l);
    }
    return value.toString();
  }
}
