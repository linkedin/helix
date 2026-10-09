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

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Renders report content as one self-contained HTML file: inline CSS and SVG charts, no scripts and
 * no external resources. Skew and utilization cells are coloured from green (even, cool) to red.
 */
public final class HtmlReport {
  private static final String[] PALETTE = {"#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e",
      "#17becf", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22"};

  private HtmlReport() {
  }

  public static String render(ReportContent report) {
    StringBuilder out = new StringBuilder();
    out.append("<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.append("<title>").append(esc(report.title)).append("</title>");
    out.append("<style>")
        .append("body{font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:24px auto;")
        .append("max-width:1200px;color:#222;line-height:1.45}")
        .append("h1{font-size:24px;margin-bottom:4px}h2{font-size:19px;margin-top:32px;border-bottom:1px solid #ddd;")
        .append("padding-bottom:4px}.sub{color:#666;margin-bottom:16px}")
        .append("table{border-collapse:collapse;margin:8px 0 16px;font-size:13px}")
        .append("th,td{border:1px solid #ddd;padding:4px 8px;text-align:right;vertical-align:top}")
        .append("th{background:#f3f5f7}td:first-child,th:first-child{text-align:left}")
        .append(".PASS{background:#c6efce;font-weight:600}.FAIL{background:#ffc7ce;font-weight:600}")
        .append(".ERROR{background:#ffeb9c;font-weight:600}.txt{text-align:left}")
        .append(".chart{margin:8px 0 20px}.legend span{margin-right:14px;font-size:12px}")
        .append("</style></head><body>\n");
    out.append("<h1>").append(esc(report.title)).append("</h1>");
    if (report.subtitle != null) {
      out.append("<div class=\"sub\">").append(esc(report.subtitle)).append("</div>");
    }
    for (ReportContent.Section section : report.sections) {
      out.append("<h2>").append(esc(section.title)).append("</h2>\n");
      for (Object block : section.blocks) {
        if (block instanceof String) {
          out.append("<p>").append(esc((String) block)).append("</p>\n");
        } else if (block instanceof ReportContent.Bullets) {
          out.append("<ul>");
          for (String item : ((ReportContent.Bullets) block).items) {
            out.append("<li>").append(esc(item)).append("</li>");
          }
          out.append("</ul>\n");
        } else if (block instanceof ReportContent.Table) {
          table(out, (ReportContent.Table) block);
        } else if (block instanceof ReportContent.Chart) {
          chart(out, (ReportContent.Chart) block);
        }
      }
    }
    out.append("</body></html>\n");
    return out.toString();
  }

  private static void table(StringBuilder out, ReportContent.Table table) {
    out.append("<table><tr>");
    for (String column : table.header) {
      out.append("<th>").append(esc(column)).append("</th>");
    }
    out.append("</tr>\n");
    for (List<String> row : table.rows) {
      out.append("<tr>");
      for (int i = 0; i < row.size(); i++) {
        String cell = row.get(i);
        String kind = i < table.kinds.size() ? table.kinds.get(i) : null;
        String attr = "";
        if ("verdict".equals(kind)) {
          attr = " class=\"" + esc(cell) + "\"";
        } else if ("skew".equals(kind)) {
          attr = colour(cell, 1.0, 1.15, 1.4);
        } else if ("util".equals(kind)) {
          attr = colour(cell, 40, 70, 100);
        }
        if (attr.isEmpty() && cell != null && !cell.isEmpty()
            && !cell.replace(",", "").replace("+", "").matches("-?[0-9.]+%?")) {
          attr = " class=\"txt\"";
        }
        out.append("<td").append(attr).append('>').append(esc(cell)).append("</td>");
      }
      out.append("</tr>\n");
    }
    out.append("</table>\n");
  }

  /** Green at or below {@code low}, yellow at {@code mid}, red at or above {@code high}. */
  static String colour(String cell, double low, double mid, double high) {
    double value;
    try {
      value = Double.parseDouble(cell.replace(",", ""));
    } catch (RuntimeException e) {
      return "";
    }
    int[] green = {198, 239, 206};
    int[] yellow = {255, 235, 156};
    int[] red = {255, 199, 206};
    int[] rgb;
    if (value <= low) {
      rgb = green;
    } else if (value >= high) {
      rgb = red;
    } else if (value <= mid) {
      rgb = mix(green, yellow, (value - low) / (mid - low));
    } else {
      rgb = mix(yellow, red, (value - mid) / (high - mid));
    }
    return String.format(Locale.ROOT, " style=\"background:rgb(%d,%d,%d)\"", rgb[0], rgb[1], rgb[2]);
  }

  private static int[] mix(int[] a, int[] b, double t) {
    return new int[]{(int) Math.round(a[0] + (b[0] - a[0]) * t), (int) Math.round(a[1] + (b[1] - a[1]) * t),
        (int) Math.round(a[2] + (b[2] - a[2]) * t)};
  }

  private static void chart(StringBuilder out, ReportContent.Chart chart) {
    int width = 760;
    int height = 260;
    int left = 60;
    int right = 20;
    int top = 30;
    int bottom = 36;
    double minX = Double.MAX_VALUE;
    double maxX = -Double.MAX_VALUE;
    double minY = Double.MAX_VALUE;
    double maxY = -Double.MAX_VALUE;
    for (List<double[]> points : chart.series.values()) {
      for (double[] p : points) {
        minX = Math.min(minX, p[0]);
        maxX = Math.max(maxX, p[0]);
        minY = Math.min(minY, p[1]);
        maxY = Math.max(maxY, p[1]);
      }
    }
    if (minX == Double.MAX_VALUE) {
      return;
    }
    if (chart.reference != null) {
      minY = Math.min(minY, chart.reference);
      maxY = Math.max(maxY, chart.reference);
    }
    if ("bars".equals(chart.type)) {
      minY = Math.min(0, minY);
      minX -= 0.5;
      maxX += 0.5;
    }
    double padY = (maxY - minY) * 0.08;
    if (padY == 0) {
      padY = Math.max(Math.abs(maxY) * 0.05, 0.05);
    }
    minY = "bars".equals(chart.type) ? minY : minY - padY;
    maxY += padY;
    if (maxX == minX) {
      maxX = minX + 1;
    }
    final double x0 = minX;
    final double x1 = maxX;
    final double y0 = minY;
    final double y1 = maxY;
    int plotW = width - left - right;
    int plotH = height - top - bottom;
    java.util.function.DoubleUnaryOperator sx = x -> left + (x - x0) / (x1 - x0) * plotW;
    java.util.function.DoubleUnaryOperator sy = y -> top + (1 - (y - y0) / (y1 - y0)) * plotH;
    out.append("<div class=\"chart\"><svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(width)
        .append("\" height=\"").append(height).append("\" role=\"img\">");
    out.append("<text x=\"").append(left).append("\" y=\"18\" font-size=\"13\" font-weight=\"600\">")
        .append(esc(chart.title)).append("</text>");
    out.append(String.format(Locale.ROOT,
        "<rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"%d\" fill=\"#fff\" stroke=\"#ccc\"/>", left, top, plotW, plotH));
    for (int i = 0; i <= 4; i++) {
      double y = y0 + (y1 - y0) * i / 4;
      double py = sy.applyAsDouble(y);
      out.append(String.format(Locale.ROOT,
          "<line x1=\"%d\" y1=\"%.1f\" x2=\"%d\" y2=\"%.1f\" stroke=\"#eee\"/>", left, py, left + plotW, py));
      out.append(String.format(Locale.ROOT,
          "<text x=\"%d\" y=\"%.1f\" font-size=\"11\" text-anchor=\"end\" fill=\"#555\">%s</text>",
          left - 6, py + 4, esc(ReportContent.number(y))));
    }
    if (!"bars".equals(chart.type)) {
      // Rounds are whole numbers: label every round, or every k-th when there are many.
      long step = Math.max(1, (long) Math.ceil((x1 - x0) / 12.0));
      for (long x = (long) Math.ceil(x0); x <= x1; x += step) {
        out.append(String.format(Locale.ROOT,
            "<text x=\"%.1f\" y=\"%d\" font-size=\"11\" text-anchor=\"middle\" fill=\"#555\">%d</text>",
            sx.applyAsDouble(x), top + plotH + 16, x));
      }
      out.append(String.format(Locale.ROOT,
          "<text x=\"%d\" y=\"%d\" font-size=\"11\" text-anchor=\"middle\" fill=\"#555\">%s</text>",
          left + plotW / 2, height - 4, esc(chart.xLabel)));
      for (Double marker : chart.markers) {
        double px = sx.applyAsDouble(marker);
        out.append(String.format(Locale.ROOT, "<line x1=\"%.1f\" y1=\"%d\" x2=\"%.1f\" y2=\"%d\" "
            + "stroke=\"#999\" stroke-dasharray=\"4,3\"/>", px, top, px, top + plotH));
      }
    } else {
      out.append(String.format(Locale.ROOT,
          "<text x=\"%d\" y=\"%d\" font-size=\"11\" text-anchor=\"middle\" fill=\"#555\">instances, "
              + "hottest first</text>", left + plotW / 2, height - 4));
    }
    if (chart.reference != null) {
      double py = sy.applyAsDouble(chart.reference);
      out.append(String.format(Locale.ROOT, "<line x1=\"%d\" y1=\"%.1f\" x2=\"%d\" y2=\"%.1f\" "
          + "stroke=\"#2ca02c\" stroke-dasharray=\"2,2\"/>", left, py, left + plotW, py));
    }
    int index = 0;
    int seriesCount = chart.series.size();
    for (Map.Entry<String, List<double[]>> series : chart.series.entrySet()) {
      String colour = PALETTE[index % PALETTE.length];
      if ("bars".equals(chart.type)) {
        double slot = plotW / Math.max(1.0, x1 - x0);
        double barWidth = Math.max(1, slot * 0.8 / seriesCount);
        for (double[] p : series.getValue()) {
          double px = sx.applyAsDouble(p[0]) - slot * 0.4 + index * barWidth;
          double py = sy.applyAsDouble(p[1]);
          double base = sy.applyAsDouble(Math.max(0, y0));
          out.append(String.format(Locale.ROOT,
              "<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" fill=\"%s\" opacity=\"0.8\"/>",
              px, Math.min(py, base), barWidth, Math.abs(base - py), colour));
        }
      } else {
        StringBuilder path = new StringBuilder();
        for (double[] p : series.getValue()) {
          path.append(path.length() == 0 ? "M" : "L").append(String.format(Locale.ROOT, "%.1f,%.1f ",
              sx.applyAsDouble(p[0]), sy.applyAsDouble(p[1])));
        }
        out.append("<path d=\"").append(path).append("\" fill=\"none\" stroke=\"").append(colour)
            .append("\" stroke-width=\"2\"/>");
        for (double[] p : series.getValue()) {
          out.append(String.format(Locale.ROOT, "<circle cx=\"%.1f\" cy=\"%.1f\" r=\"2.5\" fill=\"%s\"/>",
              sx.applyAsDouble(p[0]), sy.applyAsDouble(p[1]), colour));
        }
      }
      index++;
    }
    out.append("</svg><div class=\"legend\">");
    index = 0;
    for (String name : chart.series.keySet()) {
      out.append("<span style=\"color:").append(PALETTE[index++ % PALETTE.length]).append("\">&#9632; ")
          .append(esc(name)).append("</span>");
    }
    out.append("</div></div>\n");
  }

  static String esc(String text) {
    if (text == null) {
      return "";
    }
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
  }
}
