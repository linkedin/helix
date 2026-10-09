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

/** Renders report content as Markdown. Charts are HTML-only and are listed by title. */
public final class MarkdownReport {
  private MarkdownReport() {
  }

  public static String render(ReportContent report) {
    StringBuilder out = new StringBuilder();
    out.append("# ").append(report.title).append("\n\n");
    if (report.subtitle != null) {
      out.append(report.subtitle).append("\n\n");
    }
    for (ReportContent.Section section : report.sections) {
      out.append("## ").append(section.title).append("\n\n");
      boolean chartNote = false;
      for (Object block : section.blocks) {
        if (block instanceof String) {
          out.append(block).append("\n\n");
        } else if (block instanceof ReportContent.Bullets) {
          for (String item : ((ReportContent.Bullets) block).items) {
            out.append("- ").append(item).append('\n');
          }
          out.append('\n');
        } else if (block instanceof ReportContent.Table) {
          table(out, (ReportContent.Table) block);
        } else if (block instanceof ReportContent.Chart && !chartNote) {
          out.append("_Charts are in report.html._\n\n");
          chartNote = true;
        }
      }
    }
    return out.toString();
  }

  private static void table(StringBuilder out, ReportContent.Table table) {
    out.append('|');
    for (String column : table.header) {
      out.append(' ').append(escape(column)).append(" |");
    }
    out.append("\n|");
    for (int i = 0; i < table.header.size(); i++) {
      out.append(i > 0 && numericColumn(table, i) ? "--:|" : "---|");
    }
    out.append('\n');
    for (List<String> row : table.rows) {
      out.append('|');
      for (String cell : row) {
        out.append(' ').append(escape(cell)).append(" |");
      }
      out.append('\n');
    }
    out.append('\n');
  }

  private static boolean numericColumn(ReportContent.Table table, int column) {
    boolean any = false;
    for (List<String> row : table.rows) {
      String cell = column < row.size() ? row.get(column) : "";
      if (cell == null || cell.isEmpty()) {
        continue;
      }
      any = true;
      if (!cell.replace(",", "").replace("+", "").matches("-?[0-9.]+%?")) {
        return false;
      }
    }
    return any;
  }

  private static String escape(String text) {
    return text == null ? "" : text.replace("|", "\\|").replace("\n", " ");
  }
}
