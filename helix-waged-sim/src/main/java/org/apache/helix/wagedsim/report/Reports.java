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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Renders the requested report formats for a run folder. */
public final class Reports {
  private Reports() {
  }

  /** @return the files written */
  public static List<Path> render(Path runDir, List<String> formats) throws IOException {
    RunData data = new RunData(runDir);
    ReportContent content = ReportBuilder.build(data);
    List<Path> written = new ArrayList<>();
    for (String format : formats) {
      switch (format.trim().toLowerCase()) {
        case "md":
        case "markdown": {
          Path file = runDir.resolve("report.md");
          Files.write(file, MarkdownReport.render(content).getBytes(StandardCharsets.UTF_8));
          written.add(file);
          break;
        }
        case "html": {
          Path file = runDir.resolve("report.html");
          Files.write(file, HtmlReport.render(content).getBytes(StandardCharsets.UTF_8));
          written.add(file);
          break;
        }
        case "xlsx":
          // Rendered by the skill's script: skill/scripts/render_xlsx.py <run folder>.
          break;
        default:
          throw new IllegalArgumentException("Unknown report format '" + format + "'; use md, html or xlsx");
      }
    }
    return written;
  }
}
