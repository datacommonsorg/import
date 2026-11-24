// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.ingestion.pipeline;

import java.util.List;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.NodeSubGraph;

/** Test helpers for building {@link NodeSubGraph} fixtures in ingestion pipeline tests. */
public final class GraphIngestionUtil {
  private GraphIngestionUtil() {}

  public static NodeSubGraph subjectNodeSubGraph(
      String subjectId, String name, List<String> types) {
    return subjectNodeSubGraph(subjectId, name, types, List.of());
  }

  public static NodeSubGraph subjectNodeSubGraph(
      String subjectId, String name, List<String> types, List<Edge> edges) {
    return NodeSubGraph.builder()
        .node(Node.builder().subjectId(subjectId).value(subjectId).name(name).types(types).build())
        .edges(edges)
        .build();
  }
}
