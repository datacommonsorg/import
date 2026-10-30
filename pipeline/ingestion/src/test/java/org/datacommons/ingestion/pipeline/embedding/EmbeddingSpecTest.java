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

package org.datacommons.ingestion.pipeline.embedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link EmbeddingSpec}. */
@RunWith(JUnit4.class)
public class EmbeddingSpecTest {

  @Test
  public void testGetNodeTypeNames_preservesOrder() {
    Map<String, List<String>> nodeTypes = new LinkedHashMap<>();
    nodeTypes.put("StatisticalVariable", List.of("subject_id", "name"));
    nodeTypes.put("Topic", List.of("subject_id", "name"));

    EmbeddingSpec spec =
        new EmbeddingSpec(
            "base_text_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            nodeTypes,
            NodeFilterType.NO_FILTER);

    assertEquals(List.of("StatisticalVariable", "Topic"), spec.getNodeTypeNames());
  }

  @Test
  public void testValidation_invalidFieldsThrowException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                " ",
                "NodeEmbeddingModel",
                "text-embedding-005",
                "RETRIEVAL_QUERY",
                Map.of("Topic", List.of("name")),
                NodeFilterType.NO_FILTER));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                "base_text_embedding",
                "",
                "text-embedding-005",
                "RETRIEVAL_QUERY",
                Map.of("Topic", List.of("name")),
                NodeFilterType.NO_FILTER));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                "base_text_embedding",
                "NodeEmbeddingModel",
                " ",
                "RETRIEVAL_QUERY",
                Map.of("Topic", List.of("name")),
                NodeFilterType.NO_FILTER));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                "base_text_embedding",
                "NodeEmbeddingModel",
                "text-embedding-005",
                "",
                Map.of("Topic", List.of("name")),
                NodeFilterType.NO_FILTER));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                "base_text_embedding",
                "NodeEmbeddingModel",
                "text-embedding-005",
                "RETRIEVAL_QUERY",
                Map.of(),
                NodeFilterType.NO_FILTER));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                "base_text_embedding",
                "NodeEmbeddingModel",
                "text-embedding-005",
                "RETRIEVAL_QUERY",
                Map.of(" ", List.of("name")),
                NodeFilterType.NO_FILTER));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                "base_text_embedding",
                "NodeEmbeddingModel",
                "text-embedding-005",
                "RETRIEVAL_QUERY",
                Map.of("Topic", List.of("name")),
                null));
  }
}
