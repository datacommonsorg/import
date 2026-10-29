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

import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EmbeddingConfigTest {

  @Test
  public void testParseJson_multipleSpecs() {
    String json =
        """
        {
          "specs": [
            {
              "embedding_label": "base_text_embedding",
              "model_name": "NodeEmbeddingModel",
              "model_endpoint": "text-embedding-005",
              "task_type": "RETRIEVAL_QUERY",
              "node_types": {
                "StatisticalVariable": ["description"],
                "Topic": ["description"]
              },
              "node_filter_type": "NO_FILTER"
            },
            {
              "embedding_label": "type_embedding",
              "model_name": "NodeEmbeddingModel",
              "model_endpoint": "text-embedding-005",
              "task_type": "RETRIEVAL_QUERY",
              "node_types": {
                "Class": ["description"]
              },
              "node_filter_type": "ENTITY_TYPES"
            }
          ]
        }
        """;

    EmbeddingConfig config = EmbeddingConfig.parseJson(json);
    assertEquals(2, config.specs().size());

    EmbeddingSpec baseSpec = config.specs().get(0);
    assertEquals("base_text_embedding", baseSpec.embeddingLabel());
    assertEquals(EmbeddingSpec.NodeFilterType.NO_FILTER, baseSpec.nodeFilterType());
    assertEquals(List.of("StatisticalVariable", "Topic"), baseSpec.getNodeTypeNames());
    assertEquals(List.of("description"), baseSpec.nodeTypes().get("StatisticalVariable"));
    assertEquals(List.of("description"), baseSpec.nodeTypes().get("Topic"));

    EmbeddingSpec typeSpec = config.specs().get(1);
    assertEquals("type_embedding", typeSpec.embeddingLabel());
    assertEquals(EmbeddingSpec.NodeFilterType.ENTITY_TYPES, typeSpec.nodeFilterType());
    assertEquals(List.of("Class"), typeSpec.getNodeTypeNames());
    assertEquals(List.of("description"), typeSpec.nodeTypes().get("Class"));
  }

  @Test
  public void testParseJson_missingFieldsThrowsException() {
    String json =
        """
        {
          "specs": [
            {
              "embedding_label": "type_embedding",
              "node_types": {
                "Class": ["description"]
              },
              "node_filter_type": "ENTITY_TYPES"
            }
          ]
        }
        """;

    assertThrows(RuntimeException.class, () -> EmbeddingConfig.parseJson(json));

    String unknownFilterJson =
        """
        {
          "specs": [
            {
              "embedding_label": "type_embedding",
              "model_name": "NodeEmbeddingModel",
              "model_endpoint": "text-embedding-005",
              "task_type": "RETRIEVAL_QUERY",
              "node_types": {
                "Class": ["description"]
              },
              "node_filter_type": "UNKNOWN_FILTER"
            }
          ]
        }
        """;

    assertThrows(RuntimeException.class, () -> EmbeddingConfig.parseJson(unknownFilterJson));
  }

  @Test
  public void testLoad_fromPackagedResource() {
    EmbeddingConfig config = EmbeddingConfig.load("embedding/default.json");
    assertEquals(2, config.specs().size());
    assertEquals("base_text_embedding", config.specs().get(0).embeddingLabel());
    assertEquals("type_embedding", config.specs().get(1).embeddingLabel());
  }

  @Test
  public void testParseJson_emptyThrowsException() {
    assertThrows(IllegalArgumentException.class, () -> EmbeddingConfig.parseJson(""));
    assertThrows(IllegalArgumentException.class, () -> EmbeddingConfig.load(""));
  }

  @Test
  public void testEmbeddingSpec_validation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                " ",
                "NodeEmbeddingModel",
                "text-embedding-005",
                "RETRIEVAL_QUERY",
                Map.of("Topic", List.of()),
                EmbeddingSpec.NodeFilterType.NO_FILTER));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EmbeddingSpec(
                "base_text_embedding",
                "NodeEmbeddingModel",
                "text-embedding-005",
                "RETRIEVAL_QUERY",
                Map.of(),
                EmbeddingSpec.NodeFilterType.NO_FILTER));
  }
}
