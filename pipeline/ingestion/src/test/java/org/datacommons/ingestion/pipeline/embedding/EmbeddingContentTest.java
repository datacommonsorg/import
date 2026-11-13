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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.datacommons.ingestion.spanner.model.NodeEmbeddingRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link EmbeddingContent}. */
@RunWith(JUnit4.class)
public class EmbeddingContentTest {

  @Test
  public void testToNodeEmbeddingRecord() {
    EmbeddingContent content =
        new EmbeddingContent(
            "Count_Person",
            "base_text_embedding",
            "123456789",
            "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
            "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
            List.of("StatisticalVariable"),
            "gemini-embedding-001",
            "RETRIEVAL_DOCUMENT");

    NodeEmbeddingRecord record = content.toNodeEmbeddingRecord(List.of(0.1, 0.2, 0.3));
    assertEquals("Count_Person", record.subjectId());
    assertEquals("base_text_embedding", record.embeddingLabel());
    assertEquals("123456789", record.embeddingContentKey());
    assertEquals(
        "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
        record.embeddingContent());
    assertEquals(List.of("StatisticalVariable"), record.nodeTypes());
    assertEquals(List.of(0.1, 0.2, 0.3), record.embeddings());
  }

  @Test
  public void testNodeTypes_nullDefaultsToEmptyAndDefensiveCopy() {
    EmbeddingContent nullTypesContent =
        new EmbeddingContent(
            "Count_Person",
            "base_text_embedding",
            "123",
            "{}",
            "{}",
            null,
            "text-embedding-005",
            "RETRIEVAL_QUERY");
    assertTrue(nullTypesContent.nodeTypes().isEmpty());

    List<String> mutableTypes = new ArrayList<>(List.of("Topic"));
    EmbeddingContent copiedContent =
        new EmbeddingContent(
            "dc/topic/1",
            "base_text_embedding",
            "456",
            "{}",
            "{}",
            mutableTypes,
            "text-embedding-005",
            "RETRIEVAL_QUERY");
    mutableTypes.add("Mutated");
    assertEquals(List.of("Topic"), copiedContent.nodeTypes());
    assertThrows(
        UnsupportedOperationException.class, () -> copiedContent.nodeTypes().add("Another"));
  }
}
