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
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DeleteExistingEmbeddingsFn}. */
@RunWith(JUnit4.class)
public class DeleteExistingEmbeddingsFnTest {

  @Test
  public void testBuildDeleteDml() {
    String sql = DeleteExistingEmbeddingsFn.buildDeleteDml("NodeEmbedding");
    assertEquals(
        "DELETE FROM NodeEmbedding WHERE embedding_label = @embedding_label AND subject_id IN UNNEST(@subject_ids)",
        sql);
    assertTrue(sql.contains("DELETE FROM NodeEmbedding"));
    assertTrue(sql.contains("embedding_label = @embedding_label"));
    assertTrue(sql.contains("subject_id IN UNNEST(@subject_ids)"));
  }
}
