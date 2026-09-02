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

package org.datacommons.ingestion.pipeline.rollback;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReconcileNodeEmbeddingsFnTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final List<Mutation> outputs = Collections.synchronizedList(new ArrayList<>());

  @Before
  public void setUp() {
    outputs.clear();
  }

  @Test
  public void testNodeEmbeddingTableConstant() {
    assertEquals("NodeEmbedding", ReconcileNodeEmbeddingsFn.NODE_EMBEDDING_TABLE);
  }

  @Test
  public void testProcessElement_withEmptyBatch_outputsNothing() {
    SpannerClient client =
        SpannerClient.builder()
            .gcpProjectId("test-project")
            .spannerInstanceId("test-instance")
            .spannerDatabaseId("test-db")
            .build();

    ReconcileNodeEmbeddingsFn fn =
        new ReconcileNodeEmbeddingsFn(client, Timestamp.parseTimestamp("2026-01-01T00:00:00Z"));

    List<List<String>> input = List.of(Collections.emptyList());

    pipeline
        .apply(
            Create.of(input)
                .withCoder(
                    org.apache.beam.sdk.coders.ListCoder.of(
                        org.apache.beam.sdk.coders.StringUtf8Coder.of())))
        .apply(ParDo.of(fn))
        .apply(
            ParDo.of(
                new DoFn<Mutation, Void>() {
                  @ProcessElement
                  public void processElement(@Element Mutation m) {
                    outputs.add(m);
                  }
                }));

    pipeline.run().waitUntilFinish();

    assertEquals(0, outputs.size());
  }
}
