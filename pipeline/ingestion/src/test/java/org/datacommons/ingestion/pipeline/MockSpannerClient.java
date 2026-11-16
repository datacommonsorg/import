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

import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.spanner.SpannerClient;

/**
 * Shared test helper extending {@link SpannerClient} that captures written {@link Mutation}s in
 * memory instead of writing to a live Spanner instance.
 */
public class MockSpannerClient extends SpannerClient {
  private static final List<Mutation> CAPTURED_MUTATIONS =
      Collections.synchronizedList(new ArrayList<>());

  private final transient PCollection<Void> deleteSignal;
  private final transient SpannerWriteResult mockWriteResult;

  public MockSpannerClient(SpannerWriteResult mockWriteResult) {
    this(null, mockWriteResult);
  }

  public MockSpannerClient(PCollection<Void> deleteSignal, SpannerWriteResult mockWriteResult) {
    super(
        SpannerClient.builder()
            .gcpProjectId("test")
            .spannerInstanceId("test")
            .spannerDatabaseId("test"));
    this.deleteSignal = deleteSignal;
    this.mockWriteResult = mockWriteResult;
  }

  public static List<Mutation> getCapturedMutations() {
    return CAPTURED_MUTATIONS;
  }

  public static void clearCapturedMutations() {
    CAPTURED_MUTATIONS.clear();
  }

  @Override
  public PCollection<Void> deleteDataForImport(
      Pipeline pipeline, String importName, String tableName, String columnName) {
    return deleteSignal;
  }

  @Override
  public SpannerWriteResult writeMutations(
      Pipeline pipeline, String name, PCollection<Mutation> mutations) {
    mutations.apply(
        "Capture" + name + "Mutations",
        ParDo.of(
            new DoFn<Mutation, Void>() {
              @ProcessElement
              public void processElement(@Element Mutation m) {
                CAPTURED_MUTATIONS.add(m);
              }
            }));
    return mockWriteResult;
  }
}
