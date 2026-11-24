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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.EdgeRecord;

/**
 * Shared test helper extending {@link SpannerClient} that captures written {@link Mutation}s in
 * memory instead of writing to a live Spanner instance, and optionally stubs provenance-scoped
 * {@code Edge} reads via {@link #getReadTransform()}.
 */
public class MockSpannerClient extends SpannerClient {
  private static final List<Mutation> CAPTURED_MUTATIONS =
      Collections.synchronizedList(new ArrayList<>());
  private static final List<String> CAPTURED_STAGE_EVENTS =
      Collections.synchronizedList(new ArrayList<>());

  private final transient PCollection<Void> deleteSignal;
  private final transient SpannerWriteResult mockWriteResult;
  private final transient Map<String, List<String>> edgeSubjectIdsByProvenance;

  public MockSpannerClient(SpannerWriteResult mockWriteResult) {
    this(null, mockWriteResult, null);
  }

  public MockSpannerClient(PCollection<Void> deleteSignal, SpannerWriteResult mockWriteResult) {
    this(deleteSignal, mockWriteResult, null);
  }

  /**
   * Creates a client stubbing {@code Edge} provenance reads from {@code edgeSubjectIdsByProvenance}
   * and returning live per-stage {@link SpannerWriteResult} completion signals for {@code Wait.on}.
   */
  public MockSpannerClient(Map<String, List<String>> edgeSubjectIdsByProvenance) {
    this(null, null, edgeSubjectIdsByProvenance);
  }

  private MockSpannerClient(
      PCollection<Void> deleteSignal,
      SpannerWriteResult mockWriteResult,
      Map<String, List<String>> edgeSubjectIdsByProvenance) {
    super(
        SpannerClient.builder()
            .gcpProjectId("test")
            .spannerInstanceId("test")
            .spannerDatabaseId("test"));
    this.deleteSignal = deleteSignal;
    this.mockWriteResult = mockWriteResult;
    this.edgeSubjectIdsByProvenance = edgeSubjectIdsByProvenance;
  }

  public static List<Mutation> getCapturedMutations() {
    return CAPTURED_MUTATIONS;
  }

  public static List<String> getCapturedStageEvents() {
    return CAPTURED_STAGE_EVENTS;
  }

  public static void clearCapturedMutations() {
    CAPTURED_MUTATIONS.clear();
    CAPTURED_STAGE_EVENTS.clear();
  }

  @Override
  public PCollection<Void> deleteDataForImport(
      Pipeline pipeline, String importName, String tableName, String columnName) {
    return deleteSignal;
  }

  @Override
  public SpannerIO.Read getReadTransform() {
    if (edgeSubjectIdsByProvenance == null) {
      return super.getReadTransform();
    }
    SpannerIO.Read baseRead = mock(SpannerIO.Read.class);
    when(baseRead.withQuery(any(Statement.class)))
        .thenAnswer(
            inv -> {
              Statement stmt = inv.getArgument(0);
              String provenance = stmt.getParameters().get(EdgeRecord.COL_PROVENANCE).getString();
              List<Struct> structs =
                  edgeSubjectIdsByProvenance.getOrDefault(provenance, List.of()).stream()
                      .map(id -> Struct.newBuilder().set(EdgeRecord.COL_SUBJECT_ID).to(id).build())
                      .collect(Collectors.toList());
              SpannerIO.Read queriedRead = mock(SpannerIO.Read.class);
              when(queriedRead.getResourceHints()).thenReturn(ResourceHints.create());
              when(queriedRead.getAdditionalInputs()).thenReturn(Map.of());
              when(queriedRead.expand(any(PBegin.class)))
                  .thenAnswer(
                      expInv -> {
                        PBegin input = expInv.getArgument(0);
                        return input.apply(
                            "EmitEdgeSubjectIds",
                            Create.of(structs).withCoder(SerializableCoder.of(Struct.class)));
                      });
              return queriedRead;
            });
    return baseRead;
  }

  @Override
  public SpannerWriteResult writeMutations(
      Pipeline pipeline, String name, PCollection<Mutation> mutations) {
    PCollection<Void> done =
        mutations.apply(
            "Capture" + name + "Mutations",
            ParDo.of(
                new DoFn<Mutation, Void>() {
                  @ProcessElement
                  public void processElement(@Element Mutation m) {
                    CAPTURED_MUTATIONS.add(m);
                    CAPTURED_STAGE_EVENTS.add(name);
                  }
                }));
    if (mockWriteResult != null) {
      return mockWriteResult;
    }
    SpannerWriteResult result = mock(SpannerWriteResult.class, withSettings().serializable());
    when(result.getOutput()).thenReturn(done);
    return result;
  }
}
