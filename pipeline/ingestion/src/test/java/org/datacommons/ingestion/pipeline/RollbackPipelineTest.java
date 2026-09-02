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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RollbackPipelineTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  public static List<Mutation> capturedMutations = Collections.synchronizedList(new ArrayList<>());

  @Before
  public void setUp() {
    capturedMutations.clear();
  }

  @Test
  public void testResolveTargetProvenances_fromImportList_baseDc() {
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setImportList("CensusACS5YearSurvey,BLS_Data");
    options.setIsBaseDc(true);

    List<String> provenances = RollbackPipeline.resolveTargetProvenances(options);
    assertTrue(provenances.contains("dc/base/CensusACS5YearSurvey"));
    assertTrue(provenances.contains("dc/base/generated/CensusACS5YearSurvey"));
    assertTrue(provenances.contains("dc/base/BLS_Data"));
    assertTrue(provenances.contains("dc/base/generated/BLS_Data"));
    assertEquals(4, provenances.size());
  }

  @Test
  public void testResolveTargetProvenances_fromJsonArray_baseDc() {
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setImportList(
        "[{\"importName\": \"CensusACS5YearSurvey\"}, {\"importName\": \"BLS_Data\"}]");
    options.setIsBaseDc(true);

    List<String> provenances = RollbackPipeline.resolveTargetProvenances(options);
    assertTrue(provenances.contains("dc/base/CensusACS5YearSurvey"));
    assertTrue(provenances.contains("dc/base/generated/CensusACS5YearSurvey"));
    assertTrue(provenances.contains("dc/base/BLS_Data"));
    assertTrue(provenances.contains("dc/base/generated/BLS_Data"));
    assertEquals(4, provenances.size());
  }

  @Test
  public void testMissingRollbackTimestamp_throwsException() {
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setIsRollback(true);
    options.setRollbackTimestamp(null);

    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId("test")
            .spannerInstanceId("test")
            .spannerDatabaseId("test")
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RollbackPipeline.buildPipeline(pipeline, options, spannerClient));

    assertTrue(exception.getMessage().contains("--rollbackTimestamp must be specified"));
  }

  @Test
  public void testResolveTargetProvenances_empty_throwsException() {
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setImportList(null);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> RollbackPipeline.resolveTargetProvenances(options));

    assertTrue(exception.getMessage().contains("--importList must be specified for rollback"));
  }

  @Test
  public void testResolveTargetProvenances_fromImportList_customDc() {
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setImportList("CustomSurvey");
    options.setIsBaseDc(false);

    List<String> provenances = RollbackPipeline.resolveTargetProvenances(options);
    assertTrue(provenances.contains("CustomSurvey"));
    assertTrue(provenances.contains("generated/CustomSurvey"));
    assertEquals(2, provenances.size());
  }

  @Test
  public void testApplyHeadDeletions_skipDeleteTrue_returnsNullSignals() {
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setSkipDelete(true);

    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId("test")
            .spannerInstanceId("test")
            .spannerDatabaseId("test")
            .build();

    var signals =
        org.datacommons.ingestion.pipeline.rollback.SpannerRollbackPipeline.applyHeadDeletions(
            pipeline, options, List.of("dc/base/Test"), spannerClient);

    org.junit.Assert.assertNull(signals.delTsSignal());
    org.junit.Assert.assertNull(signals.delEdgeSignal());
    org.junit.Assert.assertNull(signals.delKvSignal());
  }

  static class MockRollbackSpannerClient extends SpannerClient {
    private final transient SpannerWriteResult mockWriteResult;

    public MockRollbackSpannerClient(SpannerWriteResult mockWriteResult) {
      super(
          SpannerClient.builder()
              .gcpProjectId("test")
              .spannerInstanceId("test")
              .spannerDatabaseId("test"));
      this.mockWriteResult = mockWriteResult;
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
                  capturedMutations.add(m);
                }
              }));
      return mockWriteResult;
    }
  }
}
