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

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.datacommons.ingestion.pipeline.rollback.SpannerRollbackPipeline;
import org.datacommons.ingestion.spanner.SpannerClient;

/** Entrypoint for Spanner time-travel rollback. */
public class RollbackPipeline implements Serializable {

  /** Builds the Beam execution graph for Spanner time-travel rollback. */
  public static void buildPipeline(
      Pipeline pipeline, IngestionPipelineOptions options, SpannerClient spannerClient) {
    SpannerRollbackPipeline.buildPipeline(pipeline, options, spannerClient);
  }

  public static List<String> resolveTargetProvenances(IngestionPipelineOptions options) {
    return SpannerRollbackPipeline.resolveTargetProvenances(options);
  }
}
