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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Pipeline options for the Spanner NodeEmbedding ingestion Dataflow pipeline. */
public interface EmbeddingPipelineOptions extends SpannerPipelineOptions {
  @Description("Classpath resource name to the embedding JSON config.")
  @Default.String("default.json")
  String getEmbeddingConfigPath();

  void setEmbeddingConfigPath(String embeddingConfigPath);

  @Description("Spanner table name for storing node embeddings.")
  @Default.String("NodeEmbedding")
  String getEmbeddingTableName();

  void setEmbeddingTableName(String embeddingTableName);

  @Description("GCP region/location.")
  @Default.String("us-central1")
  String getLocation();

  void setLocation(String location);

  @Description("GCS URI or local path to the NL statistical variable sentences CSV file.")
  @Default.String("gs://datcom-nl-models/base_uae_mem_2025_11_03_07_10_42/embeddings.csv")
  String getNlStatVarFilePath();

  void setNlStatVarFilePath(String nlStatVarFilePath);

  @Description("Batch size for Vertex AI embedding prediction requests.")
  @Default.Integer(100)
  int getEmbeddingBatchSize();

  void setEmbeddingBatchSize(int embeddingBatchSize);

  @Description("Whether to skip pre-deleting existing embeddings in Spanner.")
  @Default.Boolean(false)
  boolean getSkipDelete();

  void setSkipDelete(boolean skipDelete);

  @Description("Whether to ignore IngestionLock timestamp and process all matching nodes.")
  @Default.Boolean(false)
  boolean getIgnoreLockTimestamp();

  void setIgnoreLockTimestamp(boolean ignoreLockTimestamp);

  @Description("Spanner Property Graph name for GQL predicate queries.")
  @Default.String("DCGraph")
  String getGraphName();

  void setGraphName(String graphName);
}
