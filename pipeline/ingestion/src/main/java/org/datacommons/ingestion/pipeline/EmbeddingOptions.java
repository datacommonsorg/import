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
import org.apache.beam.sdk.options.PipelineOptions;

/** Embedding-specific pipeline options for Spanner NodeEmbedding generation. */
public interface EmbeddingOptions extends PipelineOptions {
  @Description("Classpath resource name to the embedding JSON config.")
  @Default.String("embedding/base_dc.json")
  String getEmbeddingConfigPath();

  void setEmbeddingConfigPath(String embeddingConfigPath);

  @Description("Spanner table name for storing node embeddings.")
  @Default.String("NodeEmbedding")
  String getEmbeddingTableName();

  void setEmbeddingTableName(String embeddingTableName);

  @Description("GCS URI or local path to the NL statistical variable sentences CSV file.")
  String getNlStatVarFilePath();

  void setNlStatVarFilePath(String nlStatVarFilePath);

  @Description("Batch size for Vertex AI embedding prediction requests.")
  @Default.Integer(100)
  int getEmbeddingBatchSize();

  void setEmbeddingBatchSize(int embeddingBatchSize);

  /** TODO: add the IncrementalEmbedding option and related incremental ingestion logic. */
  // @Description("Whether to generate embeddings incrementally based on timestamp.")
  // @Default.Boolean(true)
  // boolean getIncrementalEmbedding();

  // void setIncrementalEmbedding(boolean incrementalEmbedding);
}
