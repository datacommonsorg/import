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

/** IngestionPipelineOptions interface for defining Spanner ingestion pipeline options. */
public interface IngestionPipelineOptions extends SpannerPipelineOptions {
  @Description("GCS bucket Id for input data")
  @Default.String("datcom-store")
  String getStorageBucketId();

  void setStorageBucketId(String bucketId);

  @Description("Whether to skip transformation step.")
  @Default.Boolean(false)
  boolean getSkipTransformation();

  void setSkipTransformation(boolean skipTransformation);

  @Description("Whether to skip delete operations.")
  @Default.Boolean(false)
  boolean getSkipDelete();

  void setSkipDelete(boolean skipDelete);

  @Description("Whether to force combination of schema nodes across shards.")
  @Default.Boolean(false)
  boolean getForceCombineNodes();

  void setForceCombineNodes(boolean forceCombineNodes);

  @Description("Spanner Node table name")
  @Default.String("Node")
  String getSpannerNodeTableName();

  void setSpannerNodeTableName(String tableName);

  @Description("Spanner Edge table name")
  @Default.String("Edge")
  String getSpannerEdgeTableName();

  void setSpannerEdgeTableName(String tableName);

  @Description("Spanner TimeSeries table name")
  @Default.String("TimeSeries")
  String getSpannerTimeSeriesTableName();

  void setSpannerTimeSeriesTableName(String tableName);

  @Description("Spanner Observation table name")
  @Default.String("Observation")
  String getSpannerObservationTableName();

  void setSpannerObservationTableName(String tableName);
}
