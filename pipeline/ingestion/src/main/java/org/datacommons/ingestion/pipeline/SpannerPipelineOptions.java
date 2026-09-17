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

/** Shared Spanner and common execution options for Data Commons pipelines. */
public interface SpannerPipelineOptions extends PipelineOptions {
  @Description("List of imports for ingestion/rollback (JSON array)")
  String getImportList();

  void setImportList(String importList);

  @Description("GCP project id")
  @Default.String("datcom-store")
  String getProjectId();

  void setProjectId(String projectId);

  @Description("Spanner Instance Id for output")
  @Default.String("dc-kg-test")
  String getSpannerInstanceId();

  void setSpannerInstanceId(String instanceId);

  @Description("Spanner Database Id for output")
  @Default.String("dc_graph_5")
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String databaseId);

  @Description("Whether this is a base Data Commons run")
  @Default.Boolean(true)
  boolean getIsBaseDc();

  void setIsBaseDc(boolean isBaseDc);

  @Description("The number of shards to generate for writing mutations.")
  @Default.Integer(1)
  int getNumShards();

  void setNumShards(int numShards);

  @Description("Local Spanner emulator host override (e.g. localhost:15000)")
  String getEmulatorHost();

  void setEmulatorHost(String emulatorHost);
}
