package org.datacommons.ingestion.missingnodes;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Parameters for finding Edge dcids that do not have Node rows. */
public interface MissingEdgeNodesOptions extends PipelineOptions {
  @Description("GCP project id containing the Spanner database")
  @Required
  String getSpannerProjectId();

  void setSpannerProjectId(String value);

  @Description("Spanner instance id")
  @Required
  String getSpannerInstanceId();

  void setSpannerInstanceId(String value);

  @Description("Spanner database id")
  @Required
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String value);

  @Description("GCS output folder for missing dcid and provisional MCF shards")
  @Required
  String getOutputLocation();

  void setOutputLocation(String value);

  @Description("Whether to write deduplicated Edge column audit files")
  @Default.Boolean(false)
  boolean getWriteDedupedInputs();

  void setWriteDedupedInputs(boolean value);
}
