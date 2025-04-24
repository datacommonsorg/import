package org.datacommons.ingestion.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Pipeline options for the SimpleObservationsPipeline. */
public interface SimpleObservationsPipelineOptions extends BasePipelineOptions {
  @Description("Import group to be ingested into Spanner.")
  @Default.String("frequent")
  String getImportGroupVersion();

  void setImportGroupVersion(String importGroup);
}
