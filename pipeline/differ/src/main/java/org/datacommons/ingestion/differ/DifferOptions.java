package org.datacommons.ingestion.differ;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Parameters for the differ pipeline. */
public interface DifferOptions extends PipelineOptions {
  @Description("Path of the current data")
  @Required
  String getCurrentData();

  void setCurrentData(String value);

  @Description("Path of the previous data")
  @Required
  String getPreviousData();

  void setPreviousData(String value);

  @Description("Path of the diff output")
  @Required
  String getOutputLocation();

  void setOutputLocation(String value);

  @Description("Whether to use optimized tfrecord file format")
  @Default.Boolean(false)
  Boolean getUseOptimizedGraphFormat();

  void setUseOptimizedGraphFormat(Boolean value);
}
