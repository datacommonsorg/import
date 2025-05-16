package org.datacommons;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

/** Parameters for the differ pipeline. */
public interface DifferOptions extends PipelineOptions {
  @Description("Path of the current data")
  @Required
  ValueProvider<String> getCurrentData();

  void setCurrentData(ValueProvider<String> value);

  @Description("Path of the previous data")
  @Required
  ValueProvider<String> getPreviousData();

  void setPreviousData(ValueProvider<String> value);

  @Description("Path of the diff output")
  @Required
  ValueProvider<String> getOutputLocation();

  void setOutputLocation(ValueProvider<String> value);

  @Description("Whether to use optimized tfrecord file format")
  ValueProvider<Boolean> getUseOptimizedGraphFormat();

  void setUseOptimizedGraphFormat(ValueProvider<Boolean> value);
}
