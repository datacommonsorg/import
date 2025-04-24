package org.datacommons.ingestion.pipeline;

import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Pipeline options for the SimpleGraphPipeline. */
public interface SimpleGraphPipelineOptions extends BasePipelineOptions {
  @Description("Import group to be ingested into Spanner.")
  @Default.String("schema")
  String getImportGroupVersion();

  void setImportGroupVersion(String importGroup);

  @Description("List of prefixes of predicates to skip during ingestion.")
  @Default.InstanceFactory(SkipPredicatePrefixesFactory.class)
  List<String> getSkipPredicatePrefixes();

  void setSkipPredicatePrefixes(List<String> skipPredicatePrefixes);

  static class SkipPredicatePrefixesFactory implements DefaultValueFactory<List<String>> {
    @Override
    public List<String> create(PipelineOptions options) {
      return List.of("geoJsonCoordinates", "kmlCoordinates");
    }
  }
}
