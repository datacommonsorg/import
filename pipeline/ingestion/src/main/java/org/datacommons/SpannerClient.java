package org.datacommons;

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;

/** Helper functions to write data to Spanner tables. */
public class SpannerClient {

  /**
   * Writes a PCollection of observations to Spanner Database
   *
   * @param config Ingestion configuration for Spanner DB
   * @param obs    PCollection of observations to be written
   */
  public static void WriteObservations(IngestionOptions config, PCollection<Observation> obs) {
    obs.apply(
        "CreateObsMutation",
        ParDo.of(
            new DoFn<Observation, Mutation>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                IngestionOptions options = c.getPipelineOptions().as(IngestionOptions.class);
                c.output(c.element().toMutation(options.getSpannerObservationTableName()));
              }
            }))
        .apply(
            "WriteObsToSpanner",
            SpannerIO.write()
                .withProjectId(config.getProjectId())
                .withInstanceId(config.getSpannerInstanceId())
                .withDatabaseId(config.getSpannerDatabaseId()));
  }

  /**
   * Writes a PCollection of entities to Spanner Graph Database
   *
   * @param config Ingestion configuration for Spanner DB
   * @param obs    PCollection of entities to be written
   */
  public static void WriteGraph(IngestionOptions config, PCollection<Entity> entity) {
    SpannerWriteResult result = entity
        .apply(
            "CreateNodeMutation",
            ParDo.of(
                new DoFn<Entity, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    IngestionOptions options = c.getPipelineOptions().as(IngestionOptions.class);
                    c.output(c.element().toNode(options.getSpannerNodeTableName()));
                  }
                }))
        .apply("DistinctNodeMutations",
            Distinct.withRepresentativeValueFn(new SerializableFunction<Mutation, String>() {
              @Override
              public String apply(Mutation input) {
                return input.asMap().get("subject_id").getString();
              }
            }))
        .apply(
            "WriteNodesToSpanner",
            SpannerIO.write()
                .withProjectId(config.getProjectId())
                .withInstanceId(config.getSpannerInstanceId())
                .withDatabaseId(config.getSpannerDatabaseId()));

    // Wait for the node table to be written before writing Edge table due to
    // interleaving.
    entity
        .apply(Wait.on(result.getOutput()))
        .apply(
            "CreateEdgeMutation",
            ParDo.of(
                new DoFn<Entity, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    IngestionOptions options = c.getPipelineOptions().as(IngestionOptions.class);
                    c.output(c.element().toEdge(options.getSpannerEdgeTableName()));
                  }
                }))
        .apply(
            "WriteEdgesToSpanner",
            SpannerIO.write()
                .withProjectId(config.getProjectId())
                .withInstanceId(config.getSpannerInstanceId())
                .withDatabaseId(config.getSpannerDatabaseId()));
  }
}
