package org.datacommons;

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;

// Helper functions to write data to Spanner tables.
public class SpannerClient {

  public static void WriteObservations(IngestionOptions config, PCollection<Observation> obs) {
    obs.apply(
            "CreateMutation",
            ParDo.of(
                new DoFn<Observation, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Observation o = c.element();
                    c.output(o.toMutation());
                  }
                }))
        .apply(
            "WriteObs",
            SpannerIO.write()
                .withProjectId(config.getProjectId())
                .withInstanceId(config.getSpannerInstanceId())
                .withDatabaseId(config.getSpannerDatabaseId()));
  }

  public static void WriteGraph(IngestionOptions config, PCollection<Entity> entity) {
    SpannerWriteResult result =
        entity
            .apply(
                "CreateMutation",
                ParDo.of(
                    new DoFn<Entity, Mutation>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Entity e = c.element();
                        c.output(e.toNode());
                      }
                    }))
            .apply(
                "WriteNodes",
                SpannerIO.write()
                .withProjectId(config.getProjectId())
                .withInstanceId(config.getSpannerInstanceId())
                .withDatabaseId(config.getSpannerDatabaseId()));

    entity
        .apply(Wait.on(result.getOutput()))
        .apply(
            "CreateMutation",
            ParDo.of(
                new DoFn<Entity, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Entity e = c.element();
                    c.output(e.toEdge());
                  }
                }))
        .apply(
            "WriteEdges",
            SpannerIO.write()
                .withProjectId(config.getProjectId())
                .withInstanceId(config.getSpannerInstanceId())
                .withDatabaseId(config.getSpannerDatabaseId()));
  }
}
