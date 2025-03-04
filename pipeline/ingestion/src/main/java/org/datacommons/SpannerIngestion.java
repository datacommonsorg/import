package org.datacommons;

import com.google.api.gax.paging.Page;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerIngestion {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerIngestion.class);

  static PCollection<Triple> ReadTripes(Pipeline p) {
    String query = "SELECT * FROM `datcom-store.spanner.triple` LIMIT 100";
    PCollection<Triple> rows =
        p.apply(
                "Read from BigQuery table",
                BigQueryIO.readTableRows()
                    // .from(String.format("%s:%s.%s", "datcom-store", "spanner", "triple"))
                    .fromQuery(query)
                    .usingStandardSql()
                    .withMethod(Method.DIRECT_READ))
            .apply(
                "TableRows to MyData",
                MapElements.into(TypeDescriptor.of(Triple.class)).via(Triple::fromTableRow));
    return rows;
  }

  static void WriteTriples(Pipeline p, PCollection<Triple> triples) {

    triples
        .apply(
            "CreateMutation",
            ParDo.of(
                new DoFn<Triple, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Triple t = c.element();
                    c.output(t.toMutation());
                  }
                }))
        .apply(
            "WriteTriples",
            SpannerIO.write()
                .withProjectId("datcom-store")
                .withInstanceId("dc-kg-test")
                .withDatabaseId("dc_graph"));
  }

  static void WriteObs(Pipeline p, PCollection<Observation> obs) {
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
                .withProjectId("datcom-store")
                .withInstanceId("dc-kg-test")
                .withDatabaseId("dc_graph_2"));
  }

  static void WriteGraph(Pipeline p, PCollection<Entity> entity) {
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
                    .withProjectId("datcom-store")
                    .withInstanceId("dc-kg-test")
                    .withDatabaseId("dc_graph_2"));

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
                .withProjectId("datcom-store")
                .withInstanceId("dc-kg-test")
                .withDatabaseId("dc_graph_2"));
  }

  public interface CustomOptions extends PipelineOptions {
    // @Default.String("gs://datcom-store/frequent_2025_01_03_05_13_53/cache.csv-00037-of-00052")
    @Default.String("gs://datcom-store/schema_2025_01_28_12_19_52/cache.csv-00010-of-00014")
    String getInputFile();

    void setInputFile(String value);
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
    private final String prefix;

    public LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      LOG.info(prefix + c.element());
      c.output(c.element());
    }
  }

  public static String listBlobs(String projectId, String bucketId, String prefix) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

    Page<Blob> blobs =
        storage.list(
            bucketId,
            Storage.BlobListOption.prefix(prefix),
            Storage.BlobListOption.currentDirectory());

    String folder = "";
    for (Blob blob : blobs.iterateAll()) {
      folder = blob.getName();
    }
    return folder;
  }

  public static void main(String[] args) {
    CustomOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
    Pipeline p = Pipeline.create(options);
    String projectId = "datcom-store";
    String bucketId = "datcom-store";
    // PCollection<Triple> rows = TripeRead(p);
    // TripleWrite(p, rows);

    String[] prefixes = {
      // "auto1d",
      // "auto1w",
      // "auto2w"
      // "frequent",
      // "country",
      // "dcbranch",
      // "infrequent",
      // "place",
      "schema"
      // "uscensus"
    };

    // List<PCollection<Observation>> x = new ArrayList<>();
    List<PCollection<Entity>> x = new ArrayList<>();
    for (String prefix : prefixes) {
      String folder = listBlobs(projectId, bucketId, prefix);
      if (!folder.isEmpty()) {
        String gcsPath = String.format("gs://%s/%scache.csv*", bucketId, folder);
        LOGGER.info(gcsPath);
        // PCollection<Observation> result = CacheReader.GetObs(gcsPath, prefix, p);
        PCollection<Entity> result = CacheReader.GetEntities(gcsPath, prefix, p);
        x.add(result);
      }
    }

    PCollectionList<Entity> collections = PCollectionList.of(x);
    PCollection<Entity> merged = collections.apply(Flatten.pCollections());
    PCollection<Long> count = merged.apply("count", Count.globally());
    count.apply(ParDo.of(new LogOutput<>("Total obs count: ")));
    // WriteObs(p, merged);
    WriteGraph(p, merged);
    p.run();
  }
}
