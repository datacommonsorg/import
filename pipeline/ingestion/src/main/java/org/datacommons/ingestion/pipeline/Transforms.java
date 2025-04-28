package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.data.ImportGroupVersions;
import org.datacommons.ingestion.data.NodesEdges;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.spanner.SpannerClient;

/** Transforms and DoFns for the ingestion pipeline. */
public class Transforms {

  /**
   * DoFn that outputs observation and graph (nodes and edges) mutations for a single cache row.
   * Observation mutations are output as MutationGroup objects. Graph mutations are output as
   * KV<String, Mutation> pairs, where the key is the subject ID and the value is the mutation.
   */
  static class CacheRowMutationsDoFn extends DoFn<String, KV<String, Mutation>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final TupleTag<KV<String, Mutation>> graph;
    private final TupleTag<MutationGroup> observations;
    private final boolean skipObservations;
    private final Set<String> seenNodes = Sets.newConcurrentHashSet();

    public CacheRowMutationsDoFn(
        CacheReader cacheReader,
        SpannerClient spannerClient,
        boolean skipObservations,
        TupleTag<KV<String, Mutation>> graph,
        TupleTag<MutationGroup> observations) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipObservations = skipObservations;
      this.graph = graph;
      this.observations = observations;
    }

    @StartBundle
    public void startBundle() {
      this.seenNodes.clear();
    }

    @FinishBundle
    public void finishBundle() {
      this.seenNodes.clear();
    }

    @ProcessElement
    public void processElement(@Element String row, MultiOutputReceiver out) {
      if (CacheReader.isArcCacheRow(row)) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        var kvs = spannerClient.toGraphKVMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        spannerClient
            .filterGraphKVMutations(kvs, seenNodes)
            .forEach(kv -> out.get(graph).output(kv));
      } else if (CacheReader.isObsTimeSeriesCacheRow(row) && !skipObservations) {
        List<Observation> obs = cacheReader.parseTimeSeriesRow(row);
        spannerClient.toObservationMutations(obs).forEach(out.get(observations)::output);
      }
    }
  }

  static class ExtractGraphMutationsDoFn extends DoFn<KV<String, Mutation>, Mutation> {
    private final SpannerClient spannerClient;
    private final Set<String> seenNodes = Sets.newConcurrentHashSet();

    public ExtractGraphMutationsDoFn(SpannerClient spannerClient) {
      this.spannerClient = spannerClient;
    }

    @StartBundle
    public void startBundle() {
      this.seenNodes.clear();
    }

    @FinishBundle
    public void finishBundle() {
      this.seenNodes.clear();
    }

    @ProcessElement
    public void processElement(@Element KV<String, Mutation> kv, OutputReceiver<Mutation> out) {
      var mutation = kv.getValue();
      var subjectId = SpannerClient.getSubjectId(mutation);
      if (mutation.getTable().equals(spannerClient.getNodeTableName())) {
        if (seenNodes.contains(subjectId)) {
          return;
        }
        seenNodes.add(subjectId);
      }
      out.output(mutation);
    }
  }

  static class ImportGroupTransform extends PTransform<PCollection<String>, PCollection<Void>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final boolean skipObservations;

    public ImportGroupTransform(
        CacheReader cacheReader, SpannerClient spannerClient, boolean skipObservations) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipObservations = skipObservations;
    }

    @Override
    public PCollection<Void> expand(PCollection<String> cacheRows) {
      TupleTag<MutationGroup> observations = new TupleTag<>() {};
      TupleTag<KV<String, Mutation>> graph = new TupleTag<>() {};
      CacheRowMutationsDoFn createMutations =
          new CacheRowMutationsDoFn(
              cacheReader, spannerClient, skipObservations, graph, observations);

      PCollectionTuple mutations =
          cacheRows.apply(
              "CreateMutations",
              ParDo.of(createMutations).withOutputTags(graph, TupleTagList.of(observations)));

      // Write graph to spanner.
      var graphResult =
          mutations
              .get(graph)
              .apply("RedistributeGraphMutations", Redistribute.byKey())
              .apply(
                  "ExtractGraphMutations", ParDo.of(new ExtractGraphMutationsDoFn(spannerClient)))
              .apply("WriteGraphToSpanner", spannerClient.getWriteTransform());

      // Write observations to spanner.
      SpannerWriteResult obsResult = null;
      if (!skipObservations) {
        obsResult =
            mutations
                .get(observations)
                .apply("WriteObservationsToSpanner", spannerClient.getWriteGroupedTransform());
      }

      var writeResults = PCollectionList.of(graphResult.getOutput());
      if (obsResult != null) {
        writeResults.and(obsResult.getOutput());
      }

      return writeResults.apply("Done", Flatten.pCollections());
    }
  }

  static void buildImportGroupPipeline(
      Pipeline pipeline,
      String importGroupVersion,
      CacheReader cacheReader,
      SpannerClient spannerClient) {
    var options = pipeline.getOptions().as(IngestionPipelineOptions.class);
    var importGroupName = ImportGroupVersions.getImportGroupName(importGroupVersion);
    var importGroupFilePath = cacheReader.getImportGroupCachePath(importGroupVersion);
    pipeline
        .apply("Read: " + importGroupName, TextIO.read().from(importGroupFilePath))
        .apply(
            "Ingest: " + importGroupName,
            new ImportGroupTransform(cacheReader, spannerClient, options.getSkipObservations()));
  }

  static void buildIngestionPipeline(
      Pipeline pipeline,
      List<String> importGroupVersions,
      CacheReader cacheReader,
      SpannerClient spannerClient) {
    for (var importGroupVersion : importGroupVersions) {
      buildImportGroupPipeline(pipeline, importGroupVersion, cacheReader, spannerClient);
    }
  }
}
