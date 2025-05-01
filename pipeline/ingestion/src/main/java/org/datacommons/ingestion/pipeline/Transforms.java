package org.datacommons.ingestion.pipeline;

import static org.datacommons.ingestion.pipeline.SkipProcessing.SKIP_GRAPH;
import static org.datacommons.ingestion.pipeline.SkipProcessing.SKIP_OBS;

import com.google.cloud.spanner.Mutation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.data.ImportGroupVersions;
import org.datacommons.ingestion.data.NodesEdges;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transforms and DoFns for the ingestion pipeline. */
public class Transforms {

  /**
   * DoFn that outputs observation and graph (nodes and edges) mutations for a single cache row.
   * Both are output as KV<String, Mutation> objects. Observation mutations are keyed by variable.
   * Graph mutations are keyed by subject ID.
   */
  static class CacheRowMutationsDoFn extends DoFn<String, KV<String, Mutation>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final TupleTag<KV<String, Mutation>> graph;
    private final TupleTag<Mutation> observations;
    private final SkipProcessing skipProcessing;
    private final Set<String> seenNodes = new HashSet<>();

    private CacheRowMutationsDoFn(
        CacheReader cacheReader,
        SpannerClient spannerClient,
        SkipProcessing skipProcessing,
        TupleTag<KV<String, Mutation>> graph,
        TupleTag<Mutation> observations) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipProcessing = skipProcessing;
      this.graph = graph;
      this.observations = observations;
    }

    @StartBundle
    public void startBundle() {
      seenNodes.clear();
    }

    @FinishBundle
    public void finishBundle() {
      seenNodes.clear();
    }

    @ProcessElement
    public void processElement(@Element String row, MultiOutputReceiver out) {
      if (CacheReader.isArcCacheRow(row) && skipProcessing != SKIP_GRAPH) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        var kvs = spannerClient.toGraphKVMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        spannerClient.filterGraphKVMutations(kvs, seenNodes).forEach(out.get(graph)::output);
      } else if (CacheReader.isObsTimeSeriesCacheRow(row) && skipProcessing != SKIP_OBS) {
        var obs = cacheReader.parseTimeSeriesRow(row);
        spannerClient.toObservationMutations(obs).forEach(out.get(observations)::output);
      }
    }
  }

  static class CacheRowKVMutationsDoFn extends DoFn<String, KV<String, Mutation>> {
    private static final Counter DUPLICATE_OBS_COUNTER =
        Metrics.counter(CacheRowKVMutationsDoFn.class, "dc_duplicate_obs_creation");
    private static final Counter DUPLICATE_NODES_COUNTER =
        Metrics.counter(CacheRowKVMutationsDoFn.class, "dc_duplicate_nodes_creation");

    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final SkipProcessing skipProcessing;
    private final Set<String> seenNodes = new HashSet<>();
    private final Set<String> seenObs = new HashSet<>();

    private CacheRowKVMutationsDoFn(
        CacheReader cacheReader, SpannerClient spannerClient, SkipProcessing skipProcessing) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipProcessing = skipProcessing;
    }

    @StartBundle
    public void startBundle() {
      seenNodes.clear();
      seenObs.clear();
    }

    @FinishBundle
    public void finishBundle() {
      seenNodes.clear();
      seenObs.clear();
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<KV<String, Mutation>> out) {
      if (CacheReader.isArcCacheRow(row) && skipProcessing != SKIP_GRAPH) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        var kvs = spannerClient.toGraphKVMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        var filtered = spannerClient.filterGraphKVMutations(kvs, seenNodes);
        filtered.forEach(out::output);

        var dups = kvs.size() - filtered.size();
        if (dups > 0) {
          DUPLICATE_NODES_COUNTER.inc(dups);
        }
      } else if (CacheReader.isObsTimeSeriesCacheRow(row) && skipProcessing != SKIP_OBS) {
        var obs = cacheReader.parseTimeSeriesRow(row);
        var kvs = spannerClient.toObservationKVMutations(obs);
        var filtered = spannerClient.filterObservationKVMutations(kvs, seenObs);
        filtered.forEach(out::output);

        var dups = kvs.size() - filtered.size();
        if (dups > 0) {
          DUPLICATE_OBS_COUNTER.inc(dups);
        }
      }
    }
  }

  static class ExtractKVMutationsDoFn extends DoFn<KV<String, Iterable<Mutation>>, Mutation> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractKVMutationsDoFn.class);
    private static final Counter DUPLICATE_OBS_COUNTER =
        Metrics.counter(ExtractKVMutationsDoFn.class, "dc_duplicate_obs_extraction");
    private static final Counter DUPLICATE_NODES_COUNTER =
        Metrics.counter(ExtractKVMutationsDoFn.class, "dc_duplicate_nodes_extraction");

    private final SpannerClient spannerClient;
    private final Set<String> seenNodes = new HashSet<>();
    private final Set<String> seenObs = new HashSet<>();

    public ExtractKVMutationsDoFn(SpannerClient spannerClient) {
      this.spannerClient = spannerClient;
    }

    @StartBundle
    public void startBundle() {
      seenNodes.clear();
      seenObs.clear();
    }

    @FinishBundle
    public void finishBundle() {
      seenNodes.clear();
      seenObs.clear();
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<Mutation>> kv, OutputReceiver<Mutation> out) {
      for (var mutation : kv.getValue()) {
        if (mutation.getTable().equals(spannerClient.getNodeTableName())) {
          var subjectId = SpannerClient.getSubjectId(mutation);
          if (seenNodes.contains(subjectId)) {
            LOGGER.info("Duplicate node (extraction): {}", subjectId);
            DUPLICATE_NODES_COUNTER.inc();
            return;
          }
          seenNodes.add(subjectId);
        } else if (mutation.getTable().equals(spannerClient.getObservationTableName())) {
          var key = SpannerClient.getFullObservationKey(mutation);
          if (seenObs.contains(key)) {
            LOGGER.info("Duplicate observation (extraction): {}", key);
            DUPLICATE_OBS_COUNTER.inc();
            return;
          }
          seenObs.add(key);
        }
        out.output(mutation);
      }
    }
  }

  static class ExtractGraphMutationsDoFn extends DoFn<KV<String, Mutation>, Mutation> {
    private final SpannerClient spannerClient;
    private final Set<String> seenNodes = new HashSet<>();

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

  static class ExtractMutationsDoFn extends DoFn<KV<String, Iterable<Mutation>>, Mutation> {
    private final SpannerClient spannerClient;
    private final Set<String> seenNodes = new HashSet<>();

    public ExtractMutationsDoFn(SpannerClient spannerClient) {
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
    public void processElement(
        @Element KV<String, Iterable<Mutation>> kvs, OutputReceiver<Mutation> out) {
      kvs.getValue()
          .forEach(
              mutation -> {
                if (mutation.getTable().equals(spannerClient.getNodeTableName())) {
                  var subjectId = SpannerClient.getSubjectId(mutation);
                  if (seenNodes.contains(subjectId)) {
                    return;
                  }
                  seenNodes.add(subjectId);
                }
                out.output(mutation);
              });
    }
  }

  static class ImportGroupTransform extends PTransform<PCollection<String>, PCollection<Void>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final SkipProcessing skipProcessing;

    public ImportGroupTransform(
        CacheReader cacheReader, SpannerClient spannerClient, SkipProcessing skipProcessing) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.skipProcessing = skipProcessing;
    }

    @Override
    public PCollection<Void> expand(PCollection<String> cacheRows) {
      return groupBy(cacheRows);
    }

    private PCollection<Void> groupBy(PCollection<String> cacheRows) {
      var kvs =
          cacheRows.apply(
              "CreateMutations",
              ParDo.of(new CacheRowKVMutationsDoFn(cacheReader, spannerClient, skipProcessing)));
      var grouped = kvs.apply("GroupMutations", GroupByKey.create());
      var mutations =
          grouped.apply("ExtractMutations", ParDo.of(new ExtractKVMutationsDoFn(spannerClient)));
      var write = mutations.apply("WriteToSpanner", spannerClient.getWriteTransform());
      return write.getOutput();
    }

    private PCollection<Void> separateMutations(PCollection<String> cacheRows) {
      TupleTag<Mutation> observations = new TupleTag<>() {};
      TupleTag<KV<String, Mutation>> graph = new TupleTag<>() {};
      CacheRowMutationsDoFn createMutations =
          new CacheRowMutationsDoFn(
              cacheReader, spannerClient, skipProcessing, graph, observations);

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
      var obsResult =
          mutations.get(observations).apply("WriteObsToSpanner", spannerClient.getWriteTransform());

      var writeResults = PCollectionList.of(graphResult.getOutput()).and(obsResult.getOutput());

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
            new Transforms.ImportGroupTransform(
                cacheReader, spannerClient, options.getSkipProcessing()));
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
