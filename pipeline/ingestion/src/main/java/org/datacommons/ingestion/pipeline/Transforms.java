package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.*;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.data.NodesEdges;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.spanner.SpannerClient;

/** Transforms and DoFns for the ingestion pipeline. */
public class Transforms {

  /**
   * DoFn to convert arc cache rows to Spanner mutation groups. This function creates combined
   * mutation groups for both nodes and edges. DO NOT use this function if you want to write nodes
   * and edges separately.
   */
  static class ArcRowToMutationGroupDoFn extends DoFn<String, MutationGroup> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;

    public ArcRowToMutationGroupDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<MutationGroup> c) {
      if (CacheReader.isArcCacheRow(row)) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        List<Mutation> mutations =
            spannerClient.toGraphMutations(nodesEdges.getNodes(), nodesEdges.getEdges());
        List<MutationGroup> mutationGroups = spannerClient.toGraphMutationGroups(mutations);
        mutationGroups.forEach(c::output);
      }
    }
  }

  /** DoFn to convert obs time series cache rows to Spanner mutation groups. */
  static class ObsTimeSeriesRowToMutationGroupDoFn extends DoFn<String, MutationGroup> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;

    public ObsTimeSeriesRowToMutationGroupDoFn(
        CacheReader cacheReader, SpannerClient spannerClient) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
    }

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<MutationGroup> c) {
      if (CacheReader.isObsTimeSeriesCacheRow(row)) {
        List<Observation> observations = cacheReader.parseTimeSeriesRow(row);
        List<Mutation> mutations =
            observations.stream().map(spannerClient::toObservationMutation).toList();
        List<MutationGroup> mutationGroups = spannerClient.toObservationMutationGroups(mutations);
        mutationGroups.forEach(c::output);
      }
    }
  }

  /**
   * DoFn that outputs observation and graph (nodes and edges) mutations for a single cache row.
   * Observation mutations are output as MutationGroup objects.
   * Graph mutations are output as KV<String, Mutation> pairs,
   * where the key is the subject ID and the value is the mutation.
   */
  static class CacheRowMutationsDoFn extends DoFn<String, KV<String, Mutation>> {
    private final CacheReader cacheReader;
    private final SpannerClient spannerClient;
    private final TupleTag<KV<String, Mutation>> graph;
    private final TupleTag<MutationGroup> observations;

    public CacheRowMutationsDoFn(
        CacheReader cacheReader,
        SpannerClient spannerClient,
        TupleTag<KV<String, Mutation>> graph,
        TupleTag<MutationGroup> observations) {
      this.cacheReader = cacheReader;
      this.spannerClient = spannerClient;
      this.graph = graph;
      this.observations = observations;
    }

    @ProcessElement
    public void processElement(@Element String row, MultiOutputReceiver out) {
      if (CacheReader.isArcCacheRow(row)) {
        NodesEdges nodesEdges = cacheReader.parseArcRow(row);
        spannerClient
            .toGraphKVMutations(nodesEdges.getNodes(), nodesEdges.getEdges())
            .forEach(kv -> out.get(graph).output(kv));
      } else if (CacheReader.isObsTimeSeriesCacheRow(row)) {
        List<Observation> obs = cacheReader.parseTimeSeriesRow(row);
        spannerClient.toObservationMutations(obs).forEach(out.get(observations)::output);
      }
    }
  }

  static class ExtractGraphMutationsDoFn extends DoFn<KV<String, Mutation>, Mutation> {
    private final SpannerClient spannerClient;
    private Set<String> seenNodes = Sets.newConcurrentHashSet();

    public ExtractGraphMutationsDoFn(SpannerClient spannerClient) {
      this.spannerClient = spannerClient;
    }

    @StartBundle
    public void startBundle() {
      this.seenNodes = Sets.newConcurrentHashSet();
    }

    @FinishBundle
    public void finishBundle() {
      this.seenNodes = null;
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

  static void buildImportGroupPipeline(
      Pipeline pipeline,
      String importGroupFilePath,
      CacheReader cacheReader,
      SpannerClient spannerClient) {
    TupleTag<MutationGroup> observations = new TupleTag<>() {};
    TupleTag<KV<String, Mutation>> graph = new TupleTag<>() {};
    CacheRowMutationsDoFn createMutations =
        new CacheRowMutationsDoFn(cacheReader, spannerClient, graph, observations);

    PCollection<String> cacheRows =
        pipeline.apply("ReadFromCache", TextIO.read().from(importGroupFilePath));
    PCollectionTuple mutations =
        cacheRows.apply(
            "CreateMutations",
            ParDo.of(createMutations)
                .withOutputTags(graph, TupleTagList.of(observations)));

    // Write graph to spanner.
    mutations
        .get(graph)
        .apply("RedistributeGraphMutations", Redistribute.byKey())
        .apply("ExtractGraphMutations", ParDo.of(new ExtractGraphMutationsDoFn(spannerClient)))
        .apply("WriteGraphToSpanner", spannerClient.getWriteTransform());

    // Write observations to spanner.
    mutations
        .get(observations)
        .apply("WriteObservationsToSpanner", spannerClient.getWriteGroupedTransform());
  }
}
