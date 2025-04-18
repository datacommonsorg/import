package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.data.NodesEdges;
import org.datacommons.ingestion.spanner.SpannerClient;

/** Transforms and DoFns for the ingestion pipeline. */
public class Transforms {

  /**
   * DoFn to convert arc cache rows to Spanner mutation groups. This function creates combined
   * mutation groups for both nodes and edges. DO NOT use this function if you want to write nodes
   * and edges separately.
   */
  public static class ArcRowToMutationGroupDoFn extends DoFn<String, MutationGroup> {
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
}
