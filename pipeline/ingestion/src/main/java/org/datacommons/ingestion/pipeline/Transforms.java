package org.datacommons.ingestion.pipeline;

import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.data.NodesEdges;
import org.datacommons.ingestion.spanner.SpannerClient;

import com.google.cloud.spanner.Mutation;

/**
 * Transforms and DoFns for the ingestion pipeline.
 */
public class Transforms {

    /**
     * DoFn to convert arc cache rows to Spanner mutations.
     * This function creates combined mutations for nodes and edges.
     * DO NOT use this function if you want to write nodes and edges separately.
     */
    public static class ArcRowToMutationDoFn extends DoFn<String, Mutation> {
        private final CacheReader cacheReader;
        private final SpannerClient spannerClient;

        public ArcRowToMutationDoFn(CacheReader cacheReader, SpannerClient spannerClient) {
            this.cacheReader = cacheReader;
            this.spannerClient = spannerClient;
        }

        @ProcessElement
        public void processElement(@Element String row, OutputReceiver<Mutation> c) {
            if (CacheReader.isArcCacheRow(row)) {
                NodesEdges nodesEdges = cacheReader.parseArcRow(row);
                spannerClient.toGraphMutations(nodesEdges.getNodes(), nodesEdges.getEdges())
                        .forEach(c::output);
            }
        }
    }
}
