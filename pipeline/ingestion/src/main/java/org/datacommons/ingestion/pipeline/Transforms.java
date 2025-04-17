package org.datacommons.ingestion.pipeline;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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

    public static class GraphKeyDoFn extends DoFn<Mutation, KV<String, Mutation>> {
        @ProcessElement
        public void processElement(@Element Mutation mutation, OutputReceiver<KV<String, Mutation>> c) {
            String key = mutation.asMap().get("subject_id").getString();
            c.output(KV.of(key, mutation));
        }
    }

    /**
     * PTransform to group mutations by their subject_id.
     * This is used to ensure that all mutations for a given subject_id are written
     * together.
     */

    public static class GraphMutationGroupTransform
            extends PTransform<PCollection<Mutation>, PCollection<Mutation>> {
        @Override
        public PCollection<Mutation> expand(PCollection<Mutation> mutations) {
            return mutations.apply("AssignKey", ParDo.of(new GraphKeyDoFn()))
                    .apply("GroupByKey", GroupByKey.create())
                    .apply("SortMutations", ParDo.of(new DoFn<KV<String, Iterable<Mutation>>, Mutation>() {
                        @ProcessElement
                        public void processElement(@Element KV<String, Iterable<Mutation>> element,
                                OutputReceiver<Mutation> c) {
                            for (Mutation mutation : element.getValue()) {
                                c.output(mutation);
                            }
                        }
                    }));
        }
    }
}
