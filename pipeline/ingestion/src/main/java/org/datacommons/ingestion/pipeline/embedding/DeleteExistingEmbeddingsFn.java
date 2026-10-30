// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.ingestion.pipeline.embedding;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes existing embeddings in Spanner for a target {@code embedding_label} and candidate {@code
 * subject_id}s in batches of {@link #DELETE_BATCH_SIZE} using Partitioned DML.
 */
public class DeleteExistingEmbeddingsFn extends DoFn<Iterable<String>, Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteExistingEmbeddingsFn.class);
  public static final int DELETE_BATCH_SIZE = 1000;
  private static final int DELETE_SHARDS = 32;

  private final SpannerClient spannerClient;
  private final String embeddingTable;
  private final String embeddingLabel;
  private final Counter deletedRowsCounter;
  private transient Spanner spanner;
  private transient DatabaseClient dbClient;

  public DeleteExistingEmbeddingsFn(
      SpannerClient spannerClient, String embeddingTable, String embeddingLabel) {
    this.spannerClient = spannerClient;
    this.embeddingTable = embeddingTable;
    this.embeddingLabel = embeddingLabel;
    this.deletedRowsCounter =
        Metrics.counter(
            DeleteExistingEmbeddingsFn.class, "embedding_deleted_rows:" + embeddingLabel);
  }

  /**
   * Step 2 of {@link EmbeddingPipeline}: batches candidate {@code subject_id}s and deletes their
   * existing rows in {@code embeddingTable} for {@code embeddingLabel}, returning a completion
   * signal {@link PCollection} for {@code Wait.on}.
   */
  public static PCollection<Void> deleteExistingEmbeddings(
      PCollection<NodeRecord> candidateNodes,
      SpannerClient spannerClient,
      String embeddingTable,
      String embeddingLabel) {
    return candidateNodes
        .apply(
            "KeyCandidateIdsForDelete_" + embeddingLabel,
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                .via(
                    n ->
                        KV.of(
                            Math.floorMod(n.subjectId().hashCode(), DELETE_SHARDS), n.subjectId())))
        .apply(
            "BatchCandidateIdsForDelete_" + embeddingLabel,
            GroupIntoBatches.ofSize(DELETE_BATCH_SIZE))
        .apply("ExtractCandidateIdBatches_" + embeddingLabel, Values.create())
        .apply(
            "DeleteExistingEmbeddings_" + embeddingLabel,
            ParDo.of(
                new DeleteExistingEmbeddingsFn(spannerClient, embeddingTable, embeddingLabel)));
  }

  @Setup
  public void setup() {
    this.spanner = spannerClient.createSpanner();
    this.dbClient = spannerClient.getDatabaseClient(spanner);
  }

  @Teardown
  public void teardown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  @ProcessElement
  public void processElement(@Element Iterable<String> subjectIds, OutputReceiver<Void> receiver) {
    if (subjectIds == null) {
      receiver.output(null);
      return;
    }

    Set<String> uniqueIds = new LinkedHashSet<>();
    for (String id : subjectIds) {
      if (id != null && !id.isEmpty()) {
        uniqueIds.add(id);
      }
    }

    if (uniqueIds.isEmpty()) {
      receiver.output(null);
      return;
    }

    String deleteSql = buildDeleteDml(embeddingTable);
    long totalDeleted = 0L;
    for (List<String> batch : Lists.partition(new ArrayList<>(uniqueIds), DELETE_BATCH_SIZE)) {
      Statement stmt =
          Statement.newBuilder(deleteSql)
              .bind("embedding_label")
              .to(embeddingLabel)
              .bind("subject_ids")
              .toStringArray(batch)
              .build();
      try {
        long deleted = dbClient.executePartitionedUpdate(stmt);
        totalDeleted += deleted;
        deletedRowsCounter.inc(deleted);
      } catch (SpannerException e) {
        throw new IllegalStateException(
            String.format(
                "Failed to delete existing embeddings in '%s' for label '%s'",
                embeddingTable, embeddingLabel),
            e);
      }
    }

    LOGGER.info(
        "Deleted {} existing embedding rows in {} for {} nodes (label: '{}').",
        totalDeleted,
        embeddingTable,
        uniqueIds.size(),
        embeddingLabel);
    receiver.output(null);
  }

  public static String buildDeleteDml(String embeddingTable) {
    return String.format(
        "DELETE FROM %s WHERE embedding_label = @embedding_label AND subject_id IN UNNEST(@subject_ids)",
        embeddingTable);
  }
}
