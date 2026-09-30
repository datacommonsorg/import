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

package org.datacommons.ingestion.pipeline.rollback;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.Lists;
import com.google.gson.JsonParser;
import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.data.ProvenanceUtils;
import org.datacommons.ingestion.pipeline.RollbackPipelineOptions;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.EdgeRecord;
import org.datacommons.ingestion.spanner.model.KeyValueStoreRecord;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.datacommons.ingestion.spanner.model.TimeSeriesRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Director orchestrating Cloud Spanner time-travel rollback for corrupted imports.
 *
 * <p>Decomposes the rollback workflow into four referentially-safe, concurrent phases:
 *
 * <ul>
 *   <li>Phase 1: Partitioned Deletions at HEAD
 *   <li>Phase 2: Parallel Historical Snapshot Reads at T_pre
 *   <li>Phase 3: Shared Table Reconciliation (Node & NodeEmbedding)
 *   <li>Phase 4: Parallel Multi-Track Referential Integrity Write DAG
 * </ul>
 */
public class SpannerRollbackPipeline implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerRollbackPipeline.class);
  private static final long NODE_RECONCILE_BATCH_SIZE = 1000L;

  /** Parent TimeSeries keys looked up per Observation prefix read. */
  private static final long OBSERVATION_PARENT_KEY_BATCH_SIZE = 1000L;

  /** Shard count used to spread Observation parent-key batches across workers. */
  private static final int OBSERVATION_PARENT_KEY_SHARDS = 100;

  private static final String HISTORICAL_PROVENANCE_QUERY_TEMPLATE =
      "SELECT %s FROM %s WHERE provenance IN UNNEST(@provenances)";
  private static final String HISTORICAL_KV_QUERY_TEMPLATE =
      "SELECT %s FROM KeyValueStore WHERE type = 'ProvenanceSummary' AND provenance IN UNNEST(@provenances)";
  private static final String MODIFIED_NODES_QUERY =
      "SELECT subject_id FROM Node WHERE last_update_timestamp >= @tPre";

  public static final String TABLE_NODE = "Node";
  public static final String TABLE_EDGE = "Edge";
  public static final String TABLE_TIMESERIES = "TimeSeries";
  public static final String TABLE_OBSERVATION = "Observation";
  public static final String TABLE_KEY_VALUE_STORE = "KeyValueStore";

  public static final TupleTag<Mutation> RESTORE_NODES_TAG = ReconcileNodesFn.RESTORE_NODES_TAG;
  public static final TupleTag<Mutation> DELETE_NODES_TAG = ReconcileNodesFn.DELETE_NODES_TAG;
  public static final TupleTag<List<String>> RESTORED_NODE_IDS_TAG =
      ReconcileNodesFn.RESTORED_NODE_IDS_TAG;

  public record DeletionSignals(
      PCollection<Void> delTsSignal,
      PCollection<Void> delEdgeSignal,
      PCollection<Void> delKvSignal) {}

  public record HistoricalSnapshots(
      PCollection<Mutation> timeSeriesMutations,
      PCollection<Mutation> observationMutations,
      PCollection<Mutation> edgeMutations,
      PCollection<Mutation> keyValueStoreMutations) {}

  public record NodeReconciliationResult(
      PCollection<Mutation> restoreNodeMutations,
      PCollection<Mutation> deleteNodeMutations,
      PCollection<Mutation> deleteEmbeddingMutations,
      PCollection<Mutation> restoreEmbeddingMutations) {}

  /** Builds the complete Beam execution graph for Spanner time-travel rollback. */
  public static void buildPipeline(
      Pipeline pipeline, RollbackPipelineOptions options, SpannerClient spannerClient) {
    String timestampStr = options.getRollbackTimestamp();
    if (timestampStr == null || timestampStr.trim().isEmpty()) {
      throw new IllegalArgumentException("--rollbackTimestamp must be specified for rollback.");
    }

    Timestamp tPre = Timestamp.parseTimestamp(timestampStr.trim());
    List<String> targetProvenances = resolveTargetProvenances(options);
    LOGGER.info("Starting Spanner Time-Travel Rollback to T_pre: {}", tPre);
    LOGGER.info("Target provenances for rollback: {}", targetProvenances);

    // Phase 1: Partitioned Deletions at HEAD
    DeletionSignals delSignals = applyHeadDeletions(pipeline, targetProvenances, spannerClient);

    // Phase 2: Parallel Historical Snapshot Reads at T_pre
    HistoricalSnapshots snapshots =
        readHistoricalSnapshots(pipeline, tPre, targetProvenances, spannerClient);

    // Phase 3: Shared Table Reconciliation (Node & NodeEmbedding)
    NodeReconciliationResult nodeReconciliation =
        reconcileSharedEntities(pipeline, tPre, spannerClient);

    // Phase 4: Parallel Multi-Track Referential Integrity Write DAG
    writeRestorationDags(pipeline, spannerClient, delSignals, snapshots, nodeReconciliation);
  }

  // ---------------------------------------------------------------------------
  // Phase 1: Partitioned Deletions at HEAD
  // ---------------------------------------------------------------------------
  public static DeletionSignals applyHeadDeletions(
      Pipeline pipeline, List<String> targetProvenances, SpannerClient spannerClient) {
    PCollection<Void> delTsSignal =
        deleteDataForProvenances(pipeline, targetProvenances, TABLE_TIMESERIES, spannerClient);
    PCollection<Void> delEdgeSignal =
        deleteDataForProvenances(pipeline, targetProvenances, TABLE_EDGE, spannerClient);
    PCollection<Void> delKvSignal =
        deleteDataForProvenances(
            pipeline,
            targetProvenances,
            TABLE_KEY_VALUE_STORE,
            "type = 'ProvenanceSummary'",
            spannerClient);

    return new DeletionSignals(delTsSignal, delEdgeSignal, delKvSignal);
  }

  // ---------------------------------------------------------------------------
  // Phase 2: Parallel Historical Snapshot Reads at T_pre
  // ---------------------------------------------------------------------------
  public static HistoricalSnapshots readHistoricalSnapshots(
      Pipeline pipeline,
      Timestamp tPre,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    // Keep batching enabled so each read is partitioned across workers. Spanner only partitions
    // queries whose plan is rooted at a Distributed Union; the Edge, TimeSeries and KeyValueStore
    // reads qualify via their ByProvenance indexes. Observation does not, and is read by key
    // instead -- see ReadHistoricalObservationsFn.
    SpannerIO.Read baseRead = spannerClient.getReadTransform();

    // Read once and fan out: the raw Structs feed both the TimeSeries restore mutations and the
    // parent keys used to look up interleaved Observation children.
    PCollection<Struct> historicalTimeSeriesRows =
        readHistoricalTimeSeriesRows(pipeline, baseRead, tPre, targetProvenances, spannerClient);

    // Materialize each snapshot before the Phase 4 Wait.on gates. Otherwise fusion makes the read
    // inherit the write's dependency on Phases 1 and 3, when these reads are at T_pre and depend
    // on nothing at HEAD. Not redundant shuffles.
    PCollection<Mutation> restoreTimeSeriesMutations =
        historicalTimeSeriesRows
            .apply(
                "MapHistoricalTimeSeriesToMutations",
                MapElements.into(TypeDescriptor.of(Mutation.class))
                    .via(struct -> TimeSeriesRecord.from(struct).toMutation(TABLE_TIMESERIES)))
            .apply("MaterializeHistoricalTimeSeries", Reshuffle.viaRandomKey());
    PCollection<Mutation> restoreObservationMutations =
        readHistoricalObservations(historicalTimeSeriesRows, tPre, spannerClient);
    PCollection<Mutation> restoreEdgeMutations =
        readHistoricalEdges(pipeline, baseRead, tPre, targetProvenances, spannerClient);
    PCollection<Mutation> restoreKvMutations =
        readHistoricalKeyValueStore(pipeline, baseRead, tPre, targetProvenances, spannerClient);

    return new HistoricalSnapshots(
        restoreTimeSeriesMutations,
        restoreObservationMutations,
        restoreEdgeMutations,
        restoreKvMutations);
  }

  /**
   * Reads the historical TimeSeries rows at T_pre.
   *
   * <p>Selects {@code entity1} in addition to {@link TimeSeriesRecord#READ_COLUMNS}. It is a
   * generated column and therefore not writable, but it is the second component of the TimeSeries
   * primary key and so is required to address interleaved Observation children.
   */
  private static PCollection<Struct> readHistoricalTimeSeriesRows(
      Pipeline pipeline,
      SpannerIO.Read baseRead,
      Timestamp tPre,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    String tsColumns =
        Stream.concat(
                TimeSeriesRecord.READ_COLUMNS.stream(), Stream.of(TimeSeriesRecord.COL_ENTITY1))
            .collect(Collectors.joining(", "));
    String tsQuery =
        spannerClient.formatPartitionQuery(
            HISTORICAL_PROVENANCE_QUERY_TEMPLATE, tsColumns, TABLE_TIMESERIES);
    return pipeline.apply(
        "ReadHistoricalTimeSeries",
        baseRead
            .withTimestamp(tPre)
            .withQuery(
                Statement.newBuilder(tsQuery)
                    .bind("provenances")
                    .toStringArray(targetProvenances)
                    .build()));
  }

  /**
   * Reads historical Observation rows at T_pre via keyed prefix lookups on their parent TimeSeries
   * rows.
   *
   * <p>Observation is interleaved in TimeSeries, so a prefix read over the parent keys returns the
   * same rows a join would while parallelising over the upstream TimeSeries PCollection instead of
   * over query partitions. See {@link ReadHistoricalObservationsFn} for why the join form is
   * unsuitable here.
   */
  private static PCollection<Mutation> readHistoricalObservations(
      PCollection<Struct> historicalTimeSeriesRows, Timestamp tPre, SpannerClient spannerClient) {
    return historicalTimeSeriesRows
        .apply(
            "MapToObservationParentKeys",
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.integers(),
                        TypeDescriptors.lists(TypeDescriptors.strings())))
                .via(SpannerRollbackPipeline::toKeyedObservationParentKey))
        .apply(
            "GroupObservationParentKeyBatches",
            GroupIntoBatches.ofSize(OBSERVATION_PARENT_KEY_BATCH_SIZE))
        .apply(
            "ExtractObservationParentKeyBatch",
            MapElements.into(
                    TypeDescriptors.lists(TypeDescriptors.lists(TypeDescriptors.strings())))
                .via(kv -> Lists.newArrayList(kv.getValue())))
        .apply(
            "ReadHistoricalObservations",
            ParDo.of(new ReadHistoricalObservationsFn(spannerClient, tPre, TABLE_OBSERVATION)))
        .apply("MaterializeHistoricalObservations", Reshuffle.viaRandomKey());
  }

  /**
   * Projects a historical TimeSeries Struct onto its primary key, sharded by a deterministic hash
   * so that {@link GroupIntoBatches} distributes batches across workers reproducibly.
   */
  private static KV<Integer, List<String>> toKeyedObservationParentKey(Struct struct) {
    List<String> parentKey =
        List.of(
            struct.getString(TimeSeriesRecord.COL_VARIABLE_MEASURED),
            struct.getString(TimeSeriesRecord.COL_ENTITY1),
            struct.getString(TimeSeriesRecord.COL_EXTRA_ENTITIES_ID),
            struct.getString(TimeSeriesRecord.COL_FACET_ID));
    return KV.of(Math.floorMod(parentKey.hashCode(), OBSERVATION_PARENT_KEY_SHARDS), parentKey);
  }

  private static PCollection<Mutation> readHistoricalEdges(
      Pipeline pipeline,
      SpannerIO.Read baseRead,
      Timestamp tPre,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    String edgeColumns = String.join(", ", EdgeRecord.READ_COLUMNS);
    String edgeQuery =
        spannerClient.formatPartitionQuery(
            HISTORICAL_PROVENANCE_QUERY_TEMPLATE, edgeColumns, TABLE_EDGE);
    return pipeline
        .apply(
            "ReadHistoricalEdges",
            baseRead
                .withTimestamp(tPre)
                .withQuery(
                    Statement.newBuilder(edgeQuery)
                        .bind("provenances")
                        .toStringArray(targetProvenances)
                        .build()))
        .apply(
            "MapHistoricalEdgesToMutations",
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(struct -> EdgeRecord.from(struct).toMutation(TABLE_EDGE)))
        .apply("MaterializeHistoricalEdges", Reshuffle.viaRandomKey());
  }

  private static PCollection<Mutation> readHistoricalKeyValueStore(
      Pipeline pipeline,
      SpannerIO.Read baseRead,
      Timestamp tPre,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    String kvColumns = String.join(", ", KeyValueStoreRecord.READ_COLUMNS);
    String kvQuery = spannerClient.formatPartitionQuery(HISTORICAL_KV_QUERY_TEMPLATE, kvColumns);
    return pipeline
        .apply(
            "ReadHistoricalKeyValueStore",
            baseRead
                .withTimestamp(tPre)
                .withQuery(
                    Statement.newBuilder(kvQuery)
                        .bind("provenances")
                        .toStringArray(targetProvenances)
                        .build()))
        .apply(
            "MapHistoricalKeyValueStoreToMutations",
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(struct -> KeyValueStoreRecord.from(struct).toMutation(TABLE_KEY_VALUE_STORE)))
        .apply("MaterializeHistoricalKeyValueStore", Reshuffle.viaRandomKey());
  }

  // ---------------------------------------------------------------------------
  // Phase 3: Shared Table Reconciliation (Node & NodeEmbedding)
  // ---------------------------------------------------------------------------
  public static NodeReconciliationResult reconcileSharedEntities(
      Pipeline pipeline, Timestamp tPre, SpannerClient spannerClient) {
    SpannerIO.Read baseRead = spannerClient.getReadTransform();

    // TODO: Optimize node reconciliation for very large Node tables.
    // Querying Node by last_update_timestamp >= @tPre scans the Node table. As a future
    // scalability optimization, candidate subject_ids can be extracted directly from the
    // failed import's Edge and TimeSeries tables at HEAD prior to deletion, avoiding a
    // table scan over unmodified nodes.
    String modifiedNodesQuery = spannerClient.formatPartitionQuery(MODIFIED_NODES_QUERY);
    PCollection<String> modifiedSubjectIds =
        pipeline
            .apply(
                "ReadModifiedSubjectIdsAtHead",
                baseRead.withQuery(
                    Statement.newBuilder(modifiedNodesQuery).bind("tPre").to(tPre).build()))
            .apply(
                "ExtractSubjectId",
                MapElements.into(TypeDescriptors.strings())
                    .via(struct -> struct.getString(NodeRecord.COL_SUBJECT_ID)));

    PCollectionTuple nodeReconcileTuple =
        modifiedSubjectIds
            .apply(
                "MapToKeyedSubjectId",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                    .via(
                        id ->
                            KV.of(
                                java.util.concurrent.ThreadLocalRandom.current().nextInt(100), id)))
            .apply("GroupNodeBatches", GroupIntoBatches.ofSize(NODE_RECONCILE_BATCH_SIZE))
            .apply(
                "ExtractBatchElements",
                MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                    .via(kv -> Lists.newArrayList(kv.getValue())))
            .apply(
                "ReconcileNodeBatches",
                ParDo.of(new ReconcileNodesFn(spannerClient, tPre))
                    .withOutputTags(
                        RESTORE_NODES_TAG,
                        TupleTagList.of(DELETE_NODES_TAG).and(RESTORED_NODE_IDS_TAG)));

    PCollection<Mutation> restoreNodeMutations = nodeReconcileTuple.get(RESTORE_NODES_TAG);
    PCollection<Mutation> deleteNodeMutations = nodeReconcileTuple.get(DELETE_NODES_TAG);

    PCollectionTuple embTuple =
        nodeReconcileTuple
            .get(RESTORED_NODE_IDS_TAG)
            .apply(
                "ReconcileNodeEmbeddingBatches",
                ParDo.of(new ReconcileNodeEmbeddingsFn(spannerClient, tPre))
                    .withOutputTags(
                        ReconcileNodeEmbeddingsFn.RESTORE_EMBEDDINGS_TAG,
                        TupleTagList.of(ReconcileNodeEmbeddingsFn.DELETE_EMBEDDINGS_TAG)));

    PCollection<Mutation> deleteEmbeddingMutations =
        embTuple.get(ReconcileNodeEmbeddingsFn.DELETE_EMBEDDINGS_TAG);
    PCollection<Mutation> restoreEmbeddingMutations =
        embTuple.get(ReconcileNodeEmbeddingsFn.RESTORE_EMBEDDINGS_TAG);

    return new NodeReconciliationResult(
        restoreNodeMutations,
        deleteNodeMutations,
        deleteEmbeddingMutations,
        restoreEmbeddingMutations);
  }

  // ---------------------------------------------------------------------------
  // Phase 4: Parallel Multi-Track Referential Integrity Write DAG
  // ---------------------------------------------------------------------------
  public static void writeRestorationDags(
      Pipeline pipeline,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots,
      NodeReconciliationResult nodeReconciliation) {
    writeGraphTrack(pipeline, spannerClient, delSignals, snapshots, nodeReconciliation);
    writeTimeSeriesTrack(pipeline, spannerClient, delSignals, snapshots);
    writeKeyValueStoreTrack(pipeline, spannerClient, delSignals, snapshots);
  }

  private static void writeGraphTrack(
      Pipeline pipeline,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots,
      NodeReconciliationResult nodeReconciliation) {
    // 1A. Write Restored Nodes
    SpannerWriteResult writtenNodes =
        spannerClient.writeMutations(
            pipeline, "WriteRestoredNodes", nodeReconciliation.restoreNodeMutations());

    // 1B. Write Restored Edges (Interleaved in Node -> waits on both writtenNodes and
    // delEdgeSignal)
    PCollection<Mutation> edgeMutationsToWrite =
        snapshots
            .edgeMutations()
            .apply("WaitOnDelEdges", Wait.on(delSignals.delEdgeSignal()))
            .apply("WaitOnWrittenNodesForEdges", Wait.on(writtenNodes.getOutput()));
    spannerClient.writeMutations(pipeline, "WriteRestoredEdges", edgeMutationsToWrite);

    // 1C. Reconcile NodeEmbeddings (Interleaved in Node -> prefix deletes applied before restores)
    PCollection<Mutation> embDeletesToWrite = nodeReconciliation.deleteEmbeddingMutations();
    SpannerWriteResult writtenEmbDeletes =
        spannerClient.writeMutations(pipeline, "WriteDeletedNodeEmbeddings", embDeletesToWrite);

    PCollection<Mutation> embRestoresToWrite =
        nodeReconciliation
            .restoreEmbeddingMutations()
            .apply("WaitOnWrittenNodesForEmbeddings", Wait.on(writtenNodes.getOutput()))
            .apply("WaitOnDeletedEmbeddingsForRestore", Wait.on(writtenEmbDeletes.getOutput()));
    spannerClient.writeMutations(pipeline, "WriteRestoredNodeEmbeddings", embRestoresToWrite);

    // 1D. Delete Newly Added Nodes (Edge is interleaved in Node without cascade -> waits on
    // delEdgeSignal)
    PCollection<Mutation> deleteNodeMutations =
        nodeReconciliation
            .deleteNodeMutations()
            .apply("WaitOnDelEdgesForNodeDelete", Wait.on(delSignals.delEdgeSignal()));
    spannerClient.writeMutations(pipeline, "WriteDeletedNodes", deleteNodeMutations);
  }

  private static void writeTimeSeriesTrack(
      Pipeline pipeline,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots) {
    // 2A. Write Restored TimeSeries (Waits on TimeSeries delete)
    PCollection<Mutation> tsMutationsToWrite =
        snapshots.timeSeriesMutations().apply("WaitOnDelTS", Wait.on(delSignals.delTsSignal()));
    SpannerWriteResult writtenTS =
        spannerClient.writeMutations(pipeline, "WriteRestoredTimeSeries", tsMutationsToWrite);

    // 2B. Write Restored Observations (Interleaved in TimeSeries -> waits on written TimeSeries)
    PCollection<Mutation> obsMutationsToWrite =
        snapshots.observationMutations().apply("WaitOnWrittenTS", Wait.on(writtenTS.getOutput()));
    spannerClient.writeMutations(pipeline, "WriteRestoredObservations", obsMutationsToWrite);
  }

  private static void writeKeyValueStoreTrack(
      Pipeline pipeline,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots) {
    PCollection<Mutation> kvMutationsToWrite =
        snapshots.keyValueStoreMutations().apply("WaitOnDelKV", Wait.on(delSignals.delKvSignal()));
    spannerClient.writeMutations(pipeline, "WriteRestoredKeyValueStore", kvMutationsToWrite);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  public static List<String> resolveTargetProvenances(RollbackPipelineOptions options) {
    String importList = options.getImportList();
    if (importList == null || importList.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "--importList must be specified for rollback to resolve target provenances.");
    }

    Set<String> importNames = parseImportNames(importList.trim());
    if (importNames.isEmpty()) {
      throw new IllegalArgumentException(
          "Could not parse any valid import names from --importList: " + importList);
    }

    boolean isBaseDc = options.getIsBaseDc();
    return importNames.stream()
        .flatMap(
            name ->
                Stream.of(
                    ProvenanceUtils.getProvenanceDcid(name, isBaseDc),
                    ProvenanceUtils.getProvenanceDcid("generated/" + name, isBaseDc)))
        .toList();
  }

  private static Set<String> parseImportNames(String rawInput) {
    try {
      return StreamSupport.stream(
              JsonParser.parseString(rawInput).getAsJsonArray().spliterator(), false)
          .map(e -> e.getAsJsonObject().get("importName").getAsString().trim())
          .filter(name -> !name.isEmpty())
          .collect(Collectors.toCollection(LinkedHashSet::new));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse --importList as JSON array: " + rawInput, e);
    }
  }

  public static PCollection<Void> deleteDataForProvenances(
      Pipeline pipeline,
      List<String> targetProvenances,
      String tableName,
      SpannerClient spannerClient) {
    return deleteDataForProvenances(pipeline, targetProvenances, tableName, null, spannerClient);
  }

  public static PCollection<Void> deleteDataForProvenances(
      Pipeline pipeline,
      List<String> targetProvenances,
      String tableName,
      String additionalPredicate,
      SpannerClient spannerClient) {
    return pipeline
        .apply(
            "CreateTargetProvs-" + tableName,
            Create.of(List.of(targetProvenances)).withCoder(ListCoder.of(StringUtf8Coder.of())))
        .apply(
            "ExecuteDeleteProvs-" + tableName,
            ParDo.of(
                new SpannerPartitionedDeleteFn(
                    spannerClient, tableName, "provenance", additionalPredicate)));
  }
}
