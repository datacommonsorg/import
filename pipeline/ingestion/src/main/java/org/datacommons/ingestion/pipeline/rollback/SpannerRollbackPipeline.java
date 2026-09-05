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
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.data.ProvenanceUtils;
import org.datacommons.ingestion.pipeline.IngestionPipelineOptions;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.EdgeRecord;
import org.datacommons.ingestion.spanner.model.KeyValueStoreRecord;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.datacommons.ingestion.spanner.model.ObservationRecord;
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

  private static final String HISTORICAL_PROVENANCE_QUERY_TEMPLATE =
      "SELECT %s FROM %s WHERE provenance IN UNNEST(@provenances)";
  private static final String HISTORICAL_OBSERVATIONS_QUERY_TEMPLATE =
      "SELECT %s FROM Observation o JOIN TimeSeries ts ON o.variable_measured = ts.variable_measured AND"
          + " o.entity1 = ts.entity1 AND o.extra_entities_id = ts.extra_entities_id AND"
          + " o.facet_id = ts.facet_id WHERE ts.provenance IN UNNEST(@provenances)";
  private static final String HISTORICAL_KV_QUERY_TEMPLATE =
      "SELECT %s FROM KeyValueStore WHERE type = 'ProvenanceSummary' AND provenance IN UNNEST(@provenances)";
  private static final String MODIFIED_NODES_QUERY =
      "SELECT subject_id FROM Node WHERE last_update_timestamp >= @tPre";

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
      PCollection<Mutation> restoreEmbeddingMutations) {}

  /** Builds the complete Beam execution graph for Spanner time-travel rollback. */
  public static void buildPipeline(
      Pipeline pipeline, IngestionPipelineOptions options, SpannerClient spannerClient) {
    String timestampStr = options.getRollbackTimestamp();
    if (timestampStr == null || timestampStr.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "--rollbackTimestamp must be specified when --isRollback=true");
    }

    Timestamp tPre = Timestamp.parseTimestamp(timestampStr.trim());
    List<String> targetProvenances = resolveTargetProvenances(options);
    LOGGER.info("Starting Spanner Time-Travel Rollback to T_pre: {}", tPre);
    LOGGER.info("Target provenances for rollback: {}", targetProvenances);

    // Phase 1: Partitioned Deletions at HEAD
    DeletionSignals delSignals =
        applyHeadDeletions(pipeline, options, targetProvenances, spannerClient);

    // Phase 2: Parallel Historical Snapshot Reads at T_pre
    HistoricalSnapshots snapshots =
        readHistoricalSnapshots(pipeline, options, tPre, targetProvenances, spannerClient);

    // Phase 3: Shared Table Reconciliation (Node & NodeEmbedding)
    NodeReconciliationResult nodeReconciliation =
        reconcileSharedEntities(pipeline, options, tPre, spannerClient);

    // Phase 4: Parallel Multi-Track Referential Integrity Write DAG
    writeRestorationDags(
        pipeline, options, spannerClient, delSignals, snapshots, nodeReconciliation);
  }

  // ---------------------------------------------------------------------------
  // Phase 1: Partitioned Deletions at HEAD
  // ---------------------------------------------------------------------------
  public static DeletionSignals applyHeadDeletions(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    if (options.getSkipDelete()) {
      return new DeletionSignals(null, null, null);
    }

    PCollection<Void> delTsSignal =
        deleteDataForProvenances(
            pipeline, targetProvenances, spannerClient.getTimeSeriesTableName(), spannerClient);
    PCollection<Void> delEdgeSignal =
        deleteDataForProvenances(
            pipeline, targetProvenances, spannerClient.getEdgeTableName(), spannerClient);
    PCollection<Void> delKvSignal =
        deleteDataForProvenances(pipeline, targetProvenances, "KeyValueStore", spannerClient);

    return new DeletionSignals(delTsSignal, delEdgeSignal, delKvSignal);
  }

  // ---------------------------------------------------------------------------
  // Phase 2: Parallel Historical Snapshot Reads at T_pre
  // ---------------------------------------------------------------------------
  public static HistoricalSnapshots readHistoricalSnapshots(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      Timestamp tPre,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    SpannerIO.Read baseRead = spannerClient.getReadTransform();

    PCollection<Mutation> restoreTimeSeriesMutations =
        readHistoricalTimeSeries(pipeline, baseRead, tPre, targetProvenances, spannerClient);
    PCollection<Mutation> restoreObservationMutations =
        readHistoricalObservations(pipeline, baseRead, tPre, targetProvenances, spannerClient);
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

  private static PCollection<Mutation> readHistoricalTimeSeries(
      Pipeline pipeline,
      SpannerIO.Read baseRead,
      Timestamp tPre,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    String tsColumns = String.join(", ", TimeSeriesRecord.READ_COLUMNS);
    String tsQuery =
        spannerClient.formatPartitionQuery(
            HISTORICAL_PROVENANCE_QUERY_TEMPLATE,
            tsColumns,
            spannerClient.getTimeSeriesTableName());
    return pipeline
        .apply(
            "ReadHistoricalTimeSeries",
            baseRead
                .withTimestamp(tPre)
                .withQuery(
                    Statement.newBuilder(tsQuery)
                        .bind("provenances")
                        .toStringArray(targetProvenances)
                        .build()))
        .apply(
            "MapHistoricalTimeSeriesToMutations",
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(
                    struct ->
                        TimeSeriesRecord.from(struct)
                            .toMutation(spannerClient.getTimeSeriesTableName())));
  }

  private static PCollection<Mutation> readHistoricalObservations(
      Pipeline pipeline,
      SpannerIO.Read baseRead,
      Timestamp tPre,
      List<String> targetProvenances,
      SpannerClient spannerClient) {
    String obsColumns =
        ObservationRecord.READ_COLUMNS.stream()
            .map(c -> "o." + c)
            .collect(Collectors.joining(", "));
    String obsQuery =
        spannerClient.formatPartitionQuery(HISTORICAL_OBSERVATIONS_QUERY_TEMPLATE, obsColumns);
    return pipeline
        .apply(
            "ReadHistoricalObservations",
            baseRead
                .withTimestamp(tPre)
                .withQuery(
                    Statement.newBuilder(obsQuery)
                        .bind("provenances")
                        .toStringArray(targetProvenances)
                        .build()))
        .apply(
            "MapHistoricalObservationsToMutations",
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(
                    struct ->
                        ObservationRecord.from(struct)
                            .toMutation(spannerClient.getObservationTableName())));
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
            HISTORICAL_PROVENANCE_QUERY_TEMPLATE, edgeColumns, spannerClient.getEdgeTableName());
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
                .via(
                    struct ->
                        EdgeRecord.from(struct).toMutation(spannerClient.getEdgeTableName())));
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
                .via(struct -> KeyValueStoreRecord.from(struct).toMutation("KeyValueStore")));
  }

  // ---------------------------------------------------------------------------
  // Phase 3: Shared Table Reconciliation (Node & NodeEmbedding)
  // ---------------------------------------------------------------------------
  public static NodeReconciliationResult reconcileSharedEntities(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      Timestamp tPre,
      SpannerClient spannerClient) {
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

    PCollection<Mutation> restoreEmbeddingMutations =
        nodeReconcileTuple
            .get(RESTORED_NODE_IDS_TAG)
            .apply(
                "ReconcileNodeEmbeddingBatches",
                ParDo.of(new ReconcileNodeEmbeddingsFn(spannerClient, tPre)));

    return new NodeReconciliationResult(
        restoreNodeMutations, deleteNodeMutations, restoreEmbeddingMutations);
  }

  // ---------------------------------------------------------------------------
  // Phase 4: Parallel Multi-Track Referential Integrity Write DAG
  // ---------------------------------------------------------------------------
  public static void writeRestorationDags(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots,
      NodeReconciliationResult nodeReconciliation) {
    writeGraphTrack(pipeline, options, spannerClient, delSignals, snapshots, nodeReconciliation);
    writeTimeSeriesTrack(pipeline, options, spannerClient, delSignals, snapshots);
    writeKeyValueStoreTrack(pipeline, options, spannerClient, delSignals, snapshots);
  }

  private static void writeGraphTrack(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots,
      NodeReconciliationResult nodeReconciliation) {
    boolean skipDelete = options.getSkipDelete();
    boolean skipWait = options.getSkipWait();

    // 1A. Write Restored Nodes
    SpannerWriteResult writtenNodes =
        spannerClient.writeMutations(
            pipeline, "WriteRestoredNodes", nodeReconciliation.restoreNodeMutations());

    // 1B. Write Restored Edges (Only waits on its own Edge delete signal)
    PCollection<Mutation> edgeMutationsToWrite = snapshots.edgeMutations();
    if (!skipWait && !skipDelete && delSignals.delEdgeSignal() != null) {
      edgeMutationsToWrite =
          edgeMutationsToWrite.apply("WaitOnDelEdges", Wait.on(delSignals.delEdgeSignal()));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredEdges", edgeMutationsToWrite);

    // 1C. Write Restored NodeEmbeddings (Interleaved in Node -> waits on written Nodes)
    PCollection<Mutation> embMutationsToWrite = nodeReconciliation.restoreEmbeddingMutations();
    if (!skipWait) {
      embMutationsToWrite =
          embMutationsToWrite.apply(
              "WaitOnWrittenNodesForEmbeddings", Wait.on(writtenNodes.getOutput()));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredNodeEmbeddings", embMutationsToWrite);

    // 1D. Delete Newly Added Nodes
    spannerClient.writeMutations(
        pipeline, "WriteDeletedNodes", nodeReconciliation.deleteNodeMutations());
  }

  private static void writeTimeSeriesTrack(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots) {
    boolean skipDelete = options.getSkipDelete();
    boolean skipWait = options.getSkipWait();

    // 2A. Write Restored TimeSeries (Waits on TimeSeries delete)
    PCollection<Mutation> tsMutationsToWrite = snapshots.timeSeriesMutations();
    if (!skipWait && !skipDelete && delSignals.delTsSignal() != null) {
      tsMutationsToWrite =
          tsMutationsToWrite.apply("WaitOnDelTS", Wait.on(delSignals.delTsSignal()));
    }
    SpannerWriteResult writtenTS =
        spannerClient.writeMutations(pipeline, "WriteRestoredTimeSeries", tsMutationsToWrite);

    // 2B. Write Restored Observations (Interleaved in TimeSeries -> waits on written TimeSeries)
    PCollection<Mutation> obsMutationsToWrite = snapshots.observationMutations();
    if (!skipWait) {
      obsMutationsToWrite =
          obsMutationsToWrite.apply("WaitOnWrittenTS", Wait.on(writtenTS.getOutput()));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredObservations", obsMutationsToWrite);
  }

  private static void writeKeyValueStoreTrack(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      SpannerClient spannerClient,
      DeletionSignals delSignals,
      HistoricalSnapshots snapshots) {
    boolean skipDelete = options.getSkipDelete();
    boolean skipWait = options.getSkipWait();

    PCollection<Mutation> kvMutationsToWrite = snapshots.keyValueStoreMutations();
    if (!skipWait && !skipDelete && delSignals.delKvSignal() != null) {
      kvMutationsToWrite =
          kvMutationsToWrite.apply("WaitOnDelKV", Wait.on(delSignals.delKvSignal()));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredKeyValueStore", kvMutationsToWrite);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  public static List<String> resolveTargetProvenances(IngestionPipelineOptions options) {
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
    return pipeline
        .apply(
            "CreateTargetProvs-" + tableName,
            Create.of(List.of(targetProvenances)).withCoder(ListCoder.of(StringUtf8Coder.of())))
        .apply(
            "ExecuteDeleteProvs-" + tableName,
            ParDo.of(new SpannerPartitionedDeleteFn(spannerClient, tableName, "provenance")));
  }
}
