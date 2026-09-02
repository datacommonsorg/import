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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

  public static final String KEY_VALUE_STORE_TABLE = "KeyValueStore";
  public static final String NODE_EMBEDDING_TABLE = "NodeEmbedding";

  public static final TupleTag<Mutation> RESTORE_NODES_TAG = ReconcileNodesFn.RESTORE_NODES_TAG;
  public static final TupleTag<Mutation> DELETE_NODES_TAG = ReconcileNodesFn.DELETE_NODES_TAG;

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
    validateRetentionWindow(tPre);

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

    String emulatorHost = options.getEmulatorHost();
    PCollection<Void> delTsSignal =
        deleteDataForProvenances(
            pipeline,
            targetProvenances,
            spannerClient.getTimeSeriesTableName(),
            "provenance",
            spannerClient,
            emulatorHost);
    PCollection<Void> delEdgeSignal =
        deleteDataForProvenances(
            pipeline,
            targetProvenances,
            spannerClient.getEdgeTableName(),
            "provenance",
            spannerClient,
            emulatorHost);
    PCollection<Void> delKvSignal =
        deleteDataForProvenances(
            pipeline,
            targetProvenances,
            KEY_VALUE_STORE_TABLE,
            "provenance",
            spannerClient,
            emulatorHost);

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
    SpannerIO.Read baseRead = createBaseRead(spannerClient, options.getEmulatorHost());

    // A. Read Historical TimeSeries
    String tsColumns = String.join(", ", TimeSeriesRecord.READ_COLUMNS);
    String tsQuery =
        spannerClient.formatPartitionQuery(
            "SELECT %s FROM %s WHERE provenance IN UNNEST(@provenances)",
            tsColumns, spannerClient.getTimeSeriesTableName());
    PCollection<Mutation> restoreTimeSeriesMutations =
        pipeline
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

    // B. Read Historical Observations (Time-Travel JOIN via TimeSeries)
    String obsColumns =
        ObservationRecord.READ_COLUMNS.stream()
            .map(c -> "o." + c)
            .collect(java.util.stream.Collectors.joining(", "));
    String obsQuery =
        spannerClient.formatPartitionQuery(
            "SELECT %s FROM %s o JOIN %s ts ON o.variable_measured = ts.variable_measured AND"
                + " o.entity1 = ts.entity1 AND o.extra_entities_id = ts.extra_entities_id AND"
                + " o.facet_id = ts.facet_id WHERE ts.provenance IN UNNEST(@provenances)",
            obsColumns,
            spannerClient.getObservationTableName(),
            spannerClient.getTimeSeriesTableName());
    PCollection<Mutation> restoreObservationMutations =
        pipeline
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

    // C. Read Historical Edges
    String edgeColumns = String.join(", ", EdgeRecord.READ_COLUMNS);
    String edgeQuery =
        spannerClient.formatPartitionQuery(
            "SELECT %s FROM %s WHERE provenance IN UNNEST(@provenances)",
            edgeColumns, spannerClient.getEdgeTableName());
    PCollection<Mutation> restoreEdgeMutations =
        pipeline
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

    // D. Read Historical KeyValueStore (type = 'ProvenanceSummary')
    String kvColumns = String.join(", ", KeyValueStoreRecord.READ_COLUMNS);
    String kvQuery =
        spannerClient.formatPartitionQuery(
            "SELECT %s FROM %s WHERE type = 'ProvenanceSummary' AND provenance IN"
                + " UNNEST(@provenances)",
            kvColumns, KEY_VALUE_STORE_TABLE);
    PCollection<Mutation> restoreKvMutations =
        pipeline
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
                    .via(
                        struct ->
                            KeyValueStoreRecord.from(struct).toMutation(KEY_VALUE_STORE_TABLE)));

    return new HistoricalSnapshots(
        restoreTimeSeriesMutations,
        restoreObservationMutations,
        restoreEdgeMutations,
        restoreKvMutations);
  }

  // ---------------------------------------------------------------------------
  // Phase 3: Shared Table Reconciliation (Node & NodeEmbedding)
  // ---------------------------------------------------------------------------
  public static NodeReconciliationResult reconcileSharedEntities(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      Timestamp tPre,
      SpannerClient spannerClient) {
    SpannerIO.Read baseRead = createBaseRead(spannerClient, options.getEmulatorHost());
    String emulatorHost = options.getEmulatorHost();

    String modifiedNodesQuery =
        spannerClient.formatPartitionQuery(
            "SELECT subject_id FROM %s WHERE last_update_timestamp >= @tPre",
            spannerClient.getNodeTableName());
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
                    .via(id -> KV.of(0, id)))
            .apply("GroupNodeBatches", GroupIntoBatches.ofSize(NODE_RECONCILE_BATCH_SIZE))
            .apply(
                "ExtractBatchElements",
                MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                    .via(kv -> Lists.newArrayList(kv.getValue())))
            .apply(
                "ReconcileNodeBatches",
                ParDo.of(new ReconcileNodesFn(spannerClient, tPre, emulatorHost))
                    .withOutputTags(RESTORE_NODES_TAG, TupleTagList.of(DELETE_NODES_TAG)));

    PCollection<Mutation> restoreNodeMutations = nodeReconcileTuple.get(RESTORE_NODES_TAG);
    PCollection<Mutation> deleteNodeMutations = nodeReconcileTuple.get(DELETE_NODES_TAG);

    PCollection<Mutation> restoreEmbeddingMutations =
        restoreNodeMutations
            .apply(
                "ExtractRestoredSubjectIds",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        mutation ->
                            SpannerClient.getMutationValue(
                                mutation.asMap(), NodeRecord.COL_SUBJECT_ID)))
            .apply(
                "MapToKeyedEmbSubjectId",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                    .via(id -> KV.of(0, id)))
            .apply("GroupEmbNodeBatches", GroupIntoBatches.ofSize(NODE_RECONCILE_BATCH_SIZE))
            .apply(
                "ExtractEmbBatchElements",
                MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                    .via(kv -> Lists.newArrayList(kv.getValue())))
            .apply(
                "ReconcileNodeEmbeddingBatches",
                ParDo.of(new ReconcileNodeEmbeddingsFn(spannerClient, tPre, emulatorHost)));

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
    boolean skipDelete = options.getSkipDelete();
    boolean skipWait = options.getSkipWait();

    // --- Track 1: Graph Nodes, Edges, and Embeddings (Fully Concurrent) ---
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

    // --- Track 2: TimeSeries & Observations ---
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

    // --- Track 3: KeyValueStore (Fully Concurrent) ---
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
  private static SpannerIO.Read createBaseRead(SpannerClient spannerClient, String emulatorHost) {
    SpannerIO.Read baseRead =
        SpannerIO.read()
            .withProjectId(spannerClient.getGcpProjectId())
            .withInstanceId(spannerClient.getSpannerInstanceId())
            .withDatabaseId(spannerClient.getSpannerDatabaseId())
            .withLowPriority();
    if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
      baseRead = baseRead.withEmulatorHost(emulatorHost.trim());
    }
    return baseRead;
  }

  private static final int MAX_RETENTION_DAYS = 7;
  private static final com.google.gson.Gson GSON = new com.google.gson.Gson();

  public static void validateRetentionWindow(Timestamp tPre) {
    Instant tPreInstant = Instant.ofEpochSecond(tPre.getSeconds(), tPre.getNanos());
    Instant maxRetentionBoundary = Instant.now().minus(Duration.ofDays(MAX_RETENTION_DAYS));
    if (tPreInstant.isBefore(maxRetentionBoundary)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot perform rollback: Timestamp %s exceeds Spanner's %d-day PITR retention"
                  + " window.",
              tPre, MAX_RETENTION_DAYS));
    }
  }

  public static List<String> resolveTargetProvenances(IngestionPipelineOptions options) {
    java.util.Set<String> provenances = new java.util.HashSet<>();

    String customTargetJson = options.getTargetProvenances();
    if (customTargetJson != null && !customTargetJson.trim().isEmpty()) {
      java.lang.reflect.Type listType =
          new com.google.common.reflect.TypeToken<List<String>>() {}.getType();
      try {
        List<String> parsed = GSON.fromJson(customTargetJson.trim(), listType);
        if (parsed != null) {
          provenances.addAll(parsed);
        }
      } catch (Exception e) {
        // If not JSON array, parse comma-separated
        String[] tokens = customTargetJson.split(",");
        for (String token : tokens) {
          if (!token.trim().isEmpty()) {
            provenances.add(token.trim());
          }
        }
      }
    }

    String importList = options.getImportList();
    if (importList != null && !importList.trim().isEmpty()) {
      boolean parsedJson = false;
      try {
        JsonElement jsonElement = JsonParser.parseString(importList.trim());
        if (jsonElement.isJsonArray()) {
          JsonArray jsonArray = jsonElement.getAsJsonArray();
          for (JsonElement element : jsonArray) {
            if (element.isJsonObject() && element.getAsJsonObject().has("importName")) {
              String importName = element.getAsJsonObject().get("importName").getAsString();
              if (importName != null && !importName.trim().isEmpty()) {
                provenances.add(
                    org.datacommons.ingestion.data.ProvenanceUtils.getProvenanceDcid(
                        importName.trim(), options.getIsBaseDc()));
                provenances.add(
                    org.datacommons.ingestion.data.ProvenanceUtils.getProvenanceDcid(
                        "generated/" + importName.trim(), options.getIsBaseDc()));
              }
            }
          }
          parsedJson = true;
        }
      } catch (Exception e) {
        // Fall back to comma-separated parsing
      }
      if (!parsedJson) {
        String[] importNames = importList.split(",");
        for (String name : importNames) {
          String trimmed = name.trim();
          if (!trimmed.isEmpty()) {
            provenances.add(
                org.datacommons.ingestion.data.ProvenanceUtils.getProvenanceDcid(
                    trimmed, options.getIsBaseDc()));
            provenances.add(
                org.datacommons.ingestion.data.ProvenanceUtils.getProvenanceDcid(
                    "generated/" + trimmed, options.getIsBaseDc()));
          }
        }
      }
    }

    if (provenances.isEmpty()) {
      throw new IllegalArgumentException(
          "Could not resolve any target provenances for rollback. Please specify --importList or"
              + " --targetProvenances.");
    }

    return new ArrayList<>(provenances);
  }

  public static PCollection<Void> deleteDataForProvenances(
      Pipeline pipeline,
      List<String> targetProvenances,
      String tableName,
      String columnName,
      SpannerClient spannerClient,
      String emulatorHost) {
    return pipeline
        .apply(
            "CreateTargetProvs-" + tableName,
            Create.of(List.of(targetProvenances)).withCoder(ListCoder.of(StringUtf8Coder.of())))
        .apply(
            "ExecuteDeleteProvs-" + tableName,
            ParDo.of(
                new SpannerPartitionedDeleteFn(
                    spannerClient, tableName, columnName, emulatorHost)));
  }
}
