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

package org.datacommons.ingestion.pipeline;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
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
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dedicated pipeline builder for Spanner Time-Travel Rollback.
 *
 * <p>Restores database state to a historical point-in-time snapshot (T_pre) using Spanner's native
 * MVCC time-travel query mechanism and executes an ordered referential-integrity mutation DAG.
 */
public class RollbackPipeline implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollbackPipeline.class);
  private static final Gson GSON = new Gson();
  private static final long MAX_RETENTION_DAYS = 7;
  private static final long NODE_RECONCILE_BATCH_SIZE = 1000L;

  public static final String KEY_VALUE_STORE_TABLE = "KeyValueStore";
  public static final String NODE_EMBEDDING_TABLE = "NodeEmbedding";

  public static final TupleTag<Mutation> RESTORE_NODES_TAG = new TupleTag<Mutation>() {};
  public static final TupleTag<Mutation> DELETE_NODES_TAG = new TupleTag<Mutation>() {};

  /** Builds the Beam execution graph for Spanner time-travel rollback. */
  public static void buildPipeline(
      Pipeline pipeline, IngestionPipelineOptions options, SpannerClient spannerClient) {
    String timestampStr = options.getRollbackTimestamp();
    if (timestampStr == null || timestampStr.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "--rollbackTimestamp must be specified when --isRollback=true");
    }

    com.google.cloud.Timestamp tPre =
        com.google.cloud.Timestamp.parseTimestamp(timestampStr.trim());
    validateRetentionWindow(tPre);

    List<String> targetProvenances = resolveTargetProvenances(options);
    LOGGER.info("Starting Spanner Time-Travel Rollback to T_pre: {}", tPre);
    LOGGER.info("Target provenances for rollback: {}", targetProvenances);

    boolean skipDelete = options.getSkipDelete();
    boolean skipWait = options.getSkipWait();
    String emulatorHost = options.getEmulatorHost();

    // -------------------------------------------------------------------------
    // Phase 1: Partitioned Deletions at HEAD
    // -------------------------------------------------------------------------
    PCollection<Void> delTsSignal = null;
    PCollection<Void> delEdgeSignal = null;
    PCollection<Void> delKvSignal = null;

    if (!skipDelete) {
      delTsSignal =
          deleteDataForProvenances(
              pipeline,
              targetProvenances,
              spannerClient.getTimeSeriesTableName(),
              "provenance",
              spannerClient,
              emulatorHost);
      delEdgeSignal =
          deleteDataForProvenances(
              pipeline,
              targetProvenances,
              spannerClient.getEdgeTableName(),
              "provenance",
              spannerClient,
              emulatorHost);
      delKvSignal =
          deleteDataForProvenances(
              pipeline,
              targetProvenances,
              KEY_VALUE_STORE_TABLE,
              "provenance",
              spannerClient,
              emulatorHost);
    }

    // -------------------------------------------------------------------------
    // Phase 2: Time-Travel Parallel Reads at T_pre
    // -------------------------------------------------------------------------
    SpannerIO.Read baseRead =
        SpannerIO.read()
            .withProjectId(spannerClient.getGcpProjectId())
            .withInstanceId(spannerClient.getSpannerInstanceId())
            .withDatabaseId(spannerClient.getSpannerDatabaseId())
            .withLowPriority();

    if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
      baseRead = baseRead.withEmulatorHost(emulatorHost.trim());
    }

    // A. Read Historical TimeSeries
    String tsQuery =
        String.format(
            "@{spanner_emulator.disable_query_partitionability_check=true} SELECT variable_measured, extra_entities_id, facet_id, entities, facet FROM %s WHERE"
                + " provenance IN UNNEST(@provenances)",
            spannerClient.getTimeSeriesTableName());
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
                            RestoreMutationMappers.toTimeSeriesRestoreMutation(
                                struct, spannerClient.getTimeSeriesTableName())));

    // B. Read Historical Observations (Time-Travel JOIN via TimeSeries)
    String obsQuery =
        String.format(
            "@{spanner_emulator.disable_query_partitionability_check=true} SELECT o.variable_measured, o.entity1, o.extra_entities_id, o.facet_id, o.date,"
                + " o.value FROM %s o JOIN %s ts ON o.variable_measured = ts.variable_measured AND"
                + " o.entity1 = ts.entity1 AND o.extra_entities_id = ts.extra_entities_id AND"
                + " o.facet_id = ts.facet_id WHERE ts.provenance IN UNNEST(@provenances)",
            spannerClient.getObservationTableName(), spannerClient.getTimeSeriesTableName());
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
                            RestoreMutationMappers.toObservationRestoreMutation(
                                struct, spannerClient.getObservationTableName())));

    // C. Read Historical Edges
    String edgeQuery =
        String.format(
            "@{spanner_emulator.disable_query_partitionability_check=true} SELECT subject_id, predicate, object_id, provenance FROM %s WHERE provenance IN"
                + " UNNEST(@provenances)",
            spannerClient.getEdgeTableName());
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
                            RestoreMutationMappers.toEdgeRestoreMutation(
                                struct, spannerClient.getEdgeTableName())));

    // D. Read Historical KeyValueStore (type = 'ProvenanceSummary')
    String kvQuery =
        String.format(
            "@{spanner_emulator.disable_query_partitionability_check=true} SELECT type, key, provenance, value FROM %s WHERE type = 'ProvenanceSummary' AND"
                + " provenance IN UNNEST(@provenances)",
            KEY_VALUE_STORE_TABLE);
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
                            RestoreMutationMappers.toKeyValueStoreRestoreMutation(
                                struct, KEY_VALUE_STORE_TABLE)));

    // -------------------------------------------------------------------------
    // Phase 3: Shared Table Reconciliation (Node & NodeEmbedding)
    // -------------------------------------------------------------------------
    String modifiedNodesQuery =
        String.format(
            "@{spanner_emulator.disable_query_partitionability_check=true} SELECT subject_id FROM %s WHERE last_update_timestamp >= @tPre",
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
                    .via(struct -> struct.getString("subject_id")));

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
                ParDo.of(new ReconcileNodeBatchFn(spannerClient, tPre, emulatorHost))
                    .withOutputTags(RESTORE_NODES_TAG, TupleTagList.of(DELETE_NODES_TAG)));

    PCollection<Mutation> restoreNodeMutations = nodeReconcileTuple.get(RESTORE_NODES_TAG);
    PCollection<Mutation> deleteNodeMutations = nodeReconcileTuple.get(DELETE_NODES_TAG);

    // Reconcile NodeEmbedding for restored nodes
    PCollection<Mutation> restoreEmbeddingMutations =
        restoreNodeMutations
            .apply(
                "ExtractRestoredSubjectIds",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        mutation -> SpannerClient.getMutationValue(mutation.asMap(), "subject_id")))
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
                ParDo.of(new ReconcileNodeEmbeddingBatchFn(spannerClient, tPre, emulatorHost)));

    // -------------------------------------------------------------------------
    // Phase 4: Parallel Multi-Track Referential Integrity Write DAG
    // -------------------------------------------------------------------------

    // --- Track 1: Graph Nodes, Edges, and Embeddings (Fully Concurrent) ---
    // 1A. Write Restored Nodes
    SpannerWriteResult writtenNodes =
        spannerClient.writeMutations(pipeline, "WriteRestoredNodes", restoreNodeMutations);

    // 1B. Write Restored Edges (Only waits on its own Edge delete signal)
    PCollection<Mutation> edgeMutationsToWrite = restoreEdgeMutations;
    if (!skipWait && !skipDelete && delEdgeSignal != null) {
      edgeMutationsToWrite = edgeMutationsToWrite.apply("WaitOnDelEdges", Wait.on(delEdgeSignal));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredEdges", edgeMutationsToWrite);

    // 1C. Write Restored NodeEmbeddings (Interleaved in Node -> waits on written Nodes)
    PCollection<Mutation> embMutationsToWrite = restoreEmbeddingMutations;
    if (!skipWait) {
      embMutationsToWrite =
          embMutationsToWrite.apply(
              "WaitOnWrittenNodesForEmbeddings", Wait.on(writtenNodes.getOutput()));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredNodeEmbeddings", embMutationsToWrite);

    // 1D. Delete Newly Added Nodes
    spannerClient.writeMutations(pipeline, "WriteDeletedNodes", deleteNodeMutations);

    // --- Track 2: TimeSeries & Observations ---
    // 2A. Write Restored TimeSeries (Waits on TimeSeries delete)
    PCollection<Mutation> tsMutationsToWrite = restoreTimeSeriesMutations;
    if (!skipWait && !skipDelete && delTsSignal != null) {
      tsMutationsToWrite = tsMutationsToWrite.apply("WaitOnDelTS", Wait.on(delTsSignal));
    }
    SpannerWriteResult writtenTS =
        spannerClient.writeMutations(pipeline, "WriteRestoredTimeSeries", tsMutationsToWrite);

    // 2B. Write Restored Observations (Interleaved in TimeSeries -> waits on written TimeSeries)
    PCollection<Mutation> obsMutationsToWrite = restoreObservationMutations;
    if (!skipWait) {
      obsMutationsToWrite =
          obsMutationsToWrite.apply("WaitOnWrittenTS", Wait.on(writtenTS.getOutput()));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredObservations", obsMutationsToWrite);

    // --- Track 3: KeyValueStore (Fully Concurrent) ---
    // 3A. Write Restored KeyValueStore (Waits on KV delete)
    PCollection<Mutation> kvMutationsToWrite = restoreKvMutations;
    if (!skipWait && !skipDelete && delKvSignal != null) {
      kvMutationsToWrite = kvMutationsToWrite.apply("WaitOnDelKV", Wait.on(delKvSignal));
    }
    spannerClient.writeMutations(pipeline, "WriteRestoredKeyValueStore", kvMutationsToWrite);
  }

  /** Resolves the complete list of target provenances for the rollback. */
  public static List<String> resolveTargetProvenances(IngestionPipelineOptions options) {
    Set<String> provenances = new HashSet<>();

    String customTargetJson = options.getTargetProvenances();
    if (customTargetJson != null && !customTargetJson.trim().isEmpty()) {
      Type listType = new TypeToken<List<String>>() {}.getType();
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
        com.google.gson.JsonElement jsonElement =
            com.google.gson.JsonParser.parseString(importList.trim());
        if (jsonElement.isJsonArray()) {
          com.google.gson.JsonArray jsonArray = jsonElement.getAsJsonArray();
          for (com.google.gson.JsonElement element : jsonArray) {
            if (element.isJsonObject() && element.getAsJsonObject().has("importName")) {
              String importName = element.getAsJsonObject().get("importName").getAsString();
              if (importName != null && !importName.trim().isEmpty()) {
                provenances.add(
                    ProvenanceUtils.getProvenanceDcid(importName.trim(), options.getIsBaseDc()));
                provenances.add(
                    ProvenanceUtils.getProvenanceDcid(
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
            provenances.add(ProvenanceUtils.getProvenanceDcid(trimmed, options.getIsBaseDc()));
            provenances.add(
                ProvenanceUtils.getProvenanceDcid("generated/" + trimmed, options.getIsBaseDc()));
          }
        }
      }
    }

    return new ArrayList<>(provenances);
  }

  private static void validateRetentionWindow(com.google.cloud.Timestamp tPre) {
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

  public static PCollection<Void> deleteDataForProvenances(
      Pipeline pipeline,
      List<String> provenances,
      String tableName,
      String columnName,
      SpannerClient spannerClient,
      String emulatorHost) {
    if (provenances == null || provenances.isEmpty()) {
      return pipeline.apply(
          "EmptyDeleteSignal-" + tableName, Create.empty(TypeDescriptor.of(Void.class)));
    }
    return pipeline
        .apply(
            "StartDeleteProvs-" + tableName,
            Create.of(Collections.singletonList(provenances))
                .withCoder(ListCoder.of(StringUtf8Coder.of())))
        .apply(
            "ExecuteDeleteProvs-" + tableName,
            ParDo.of(new DeleteByColumnListFn(spannerClient, tableName, columnName, emulatorHost)));
  }

  static class DeleteByColumnListFn extends DoFn<List<String>, Void> {
    private final SpannerClient spannerClient;
    private final String tableName;
    private final String columnName;
    private final String emulatorHost;

    public DeleteByColumnListFn(
        SpannerClient spannerClient, String tableName, String columnName, String emulatorHost) {
      this.spannerClient = spannerClient;
      this.tableName = tableName;
      this.columnName = columnName;
      this.emulatorHost = emulatorHost;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<String> values = c.element();
      if (values == null || values.isEmpty()) {
        c.output(null);
        return;
      }
      SpannerOptions.Builder builder =
          SpannerOptions.newBuilder().setProjectId(spannerClient.getGcpProjectId());
      if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
        builder.setEmulatorHost(emulatorHost.trim());
        builder.setCredentials(NoCredentials.getInstance());
      }
      try (Spanner spanner = builder.build().getService()) {
        DatabaseClient dbClient =
            spanner.getDatabaseClient(
                DatabaseId.of(
                    spannerClient.getGcpProjectId(),
                    spannerClient.getSpannerInstanceId(),
                    spannerClient.getSpannerDatabaseId()));
        String dml =
            String.format(
                "DELETE FROM %s WHERE %s IN UNNEST(@%s)", tableName, columnName, columnName);
        Statement statement =
            Statement.newBuilder(dml).bind(columnName).toStringArray(values).build();
        long rowCount = dbClient.executePartitionedUpdate(statement);
        LOGGER.info(
            "Deleted {} rows from {} for {} IN {}", rowCount, tableName, columnName, values);
        c.output(null);
      }
    }
  }

  /**
   * Reconciles batches of modified subject IDs against the historical snapshot at T_pre. Emits
   * restore mutations for nodes that existed at T_pre, and delete mutations for new nodes.
   */
  public static class ReconcileNodeBatchFn extends DoFn<List<String>, Mutation> {
    private final SpannerClient spannerClient;
    private final com.google.cloud.Timestamp tPre;
    private final String emulatorHost;
    private transient Spanner spanner;
    private transient DatabaseClient dbClient;

    public ReconcileNodeBatchFn(
        SpannerClient spannerClient, com.google.cloud.Timestamp tPre, String emulatorHost) {
      this.spannerClient = spannerClient;
      this.tPre = tPre;
      this.emulatorHost = emulatorHost;
    }

    @Setup
    public void setup() {
      SpannerOptions.Builder builder =
          SpannerOptions.newBuilder().setProjectId(spannerClient.getGcpProjectId());
      if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
        builder.setEmulatorHost(emulatorHost.trim());
        builder.setCredentials(NoCredentials.getInstance());
      }
      this.spanner = builder.build().getService();
      this.dbClient =
          spanner.getDatabaseClient(
              DatabaseId.of(
                  spannerClient.getGcpProjectId(),
                  spannerClient.getSpannerInstanceId(),
                  spannerClient.getSpannerDatabaseId()));
    }

    @Teardown
    public void teardown() {
      if (spanner != null) {
        spanner.close();
      }
    }

    @ProcessElement
    public void processElement(@Element List<String> batch, MultiOutputReceiver receiver) {
      if (batch == null || batch.isEmpty()) {
        return;
      }

      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      for (String subjectId : batch) {
        keySetBuilder.addKey(com.google.cloud.spanner.Key.of(subjectId));
      }

      Set<String> restoredIds = new HashSet<>();
      try (ResultSet rs =
          dbClient
              .singleUse(TimestampBound.ofReadTimestamp(tPre))
              .read(
                  spannerClient.getNodeTableName(),
                  keySetBuilder.build(),
                  Arrays.asList("subject_id", "name", "types", "value", "bytes"))) {
        while (rs.next()) {
          Struct row = rs.getCurrentRowAsStruct();
          String id = row.getString("subject_id");
          restoredIds.add(id);
          receiver
              .get(RESTORE_NODES_TAG)
              .output(
                  RestoreMutationMappers.toNodeRestoreMutation(
                      row, spannerClient.getNodeTableName()));
        }
      }

      for (String id : batch) {
        if (!restoredIds.contains(id)) {
          receiver
              .get(DELETE_NODES_TAG)
              .output(
                  Mutation.delete(
                      spannerClient.getNodeTableName(), com.google.cloud.spanner.Key.of(id)));
        }
      }
    }
  }

  /** Reconciles vector embeddings for restored nodes against the historical snapshot at T_pre. */
  public static class ReconcileNodeEmbeddingBatchFn extends DoFn<List<String>, Mutation> {
    private final SpannerClient spannerClient;
    private final com.google.cloud.Timestamp tPre;
    private final String emulatorHost;
    private transient Spanner spanner;
    private transient DatabaseClient dbClient;

    public ReconcileNodeEmbeddingBatchFn(
        SpannerClient spannerClient, com.google.cloud.Timestamp tPre, String emulatorHost) {
      this.spannerClient = spannerClient;
      this.tPre = tPre;
      this.emulatorHost = emulatorHost;
    }

    @Setup
    public void setup() {
      SpannerOptions.Builder builder =
          SpannerOptions.newBuilder().setProjectId(spannerClient.getGcpProjectId());
      if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
        builder.setEmulatorHost(emulatorHost.trim());
        builder.setCredentials(NoCredentials.getInstance());
      }
      this.spanner = builder.build().getService();
      this.dbClient =
          spanner.getDatabaseClient(
              DatabaseId.of(
                  spannerClient.getGcpProjectId(),
                  spannerClient.getSpannerInstanceId(),
                  spannerClient.getSpannerDatabaseId()));
    }

    @Teardown
    public void teardown() {
      if (spanner != null) {
        spanner.close();
      }
    }

    @ProcessElement
    public void processElement(@Element List<String> batch, OutputReceiver<Mutation> receiver) {
      if (batch == null || batch.isEmpty()) {
        return;
      }

      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      for (String subjectId : batch) {
        keySetBuilder.addRange(
            com.google.cloud.spanner.KeyRange.prefix(com.google.cloud.spanner.Key.of(subjectId)));
      }

      try (ResultSet rs =
          dbClient
              .singleUse(TimestampBound.ofReadTimestamp(tPre))
              .read(
                  NODE_EMBEDDING_TABLE,
                  keySetBuilder.build(),
                  Arrays.asList(
                      "subject_id",
                      "embedding_label",
                      "embedding_content_key",
                      "embedding_content",
                      "node_types",
                      "embeddings"))) {
        while (rs.next()) {
          Struct row = rs.getCurrentRowAsStruct();
          receiver.output(
              RestoreMutationMappers.toNodeEmbeddingRestoreMutation(row, NODE_EMBEDDING_TABLE));
        }
      }
    }
  }
}
