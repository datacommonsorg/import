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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.EdgeRecord;
import org.datacommons.ingestion.spanner.model.KeyValueStoreRecord;
import org.datacommons.ingestion.spanner.model.NodeEmbeddingRecord;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.datacommons.ingestion.spanner.model.ObservationRecord;
import org.datacommons.ingestion.spanner.model.TimeSeriesRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class RollbackPipelineIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RollbackPipelineIntegrationTest.class);
  private static final String TEST_MODE_PROPERTY = "testMode";
  private static final String MODE_LOCAL = "LOCAL";
  private static final String MODE_DATAFLOW = "DATAFLOW";

  public static GenericContainer<?> spannerEmulator;

  private Spanner spanner;
  private String projectId;
  private String instanceId;
  private String databaseId;
  private String gcsBucket;
  private String tempLocation;
  private String region;
  private String emulatorHost;
  private boolean isLocal;
  private SpannerClient spannerClient;
  private DatabaseClient dbClient;

  @Before
  public void setUp() throws Exception {
    String mode = System.getProperty(TEST_MODE_PROPERTY, MODE_LOCAL);
    isLocal = MODE_LOCAL.equalsIgnoreCase(mode);
    LOGGER.info("Running Rollback Integration Test in {} mode.", mode);

    if (isLocal) {
      setupLocalEnvironment();
    } else {
      setupDataflowEnvironment();
    }

    spannerClient =
        SpannerClient.builder()
            .gcpProjectId(projectId)
            .spannerInstanceId(instanceId)
            .spannerDatabaseId(databaseId)
            .nodeTableName("Node")
            .edgeTableName("Edge")
            .timeSeriesTableName("TimeSeries")
            .observationTableName("Observation")
            .numShards(1)
            .emulatorHost(emulatorHost)
            .build();

    SpannerDatabaseInitializer.validateOrInitializeDatabase(
        projectId, instanceId, databaseId, emulatorHost);

    // Clear tables
    DatabaseId dbId = DatabaseId.of(projectId, instanceId, databaseId);
    dbClient = spanner.getDatabaseClient(dbId);
    List<String> tables = List.of("Observation", "TimeSeries", "Edge", "Node", "KeyValueStore");
    dbClient
        .readWriteTransaction()
        .run(
            transaction -> {
              tables.forEach(
                  t -> transaction.executeUpdate(Statement.of("DELETE FROM " + t + " WHERE true")));
              return null;
            });
  }

  @SuppressWarnings("resource")
  private void setupLocalEnvironment() throws Exception {
    emulatorHost = System.getenv("SPANNER_EMULATOR_HOST");
    if (emulatorHost == null) {
      emulatorHost = System.getProperty("spanner.emulator.host");
    }

    if (emulatorHost != null) {
      LOGGER.info("Using existing Spanner Emulator at {}", emulatorHost);
    } else {
      if (spannerEmulator == null) {
        spannerEmulator =
            new GenericContainer<>(
                    DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:latest"))
                .withExposedPorts(9010, 9020);
        spannerEmulator.start();
      }
      emulatorHost = spannerEmulator.getHost() + ":" + spannerEmulator.getMappedPort(9010);
      LOGGER.info("Started Spanner Emulator via Testcontainers at {}", emulatorHost);
    }
    projectId = "test-project";
    instanceId = "test-instance";
    databaseId = "test-db";

    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder().setProjectId(projectId);
    optionsBuilder.setEmulatorHost(emulatorHost);
    optionsBuilder.setCredentials(NoCredentials.getInstance());
    spanner = optionsBuilder.build().getService();

    // Create Instance in Emulator
    String configId = "emulator-config";
    InstanceInfo instanceInfo =
        InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
            .setDisplayName("Test Instance")
            .setNodeCount(1)
            .setInstanceConfigId(InstanceConfigId.of(projectId, configId))
            .build();
    try {
      spanner.getInstanceAdminClient().createInstance(instanceInfo).get();
    } catch (Exception e) {
      // Instance already exists, ignore.
    }
  }

  private void setupDataflowEnvironment() {
    projectId = System.getProperty("projectId", "datcom-ci");
    instanceId = System.getProperty("instanceId", "datcom-spanner-test");
    databaseId = System.getProperty("databaseId", "dc-test-db");
    gcsBucket = System.getProperty("gcsBucket", "datcom-ci-test");
    tempLocation = "gs://" + gcsBucket + "/dataflow/temp";
    region = System.getProperty("region", "us-central1");
    emulatorHost = null;

    SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
    spanner = options.getService();
  }

  @After
  public void tearDown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  @Test
  public void testRollback_restoresBaselineAndCleansCorruptedState() throws Exception {
    DatabaseId dbId = DatabaseId.of(projectId, instanceId, databaseId);
    DatabaseClient dbClient = spanner.getDatabaseClient(dbId);

    // 1. Seed Clean Baseline State (V1)
    Timestamp tPre = dbClient.write(SpannerTestData.V1_BASELINE_MUTATIONS);
    LOGGER.info("Seeded V1 baseline at commit timestamp: {}", tPre);
    Thread.sleep(1000);

    // 2. Corrupt Database State (V2 Failed Run)
    dbClient.write(SpannerTestData.V2_CORRUPTED_MUTATIONS);
    LOGGER.info("Corrupted database with V2 dirty records");

    // 3. Launch RollbackPipeline
    runRollbackPipeline(tPre);
    LOGGER.info("Rollback pipeline finished execution. Verifying assertions...");

    // 4. Verification Assertions
    assertNode(dbClient, SpannerTestData.SUBJECT_ID_CA, "California", List.of("State", "Place"));
    assertNodeDeleted(dbClient, SpannerTestData.SUBJECT_ID_DIRTY);
    assertObservationRestoredAndDirtyDeleted(dbClient);
    assertKeyValueRestored(dbClient);
  }

  private void runRollbackPipeline(Timestamp tPre) {
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setIsRollback(true);
    options.setRollbackTimestamp(tPre.toString());
    options.setImportList("TestImport");
    options.setIsBaseDc(true);
    options.setProjectId(projectId);
    options.setSpannerInstanceId(instanceId);
    options.setSpannerDatabaseId(databaseId);
    options.setEmulatorHost(emulatorHost);

    if (!isLocal) {
      options.setRunner(DataflowRunner.class);
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      dataflowOptions.setProject(projectId);
      dataflowOptions.setTempLocation(tempLocation);
      dataflowOptions.setRegion(region);
    }

    Pipeline pipeline = Pipeline.create(options);
    RollbackPipeline.buildPipeline(pipeline, options, spannerClient);
    pipeline.run().waitUntilFinish();
  }

  private void assertNode(
      DatabaseClient dbClient, String subjectId, String expectedName, List<String> expectedTypes) {
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    String.format(
                        "SELECT name, types FROM Node WHERE subject_id = '%s'", subjectId)))) {
      assertTrue(String.format("Node %s should exist", subjectId), rs.next());
      assertEquals(expectedName, rs.getString("name"));
      assertEquals(expectedTypes, rs.getStringList("types"));
    }
  }

  private void assertNodeDeleted(DatabaseClient dbClient, String subjectId) {
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    String.format(
                        "SELECT subject_id FROM Node WHERE subject_id = '%s'", subjectId)))) {
      assertFalse(String.format("Dirty node %s should be deleted", subjectId), rs.next());
    }
  }

  private void assertObservationRestoredAndDirtyDeleted(DatabaseClient dbClient) {
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    String.format(
                        "SELECT date, value FROM Observation WHERE variable_measured = '%s'",
                        SpannerTestData.STAT_VAR)))) {
      assertTrue("Observation 2020 should exist", rs.next());
      assertEquals(SpannerTestData.DATE_2020, rs.getString("date"));
      assertEquals(SpannerTestData.VALUE_2020, rs.getString("value"));
      assertFalse("Observation 2021 should NOT exist", rs.next());
    }
  }

  private void assertKeyValueRestored(DatabaseClient dbClient) {
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    String.format(
                        "SELECT value FROM KeyValueStore WHERE key = '%s' AND provenance = '%s'",
                        SpannerTestData.STAT_VAR, SpannerTestData.PROVENANCE)))) {
      assertTrue("KeyValueStore should exist", rs.next());
      assertEquals(
          SpannerTestData.KV_VALUE_BASELINE.replace(" ", ""), rs.getJson("value").replace(" ", ""));
    }
  }

  @Test
  public void testSchemaCoverage_allWritableColumnsAreMappedInSpannerClient() {
    // 1. Query INFORMATION_SCHEMA for all columns in the 6 restored tables
    Map<String, Set<String>> writableColsByTable = new HashMap<>();
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT table_name, column_name, is_generated FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE table_schema = '' AND table_name IN "
                        + "('Node', 'Edge', 'TimeSeries', 'Observation', 'KeyValueStore', 'NodeEmbedding')"))) {
      while (rs.next()) {
        String tableName = rs.getString("table_name");
        String colName = rs.getString("column_name");
        String isGenerated = rs.getString("is_generated");
        if ("ALWAYS".equalsIgnoreCase(isGenerated)) {
          continue; // Skip STORED generated columns (e.g. TimeSeries.entity1)
        }
        writableColsByTable.computeIfAbsent(tableName, k -> new HashSet<>()).add(colName);
      }
    }

    // 2. Validate against canonical record model writable column sets
    Map<String, Set<String>> rollbackColsByTable =
        Map.of(
            "Node", NodeRecord.WRITABLE_COLUMNS,
            "Edge", EdgeRecord.WRITABLE_COLUMNS,
            "TimeSeries", TimeSeriesRecord.WRITABLE_COLUMNS,
            "Observation", ObservationRecord.WRITABLE_COLUMNS,
            "KeyValueStore", KeyValueStoreRecord.WRITABLE_COLUMNS,
            "NodeEmbedding", NodeEmbeddingRecord.WRITABLE_COLUMNS);

    for (Map.Entry<String, Set<String>> entry : writableColsByTable.entrySet()) {
      String tableName = entry.getKey();
      Set<String> schemaCols = entry.getValue();
      Set<String> rollbackCols = rollbackColsByTable.get(tableName);
      assertNotNull(
          "Table " + tableName + " must have mapped writable columns in record model",
          rollbackCols);

      Set<String> missingCols = Sets.difference(schemaCols, rollbackCols);
      assertTrue(
          String.format(
              "SCHEMA DRIFT DETECTED IN TABLE '%s': Column(s) %s exist in schema but are NOT handled in SpannerClient/RollbackPipeline! "
                  + "Update %sRecord.WRITABLE_COLUMNS and restore mappers.",
              tableName, missingCols, tableName),
          missingCols.isEmpty());
    }
  }
}
