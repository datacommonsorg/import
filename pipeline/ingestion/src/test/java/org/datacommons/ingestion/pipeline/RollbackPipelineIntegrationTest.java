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
import static org.junit.Assert.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.datacommons.ingestion.spanner.SpannerClient;
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

  public static GenericContainer<?> spannerEmulator;

  private Spanner spanner;
  private String projectId = "test-project";
  private String instanceId = "test-instance";
  private String databaseId = "test-db";
  private String emulatorHost;
  private SpannerClient spannerClient;

  @Before
  public void setUp() throws Exception {
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
      LOGGER.info("Started Spanner Emulator at {}", emulatorHost);
    }

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
    DatabaseClient dbClient = spanner.getDatabaseClient(dbId);
    dbClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(Statement.of("DELETE FROM Observation WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM TimeSeries WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM Edge WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM Node WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM KeyValueStore WHERE true"));
              return null;
            });
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
    Timestamp tPre =
        dbClient.write(
            Arrays.asList(
                Mutation.newInsertOrUpdateBuilder("Node")
                    .set("subject_id")
                    .to("geoId/06")
                    .set("name")
                    .to("California")
                    .set("types")
                    .toStringArray(List.of("State", "Place"))
                    .set("last_update_timestamp")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("Edge")
                    .set("subject_id")
                    .to("geoId/06")
                    .set("predicate")
                    .to("typeOf")
                    .set("object_id")
                    .to("Place")
                    .set("provenance")
                    .to("dc/base/TestImport")
                    .set("last_update_timestamp")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("TimeSeries")
                    .set("variable_measured")
                    .to("Count_Person")
                    .set("extra_entities_id")
                    .to("")
                    .set("facet_id")
                    .to("F1")
                    .set("entities")
                    .to(Value.json("{\"entity1\":\"geoId/06\"}"))
                    .set("facet")
                    .to(Value.json("{\"provenance\":\"dc/base/TestImport\"}"))
                    .set("last_update_timestamp")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("Observation")
                    .set("variable_measured")
                    .to("Count_Person")
                    .set("entity1")
                    .to("geoId/06")
                    .set("extra_entities_id")
                    .to("")
                    .set("facet_id")
                    .to("F1")
                    .set("date")
                    .to("2020")
                    .set("value")
                    .to("39000000")
                    .set("last_update_timestamp")
                    .to(Value.COMMIT_TIMESTAMP)
                    .build(),
                Mutation.newInsertOrUpdateBuilder("KeyValueStore")
                    .set("type")
                    .to("ProvenanceSummary")
                    .set("key")
                    .to("Count_Person")
                    .set("provenance")
                    .to("dc/base/TestImport")
                    .set("value")
                    .to(Value.json("{\"obs_count\": 1}"))
                    .build()));

    LOGGER.info("Seeded V1 baseline at commit timestamp: {}", tPre);
    Thread.sleep(1000);

    // 2. Corrupt Database State (V2 Failed Run)
    dbClient.write(
        Arrays.asList(
            // Corrupt existing node
            Mutation.newInsertOrUpdateBuilder("Node")
                .set("subject_id")
                .to("geoId/06")
                .set("name")
                .to("Corrupted California")
                .set("types")
                .toStringArray(List.of("CorruptedType"))
                .set("last_update_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            // Add new dirty node
            Mutation.newInsertOrUpdateBuilder("Node")
                .set("subject_id")
                .to("geoId/999")
                .set("name")
                .to("Dirty Node")
                .set("types")
                .toStringArray(List.of("Place"))
                .set("last_update_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            // Add dirty edge referencing dirty node
            Mutation.newInsertOrUpdateBuilder("Edge")
                .set("subject_id")
                .to("geoId/999")
                .set("predicate")
                .to("typeOf")
                .set("object_id")
                .to("Place")
                .set("provenance")
                .to("dc/base/TestImport")
                .set("last_update_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            // Add dirty observation
            Mutation.newInsertOrUpdateBuilder("Observation")
                .set("variable_measured")
                .to("Count_Person")
                .set("entity1")
                .to("geoId/06")
                .set("extra_entities_id")
                .to("")
                .set("facet_id")
                .to("F1")
                .set("date")
                .to("2021")
                .set("value")
                .to("40000000")
                .set("last_update_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
            // Corrupt KeyValueStore
            Mutation.newInsertOrUpdateBuilder("KeyValueStore")
                .set("type")
                .to("ProvenanceSummary")
                .set("key")
                .to("Count_Person")
                .set("provenance")
                .to("dc/base/TestImport")
                .set("value")
                .to(Value.json("{\"obs_count\": 999}"))
                .build()));

    LOGGER.info("Corrupted database with V2 dirty records");

    // 3. Launch RollbackPipeline
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setIsRollback(true);
    options.setRollbackTimestamp(tPre.toString());
    options.setImportList("TestImport");
    options.setIsBaseDc(true);
    options.setProjectId(projectId);
    options.setSpannerInstanceId(instanceId);
    options.setSpannerDatabaseId(databaseId);
    options.setEmulatorHost(emulatorHost);

    Pipeline pipeline = Pipeline.create(options);
    RollbackPipeline.buildPipeline(pipeline, options, spannerClient);
    pipeline.run().waitUntilFinish();

    LOGGER.info("Rollback pipeline finished execution. Verifying assertions...");

    // 4. Verification Assertions
    // Assert 1: Restored node geoId/06 has original name & types
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of("SELECT name, types FROM Node WHERE subject_id = 'geoId/06'"))) {
      assertTrue("Node geoId/06 should exist", rs.next());
      assertEquals("California", rs.getString("name"));
      assertEquals(List.of("State", "Place"), rs.getStringList("types"));
    }

    // Assert 2: Newly added dirty node geoId/999 was deleted
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of("SELECT subject_id FROM Node WHERE subject_id = 'geoId/999'"))) {
      assertFalse("Dirty node geoId/999 should be deleted", rs.next());
    }

    // Assert 3: Dirty observation (2021) deleted, original (2020) restored
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT date, value FROM Observation WHERE variable_measured = 'Count_Person'"))) {
      assertTrue("Observation 2020 should exist", rs.next());
      assertEquals("2020", rs.getString("date"));
      assertEquals("39000000", rs.getString("value"));
      assertFalse("Observation 2021 should NOT exist", rs.next());
    }

    // Assert 4: KeyValueStore has original value restored
    try (ResultSet rs =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT value FROM KeyValueStore WHERE key = 'Count_Person' AND provenance = 'dc/base/TestImport'"))) {
      assertTrue("KeyValueStore should exist", rs.next());
      assertEquals("{\"obs_count\":1}", rs.getJson("value").replace(" ", ""));
    }
  }
}
