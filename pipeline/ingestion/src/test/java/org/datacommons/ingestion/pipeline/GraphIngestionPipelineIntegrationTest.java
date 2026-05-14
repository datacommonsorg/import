package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for GraphIngestionPipeline.
 *
 * <p>This test verifies the end-to-end execution of the GraphIngestionPipeline. It can run in two
 * modes:
 *
 * <ul>
 *   <li>LOCAL: Uses Spanner Emulator and local files. Requires Docker.
 *   <li>DATAFLOW: Uses real Spanner and GCS. Requires GCP credentials and configuration.
 * </ul>
 *
 * <p>To run locally:
 *
 * <pre>
 * mvn test -Dtest=GraphIngestionPipelineIntegrationTest -DtestMode=LOCAL
 * </pre>
 *
 * <p>To run on Dataflow (with default configuration):
 *
 * <pre>
 * mvn test -Dtest=GraphIngestionPipelineIntegrationTest -DtestMode=DATAFLOW
 * </pre>
 */
@RunWith(JUnit4.class)
public class GraphIngestionPipelineIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GraphIngestionPipelineIntegrationTest.class);
  private static final String TEST_MODE_PROPERTY = "testMode";
  private static final String MODE_LOCAL = "LOCAL";
  private static final String MODE_DATAFLOW = "DATAFLOW";

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  // TestContainers for Local Mode
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
  private String importName = "TestImport";
  private String nodeNameValue = "Test Node Name";
  private SpannerClient spannerClient;

  @Before
  public void setUp() throws Exception {
    String mode = System.getProperty(TEST_MODE_PROPERTY, MODE_LOCAL);
    isLocal = MODE_LOCAL.equalsIgnoreCase(mode);
    LOGGER.info("Running Integration Test in {} mode.", mode);

    if (isLocal) {
      setupLocalEnvironment();
    } else {
      setupDataflowEnvironment();
    }

    // Create Database if it doesn't exist
    spannerClient =
        SpannerClient.builder()
            .gcpProjectId(projectId)
            .spannerInstanceId(instanceId)
            .spannerDatabaseId(databaseId)
            .nodeTableName("Node")
            .edgeTableName("Edge")
            .timeSeriesTableName("TimeSeries")
            .numShards(1)
            .emulatorHost(emulatorHost)
            .build();
    spannerClient.validateOrInitializeDatabase();

    // Clear tables
    DatabaseId dbId = DatabaseId.of(projectId, instanceId, databaseId);
    DatabaseClient dbClient = spanner.getDatabaseClient(dbId);
    dbClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(Statement.of("DELETE FROM Node WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM Edge WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM TimeSeries WHERE true"));
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
      if (!(e.getCause() instanceof com.google.cloud.spanner.SpannerException)) {
        throw e;
      }
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

    SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
    spanner = options.getService();
  }

  @After
  public void tearDown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  private void setPipelineOptions(IngestionPipelineOptions options) throws IOException {
    String graphPath;
    String mcfContent =
        "Node: dcid:TestNode\n"
            + "typeOf: schema:Thing\n"
            + "name: \""
            + nodeNameValue
            + "\"\n\n"
            + "Node: dcid:TestObs\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "observationAbout: dcid:TestNode\n"
            + "variableMeasured: dcs:TestVariable\n"
            + "value: 10\n"
            + "observationDate: \"2023\"\n"
            + "unit: dcs:TestUnit\n"
            + "measurementMethod: dcs:TestMethod\n\n"
            + "Node: dcid:TestObs2\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "observationAbout: dcid:TestNode\n"
            + "variableMeasured: dcs:TestVariable\n"
            + "value: 20\n"
            + "observationDate: \"2024\"\n"
            + "unit: dcs:TestUnit\n"
            + "measurementMethod: dcs:TestMethod\n";

    if (isLocal) {
      // Local Mode: Use local file
      File mcfFile = tempFolder.newFile("graph.mcf");
      Files.write(mcfFile.toPath(), mcfContent.getBytes(StandardCharsets.UTF_8));
      graphPath = mcfFile.getPath();
    } else {
      // Dataflow Mode: Use GCS
      String gcsFolder = "integration-test-input";
      Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
      BlobId blobId = BlobId.of(gcsBucket, gcsFolder + "/graph.mcf");
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
      storage.create(blobInfo, mcfContent.getBytes(StandardCharsets.UTF_8));

      options.setRunner(DataflowRunner.class);
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      dataflowOptions.setProject(projectId);
      dataflowOptions.setTempLocation(tempLocation);
      dataflowOptions.setRegion(region);
      options.setStorageBucketId(gcsBucket);
      graphPath = "gs://" + gcsBucket + "/" + gcsFolder + "/graph.mcf";
    }

    // Configure Import List
    JsonObject importObj = new JsonObject();
    importObj.addProperty("importName", importName);
    importObj.addProperty("graphPath", graphPath);

    JsonArray importArray = new JsonArray();
    importArray.add(importObj);

    options.setProjectId(projectId);
    options.setWriteObsGraph(false);
    options.setSpannerInstanceId(instanceId);
    options.setSpannerDatabaseId(databaseId);
    options.setNumShards(1);
    options.setImportList(importArray.toString());
    options.setInitializeDatabase(true);
  }

  @Test
  public void testPipelineExecution() {
    IngestionPipelineOptions options =
        PipelineOptionsFactory.create().as(IngestionPipelineOptions.class);

    // Common Options
    try {
      setPipelineOptions(options);
    } catch (IOException ex) {
      assertFalse(options.getImportList().isEmpty());
    }

    // Run Pipeline
    Pipeline pipeline = Pipeline.create(options);
    GraphIngestionPipeline.buildPipeline(pipeline, options, spannerClient);
    pipeline.run().waitUntilFinish();

    // Verify Spanner
    DatabaseId dbId = DatabaseId.of(projectId, instanceId, databaseId);
    DatabaseClient dbClient = spanner.getDatabaseClient(dbId);

    // Verify Node
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(Statement.of("SELECT * FROM Node where subject_id = 'TestNode'"))) {
      boolean found = false;
      while (resultSet.next()) {
        String subjectId = resultSet.getString("subject_id");
        String name = resultSet.getString("name");
        if ("TestNode".equals(subjectId) || "dcid:TestNode".equals(subjectId)) {
          found = true;
          assertTrue("Name should match", nodeNameValue.equals(name));
        }
      }
      assertTrue("Node should exist in Spanner", found);
    }

    // Verify Edges
    String expectedObjectId =
        org.datacommons.ingestion.util.PipelineUtils.generateObjectValueKey(nodeNameValue);

    // Verify 'name' edge
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM Edge WHERE subject_id = @subject_id AND predicate = @predicate")
                    .bind("subject_id")
                    .to("TestNode")
                    .bind("predicate")
                    .to("name")
                    .build())) {
      assertTrue("Name edge should exist", resultSet.next());
      assertEquals(
          "Name edge object_id should match", expectedObjectId, resultSet.getString("object_id"));
      assertEquals(
          "Name edge provenance should match",
          "dc/base/" + importName,
          resultSet.getString("provenance"));
      assertFalse("Should only have one name edge", resultSet.next());
    }

    // Verify 'typeOf' edge
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM Edge WHERE subject_id = @subject_id AND predicate = @predicate")
                    .bind("subject_id")
                    .to("TestNode")
                    .bind("predicate")
                    .to("typeOf")
                    .build())) {
      assertTrue("TypeOf edge should exist", resultSet.next());
      assertEquals("TypeOf edge object_id should match", "Thing", resultSet.getString("object_id"));
      assertEquals(
          "TypeOf edge provenance should match",
          "dc/base/" + importName,
          resultSet.getString("provenance"));
      assertFalse("Should only have one typeOf edge", resultSet.next());
    }

    // Verify TimeSeries
    String variableMeasured = null;
    String entity1 = null;
    String extraEntitiesId = null;
    String facetId = null;
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM TimeSeries WHERE variable_measured LIKE '%TestVariable' AND provenance = @provenance")
                    .bind("provenance")
                    .to("dc/base/" + importName)
                    .build())) {
      assertTrue("TimeSeries should exist", resultSet.next());
      variableMeasured = resultSet.getString("variable_measured");
      entity1 = resultSet.getString("entity1");
      extraEntitiesId = resultSet.getString("extra_entities_id");
      facetId = resultSet.getString("facet_id");
      assertTrue("entity1 should contain TestNode", entity1.contains("TestNode"));

      // Verify facet JSON contains unit and measurement method
      String facet = resultSet.getJson("facet");
      assertTrue("Unit should be found in attributes: " + facet, facet.contains("TestUnit"));
      assertTrue(
          "Measurement method should be found in attributes: " + facet,
          facet.contains("TestMethod"));

      assertFalse("Should only have ONE TimeSeries row (deduplication check)", resultSet.next());
    }

    // Verify Observation
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM Observation WHERE variable_measured = @variable_measured AND entity1 = @entity1 AND extra_entities_id = @extra_entities_id AND facet_id = @facet_id ORDER BY date ASC")
                    .bind("variable_measured")
                    .to(variableMeasured)
                    .bind("entity1")
                    .to(entity1)
                    .bind("extra_entities_id")
                    .to(extraEntitiesId)
                    .bind("facet_id")
                    .to(facetId)
                    .build())) {
      // First Observation (2023)
      assertTrue("First Observation should exist", resultSet.next());
      assertEquals("First Observation date should match", "2023", resultSet.getString("date"));
      String val1 = resultSet.getString("value");
      assertTrue("First Observation value should match", "10".equals(val1) || "10.0".equals(val1));

      // Second Observation (2024)
      assertTrue("Second Observation should exist", resultSet.next());
      assertEquals("Second Observation date should match", "2024", resultSet.getString("date"));
      String val2 = resultSet.getString("value");
      assertTrue("Second Observation value should match", "20".equals(val2) || "20.0".equals(val2));

      assertFalse("Should only have TWO Observation rows", resultSet.next());
    }
  }
}
