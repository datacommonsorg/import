package org.datacommons.ingestion.pipeline;

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
import java.util.UUID;
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
  private String importName = "TestImport-" + UUID.randomUUID().toString();
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
            .observationTableName("Observation")
            .numShards(1)
            .emulatorHost(emulatorHost)
            .build();
    spannerClient.createDatabase();

    // Clear tables
    DatabaseId dbId = DatabaseId.of(projectId, instanceId, databaseId);
    DatabaseClient dbClient = spanner.getDatabaseClient(dbId);
    dbClient
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(Statement.of("DELETE FROM Node WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM Edge WHERE true"));
              transaction.executeUpdate(Statement.of("DELETE FROM Observation WHERE true"));
              return null;
            });
  }

  @SuppressWarnings("resource")
  private void setupLocalEnvironment() throws Exception {
    if (spannerEmulator == null) {
      spannerEmulator =
          new GenericContainer<>(
                  DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:latest"))
              .withExposedPorts(9010, 9020)
              .withReuse(true);
      spannerEmulator.start();
    }

    emulatorHost = spannerEmulator.getHost() + ":" + spannerEmulator.getMappedPort(9010);
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
    tempLocation = System.getProperty("tempLocation", "gs://datcom-ci-test/dataflow/temp");
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

    if (isLocal) {
      // Local Mode: Use local file
      File mcfFile = tempFolder.newFile("graph.mcf");
      String mcfContent =
          "Node: dcid:TestNode\n" + "typeOf: schema:Thing\n" + "name: \"" + nodeNameValue + "\"\n";
      Files.write(mcfFile.toPath(), mcfContent.getBytes(StandardCharsets.UTF_8));

      graphPath = mcfFile.getPath();

    } else {
      // Dataflow Mode: Use GCS
      String mcfContent =
          "Node: dcid:TestNode\n" + "typeOf: schema:Thing\n" + "name: \"" + nodeNameValue + "\"\n";

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
  }
}
