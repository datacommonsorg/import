package org.datacommons.ingestion.spanner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class SpannerClientTest {

  private SpannerClient spannerClient;

  @Before
  public void setUp() {
    spannerClient =
        SpannerClient.builder()
            .gcpProjectId("test-project")
            .spannerInstanceId("test-instance")
            .spannerDatabaseId("test-db")
            .build();
  }

  @Test
  public void testReadDdlStatements() throws IOException {
    List<String> ddlStatements = spannerClient.readDdlStatements();
    assertNotNull(ddlStatements);
    assertFalse(ddlStatements.isEmpty());

    // Verify that we have at least table creation statements
    boolean hasNodeTable = false;
    for (String ddl : ddlStatements) {
      if (ddl.contains("CREATE TABLE Node")) {
        hasNodeTable = true;
        break;
      }
    }
    assertTrue("Should contain Node table DDL", hasNodeTable);
  }

  @Test
  public void testCheckTableExists_True() {
    List<String> statements = new java.util.ArrayList<>();
    statements.add("CREATE TABLE Node");

    boolean exists = spannerClient.checkTableExists(statements, "Node");
    assertTrue(exists);
  }

  @Test
  public void testCheckTableExists_False() {
    List<String> statements = new java.util.ArrayList<>();
    statements.add("CREATE TABLE Edge");

    boolean exists = spannerClient.checkTableExists(statements, "Node");
  }

  @Test
  public void testCheckTableExists_FalsePositive() {
    List<String> statements = new java.util.ArrayList<>();
    statements.add("CREATE TABLE Node_Old");

    boolean exists = spannerClient.checkTableExists(statements, "Node");
    assertFalse(exists);
  }
}
