package org.datacommons.ingestion.spanner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.datacommons.ingestion.data.Node;
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
  public void testReadDdlStatements() {
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
    assertFalse(exists);
  }

  @Test
  public void testCheckTableExists_FalsePositive() {
    List<String> statements = new java.util.ArrayList<>();
    statements.add("CREATE TABLE Node_Old");

    boolean exists = spannerClient.checkTableExists(statements, "Node");
    assertFalse(exists);
  }

  @Test
  public void testToNodeMutation_RegularNode() {
    Node node =
        Node.builder()
            .subjectId("dcid:123")
            .value("value123")
            .name("Node Name")
            .types(List.of("Type1", "Type2"))
            .build();

    Mutation mutation = spannerClient.toNodeMutation(node);
    assertNotNull(mutation);
    assertEquals("Node", mutation.getTable());

    var mutationMap = mutation.asMap();
    assertEquals("dcid:123", mutationMap.get("subject_id").getString());
    assertEquals("value123", mutationMap.get("value").getString());
    assertEquals("Node Name", mutationMap.get("name").getString());
    assertEquals(List.of("Type1", "Type2"), mutationMap.get("types").getStringArray());
    assertEquals(Value.COMMIT_TIMESTAMP, mutationMap.get("last_update_timestamp"));
  }

  @Test
  public void testToNodeMutation_ProvisionalNode() {
    Node node = Node.builder().subjectId("dcid:456").types(List.of("ProvisionalNode")).build();

    Mutation mutation = spannerClient.toNodeMutation(node);
    assertNotNull(mutation);
    assertEquals("Node", mutation.getTable());

    var mutationMap = mutation.asMap();
    assertEquals("dcid:456", mutationMap.get("subject_id").getString());
    assertFalse(mutationMap.containsKey("value"));
    assertEquals(Value.COMMIT_TIMESTAMP, mutationMap.get("last_update_timestamp"));
  }
}
