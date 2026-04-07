package org.datacommons.ingestion.spanner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
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
    DatabaseClient dbClient = mock(DatabaseClient.class);
    ReadContext readContext = mock(ReadContext.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(dbClient.singleUse()).thenReturn(readContext);
    when(readContext.executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);

    boolean exists = spannerClient.checkTableExists(dbClient, "Node");
    assertTrue(exists);

    verify(readContext).executeQuery(any(Statement.class));
  }

  @Test
  public void testCheckTableExists_False() {
    DatabaseClient dbClient = mock(DatabaseClient.class);
    ReadContext readContext = mock(ReadContext.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(dbClient.singleUse()).thenReturn(readContext);
    when(readContext.executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    boolean exists = spannerClient.checkTableExists(dbClient, "NonExistentTable");
    assertFalse(exists);

    verify(readContext).executeQuery(any(Statement.class));
  }
}
