package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class LocalProgressTrackerTest {
  @Test
  public void recordRow_logsAtConfiguredInterval() {
    List<String> messages = new ArrayList<>();
    AtomicLong nowMillis = new AtomicLong(1_000L);
    LocalProgressTracker tracker =
        new LocalProgressTracker("DirectRunner", 2, 0, messages::add, nowMillis::get, null);

    tracker.recordRow(3, 7);
    tracker.recordRow(5, 11);

    assertEquals(1, messages.size());
    assertTrue(messages.get(0).contains("DirectRunner progress"));
    assertTrue(messages.get(0).contains("source_rows=2"));
    assertTrue(messages.get(0).contains("timeseries_attribute_rows=8"));
    assertTrue(messages.get(0).contains("stat_var_observation_rows=18"));
  }

  @Test
  public void logHeartbeatNow_reportsNoRowsYet() {
    List<String> messages = new ArrayList<>();
    AtomicLong nowMillis = new AtomicLong(31_000L);
    LocalProgressTracker tracker =
        new LocalProgressTracker("validator", 0, 30, messages::add, nowMillis::get, null);

    tracker.logHeartbeatNow();

    assertEquals(1, messages.size());
    assertTrue(messages.get(0).contains("validator heartbeat"));
    assertTrue(messages.get(0).contains("source_rows=0"));
    assertTrue(messages.get(0).contains("no_source_rows_yet=true"));
  }

  @Test
  public void recordValidatorFlush_logsMutationCountAndTimestamp() {
    List<String> messages = new ArrayList<>();
    LocalProgressTracker tracker =
        new LocalProgressTracker("validator", 1, 0, messages::add, System::currentTimeMillis, null);

    tracker.recordValidatorFlush(12, Timestamp.parseTimestamp("2026-04-23T00:00:00Z"));

    assertEquals(1, messages.size());
    assertTrue(messages.get(0).contains("validator flush"));
    assertTrue(messages.get(0).contains("mutations=12"));
    assertTrue(messages.get(0).contains("2026-04-23T00:00:00Z"));
  }
}
