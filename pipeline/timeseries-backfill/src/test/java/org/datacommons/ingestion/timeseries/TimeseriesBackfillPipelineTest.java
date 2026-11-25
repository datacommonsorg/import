package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.util.OptionalInt;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.junit.Test;

public class TimeseriesBackfillPipelineTest {
  @Test
  public void options_haveLocalProgressDefaults() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);

    assertEquals(1000, options.getProgressEverySourceRows());
    assertEquals(30, options.getHeartbeatSeconds());
  }

  @Test
  public void shouldEnableLocalProgress_trueForDefaultAndDirectRunner() {
    TimeseriesBackfillOptions defaultOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    TimeseriesBackfillOptions directOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    directOptions.setRunner(DirectRunner.class);

    assertTrue(TimeseriesBackfillPipeline.shouldEnableLocalProgress(defaultOptions));
    assertTrue(TimeseriesBackfillPipeline.shouldEnableLocalProgress(directOptions));
  }

  @Test
  public void shouldEnableLocalProgress_falseForDataflowRunner() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setRunner(DataflowRunner.class);

    assertFalse(TimeseriesBackfillPipeline.shouldEnableLocalProgress(options));
  }

  @Test
  public void resolveProjectId_prefersProjectAndFallsBackToProjectId() {
    TimeseriesBackfillOptions projectOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    projectOptions.setProject("beam-project");
    projectOptions.setProjectId("legacy-project");

    TimeseriesBackfillOptions legacyOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    legacyOptions.setProject("");
    legacyOptions.setProjectId("legacy-project");

    assertEquals("beam-project", TimeseriesBackfillIO.resolveProjectId(projectOptions));
    assertEquals("legacy-project", TimeseriesBackfillIO.resolveProjectId(legacyOptions));
  }

  @Test
  public void options_leaveOptionalSpannerSinkFlagsUnsetByDefault() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);

    assertNull(options.getBatchSizeBytes());
    assertNull(options.getMaxNumRows());
    assertNull(options.getMaxNumMutations());
    assertNull(options.getGroupingFactor());
    assertNull(options.getCommitDeadlineSeconds());
  }

  @Test
  public void options_parseOptionalSpannerSinkFlagsFromArgs() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.fromArgs(
                "--batchSizeBytes=1024",
                "--maxNumRows=100",
                "--maxNumMutations=500",
                "--groupingFactor=20",
                "--commitDeadlineSeconds=45")
            .as(TimeseriesBackfillOptions.class);

    assertEquals(Long.valueOf(1024), options.getBatchSizeBytes());
    assertEquals(Integer.valueOf(100), options.getMaxNumRows());
    assertEquals(Integer.valueOf(500), options.getMaxNumMutations());
    assertEquals(Integer.valueOf(20), options.getGroupingFactor());
    assertEquals(Integer.valueOf(45), options.getCommitDeadlineSeconds());
  }

  @Test
  public void validateOptions_rejectsNonPositiveOptionalSpannerSinkFlags() {
    assertRejectsInvalidWriteOption("batchSizeBytes", "batchSizeBytes must be positive");
    assertRejectsInvalidWriteOption("maxNumRows", "maxNumRows must be positive");
    assertRejectsInvalidWriteOption("maxNumMutations", "maxNumMutations must be positive");
    assertRejectsInvalidWriteOption("groupingFactor", "groupingFactor must be positive");
    assertRejectsInvalidWriteOption(
        "commitDeadlineSeconds", "commitDeadlineSeconds must be positive");
  }

  @Test
  public void buildWrite_appliesOptionalSpannerSinkFlagsWhenProvided() throws Exception {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setProject("beam-project");
    options.setSpannerInstanceId("instance");
    options.setSpannerDatabaseId("database");
    options.setBatchSizeBytes(1024L);
    options.setMaxNumRows(100);
    options.setMaxNumMutations(500);
    options.setGroupingFactor(20);
    options.setCommitDeadlineSeconds(45);

    SpannerIO.Write write = TimeseriesBackfillIO.buildWrite(options);

    assertEquals(1024L, invokeLong(write, "getBatchSizeBytes"));
    assertEquals(100L, invokeLong(write, "getMaxNumRows"));
    assertEquals(500L, invokeLong(write, "getMaxNumMutations"));
    assertEquals(OptionalInt.of(20), invokeGroupingFactor(write));
    assertEquals(
        Duration.standardSeconds(45), invokeSpannerConfig(write).getCommitDeadline().get());
  }

  private static void assertRejectsInvalidWriteOption(String optionName, String messageFragment) {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    setInvalidWriteOption(options, optionName);

    try {
      TimeseriesBackfillOptionValidator.validateCommonOptions(options);
      fail("Expected IllegalArgumentException for " + optionName);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(messageFragment));
    }
  }

  private static void setInvalidWriteOption(TimeseriesBackfillOptions options, String optionName) {
    switch (optionName) {
      case "batchSizeBytes":
        options.setBatchSizeBytes(0L);
        return;
      case "maxNumRows":
        options.setMaxNumRows(0);
        return;
      case "maxNumMutations":
        options.setMaxNumMutations(0);
        return;
      case "groupingFactor":
        options.setGroupingFactor(0);
        return;
      case "commitDeadlineSeconds":
        options.setCommitDeadlineSeconds(0);
        return;
      default:
        throw new IllegalArgumentException("Unexpected option " + optionName);
    }
  }

  private static long invokeLong(SpannerIO.Write write, String methodName) throws Exception {
    Method method = SpannerIO.Write.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    return (long) method.invoke(write);
  }

  private static OptionalInt invokeGroupingFactor(SpannerIO.Write write) throws Exception {
    Method method = SpannerIO.Write.class.getDeclaredMethod("getGroupingFactor");
    method.setAccessible(true);
    return (OptionalInt) method.invoke(write);
  }

  private static SpannerConfig invokeSpannerConfig(SpannerIO.Write write) throws Exception {
    Method method = SpannerIO.Write.class.getDeclaredMethod("getSpannerConfig");
    method.setAccessible(true);
    return (SpannerConfig) method.invoke(write);
  }
}
