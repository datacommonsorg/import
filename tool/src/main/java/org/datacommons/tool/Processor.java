// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.tool;

import freemarker.template.TemplateException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.OptimizedMcfGraph;
import org.datacommons.util.*;

public class Processor {
  private static final Logger logger = LogManager.getLogger(Processor.class);
  private final Args args;
  private ExistenceChecker existenceChecker;
  private ExternalIdResolver idResolver;
  private StatChecker statChecker;
  private StatVarState statVarState;
  private final List<Mcf.McfGraph> nodesForVariousChecks = new ArrayList<>();
  private final ExecutorService execService;
  private final LogWrapper logCtx;
  private HttpClient httpClient;

  public static Integer process(Args args) throws IOException, TemplateException {
    Integer retVal = 0;
    Processor processor = new Processor(args);
    try {
      // Load all the instance MCFs into memory, so we can do existence checks, resolution, etc.
      if (args.doExistenceChecks) {
        logger.info("Loading Instance MCF files into memory");
      } else {
        logger.info("Loading and Checking Instance MCF files (without Existence checks)");
      }
      processor.processNodes(Mcf.McfType.INSTANCE_MCF);

      // Perform existence checks.
      if (args.doExistenceChecks) {
        logger.info("Checking Instance MCF nodes (with Existence checks)");
        // NOTE: If doExistenceChecks is true, we do a checkNodes() call *after* all instance MCF
        // files are processed (via processNodes). This is so that the newly added schema, StatVar,
        // etc. are known to the Existence Checker first, before existence checks are performed.
        processor.checkNodes();
      }

      List<Mcf.McfGraph> nodesForStatProcessing = processor.nodesForVariousChecks;
      if (args.resolutionMode != Args.ResolutionMode.NONE) {
        if (args.resolutionMode == Args.ResolutionMode.FULL) {
          // Find external IDs from in-memory MCF nodes and CSVs, and map them to DCIDs.
          processor.lookupExternalIds();
        }

        // Having looked up the external IDs, resolve the instances.
        logger.info("Resolving Instance MCF nodes");
        nodesForStatProcessing = List.of(processor.resolveNodes());

        // Resolution for table nodes will happen inside processTables().
      }

      // Add relevant nodes from instance MCFs to statChecker and check for value inconsistencies.
      processor.processStats(nodesForStatProcessing);

      if (!args.fileGroup.getCsvs().isEmpty()) {
        String threadStr = "(with numThreads=" + args.numThreads + ")";
        // Process all the tables.
        if (args.resolutionMode == Args.ResolutionMode.FULL) {
          logger.info("Re-loading, Checking and Resolving Table MCF files " + threadStr);
        } else if (args.resolutionMode == Args.ResolutionMode.LOCAL) {
          logger.info("Loading, Checking and Resolving Table MCF files " + threadStr);
        } else {
          logger.info("Loading and Checking Table MCF files " + threadStr);
        }
        processor.processTables();
      } else if (args.fileGroup.getTmcfs() != null) {
        // Sanity check the TMCF nodes.
        logger.info("Loading and Checking Template MCF files");
        processor.processNodes(Mcf.McfType.TEMPLATE_MCF);
      }

      // We've been adding stats to statChecker all along, now do the actual check.
      processor.checkStats();
    } catch (DCTooManyFailuresException | InterruptedException ex) {
      // Only for DCTooManyFailuresException, we will dump the logCtx and exit.
      logger.error("Aborting prematurely, see report.json.");
      retVal = -1;
    }
    processor.logCtx.persistLog();
    if (args.generateSummaryReport) {
      SummaryReportGenerator.generateReportSummary(
          args.outputDir,
          processor.logCtx.getLog(),
          processor.statChecker.getSVSummaryMap(),
          processor.statChecker.getPlaceSeriesSummaryMap());
    }
    return retVal;
  }

  private Processor(Args args) {
    logger.info("Command options: " + args.toString());

    this.args = args;
    this.logCtx =
        new LogWrapper(Debug.Log.newBuilder().setCommandArgs(args.toProto()), args.outputDir);

    // we initialize an httpClient regardless of args.doExistenceChecks
    // because other features might still make API calls
    this.httpClient = HttpClient.newHttpClient();
    if (args.doExistenceChecks) {
      existenceChecker = new ExistenceChecker(this.httpClient, args.verbose, logCtx);
    }
    if (args.resolutionMode == Args.ResolutionMode.FULL) {
      idResolver =
          new ExternalIdResolver(
              this.httpClient, args.doCoordinatesResolution, args.verbose, logCtx);
    }
    statVarState = new StatVarState(this.httpClient, logCtx);
    if (args.doStatChecks) {
      Set<String> samplePlaces =
          args.samplePlaces == null ? null : new HashSet<>(args.samplePlaces);
      statChecker =
          new StatChecker(
              logCtx, samplePlaces, statVarState, existenceChecker, args.checkMeasurementResult);
    }
    execService = Executors.newFixedThreadPool(args.numThreads);
  }

  private void processNodes(Mcf.McfType type)
      throws IOException, DCTooManyFailuresException, InterruptedException {
    if (type == Mcf.McfType.INSTANCE_MCF) {
      for (var f : args.fileGroup.getMcfs()) {
        processNodes(type, f);
      }
    } else {
      for (var f : args.fileGroup.getTmcfs()) {
        processNodes(type, f);
      }
    }
  }

  private void processNodes(Mcf.McfType type, File file)
      throws IOException, DCTooManyFailuresException, InterruptedException {
    long numNodesProcessed = 0;
    if (args.verbose) logger.info("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      n = McfMutator.mutate(n.toBuilder(), logCtx);

      if (idResolver != null && type == Mcf.McfType.INSTANCE_MCF) {
        idResolver.addLocalGraph(n);
      }
      if (existenceChecker != null && type == Mcf.McfType.INSTANCE_MCF) {
        // Add instance MCF nodes to ExistenceChecker.  We load all the nodes up first
        // before we check them later in checkNodes().
        existenceChecker.addLocalGraph(n);
      } else {
        McfChecker.check(n, existenceChecker, statVarState, logCtx);
      }
      if (args.checkMeasurementResult && type == Mcf.McfType.INSTANCE_MCF) {
        // Add instance MCF nodes to StatVarState, which will remember the statType
        // of SVs so that we don't need to make HTTP requests for it for
        // measurementResult checks.
        statVarState.addLocalGraph(n);
      }
      if (existenceChecker != null
          || args.resolutionMode != Args.ResolutionMode.NONE
          || statChecker != null) {
        nodesForVariousChecks.add(n);
      }

      numNodesProcessed++;
      if (!logCtx.trackStatus(1, "nodes processed")) {
        throw new DCTooManyFailuresException("encountered too many failures");
      }
    }
    logger.info("Checked {} with {} nodes", file.getName(), numNodesProcessed);
  }

  private void processTables()
      throws IOException, DCTooManyFailuresException, InterruptedException {
    // Parallelize
    if (args.verbose) logger.info("TMCF " + args.fileGroup.getTmcf().getName());

    List<Callable<Void>> cbs = new ArrayList<Callable<Void>>(args.fileGroup.getCsvs().size());
    for (File csvFile : args.fileGroup.getCsvs()) {
      cbs.add(
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              processTable(csvFile);
              return null;
            }
          });
    }

    var futures = execService.invokeAll(cbs);
    for (var f : futures) {
      try {
        f.get();
      } catch (ExecutionException ex) {
        ex.getCause().printStackTrace();
        throw new DCTooManyFailuresException("Fatal error processing CSVs!");
      }
    }

    if (existenceChecker != null) existenceChecker.drainRemoteCalls();
  }

  // This is a thread-safe function invoked in parallel per CSV file.
  private void processTable(File csvFile)
      throws IOException, DCTooManyFailuresException, InterruptedException {
    if (args.verbose) logger.info("Checking CSV " + csvFile.getPath());
    TmcfCsvParser parser =
        TmcfCsvParser.init(
            args.fileGroup.getTmcf().getPath(),
            csvFile.getPath(),
            args.fileGroup.delimiter(),
            logCtx);
    // If there were too many failures when initializing the parser, parser will be null and we
    // don't want to continue processing.
    if (parser == null) {
      throw new DCTooManyFailuresException("processTables encountered too many failures");
    }
    WriterPair writerPair =
        new WriterPair(
            args,
            Args.OutputFileType.TABLE_MCF_NODES,
            Args.OutputFileType.FAILED_TABLE_MCF_NODES,
            csvFile);
    Mcf.McfGraph g;
    int numNodeSuccesses = 0, numPVSuccesses = 0, numRowSuccesses = 0, numRowsProcessed = 0;
    List<Mcf.McfGraph> graphList = new ArrayList<>();
    while ((g = parser.parseNextRow()) != null) {
      g = McfMutator.mutate(g.toBuilder(), logCtx);

      // This will set counters/messages in logCtx.
      boolean success =
          McfChecker.check(
              g,
              existenceChecker,
              statVarState,
              args.checkObservationAbout,
              args.allowNonNumericStatVarObservation,
              logCtx);

      if (args.resolutionMode != Args.ResolutionMode.NONE) {
        g = resolveCommon(g, writerPair);
      } else {
        if (args.outputFiles != null) {
          writerPair.writeSuccess(g);
        }
      }

      // Add relevant nodes from graph to statChecker and check for value inconsistencies.
      success &= processStats(List.of(g));
      if (success) {
        numRowSuccesses++;
        numNodeSuccesses += g.getNodesCount();
        for (var kv : g.getNodesMap().entrySet()) {
          numPVSuccesses += kv.getValue().getPvsCount();
        }
      }
      numRowsProcessed++;
      graphList.add(g);
      if (!logCtx.trackStatus(1, "rows processed")) {
        throw new DCTooManyFailuresException("encountered too many failures");
      }
    }
    logger.info("Writing optimized graph file");
    OptimizedMcfGraph og = GraphUtils.buildOptimizedMcfGraph(graphList);
    try (FileOutputStream output = new FileOutputStream("graph.tfrecord")) {
      og.writeTo(output);
    } catch (IOException e) {
      e.printStackTrace();
    }
    logCtx.incrementInfoCounterBy("NumRowSuccesses", numRowSuccesses);
    logCtx.incrementInfoCounterBy("NumNodeSuccesses", numNodeSuccesses);
    logCtx.incrementInfoCounterBy("NumPVSuccesses", numPVSuccesses);
    logger.info(
        "Checked "
            + (args.resolutionMode != Args.ResolutionMode.NONE ? "and Resolved " : "")
            + "CSV {} ({}"
            + " rows, {} nodes)",
        csvFile.getName(),
        numRowsProcessed,
        numNodeSuccesses);
    writerPair.close();
  }

  // Called only when existenceChecker is enabled.
  private void checkNodes() throws IOException, InterruptedException, DCTooManyFailuresException {
    for (Mcf.McfGraph n : nodesForVariousChecks) {
      McfChecker.check(n, existenceChecker, statVarState, logCtx);
      if (!logCtx.trackStatus(n.getNodesCount(), "nodes checked")) {
        throw new DCTooManyFailuresException("checkNodes encountered too many failures");
      }
    }
    existenceChecker.drainRemoteCalls();
  }

  // Called only when resolution is enabled.
  private Mcf.McfGraph resolveNodes() throws IOException {
    var writerPair =
        new WriterPair(
            args,
            Args.OutputFileType.INSTANCE_MCF_NODES,
            Args.OutputFileType.FAILED_INSTANCE_MCF_NODES,
            null);
    var result = resolveCommon(McfUtil.mergeGraphs(nodesForVariousChecks), writerPair);
    writerPair.close();
    return result;
  }

  private Mcf.McfGraph resolveCommon(Mcf.McfGraph mcfGraph, WriterPair writerPair)
      throws IOException {
    McfResolver resolver = new McfResolver(mcfGraph, args.verbose, idResolver, logCtx);
    resolver.resolve();
    if (args.outputFiles != null) {
      var resolved = resolver.resolvedGraph();
      if (!resolved.getNodesMap().isEmpty()) {
        writerPair.writeSuccess(resolved);
      }
      var failed = resolver.failedGraph();
      if (!failed.getNodesMap().isEmpty()) {
        writerPair.writeFailure(failed);
      }
    }
    return resolver.resolvedGraph();
  }

  // Process all the CSV tables to load all external IDs.
  private void lookupExternalIds()
      throws IOException, InterruptedException, DCTooManyFailuresException {
    LogWrapper dummyLog = new LogWrapper(Debug.Log.newBuilder());
    logger.info("Processing External IDs from Instance MCF nodes");
    for (var g : nodesForVariousChecks) {
      for (var idAndNode : g.getNodesMap().entrySet()) {
        idResolver.submitNode(idAndNode.getValue());
      }
      if (!dummyLog.trackStatus(g.getNodesCount(), "nodes processed")) {
        System.err.println("Too Many Errors ::\n" + dummyLog.dumpLog());
        throw new DCTooManyFailuresException("encountered too many failures");
      }
    }

    logger.info(
        "Loading and Processing External IDs from Table MCF files (with numThreads="
            + args.numThreads
            + ")");

    List<Callable<Void>> cbs = new ArrayList<Callable<Void>>(args.fileGroup.getCsvs().size());
    for (File csvFile : args.fileGroup.getCsvs()) {
      cbs.add(
          new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              lookupExternalIdsFromTable(csvFile, dummyLog);
              return null;
            }
          });
    }

    var futures = execService.invokeAll(cbs);
    for (var f : futures) {
      try {
        f.get();
      } catch (ExecutionException ex) {
        ex.getCause().printStackTrace();
        throw new DCTooManyFailuresException("Fatal error during resolution!");
      }
    }

    idResolver.drainRemoteCalls();
  }

  // This is a thread-safe function invoked in parallel per CSV file.
  private void lookupExternalIdsFromTable(File csvFile, LogWrapper dummyLog)
      throws DCTooManyFailuresException, IOException, InterruptedException {
    if (args.verbose) logger.info("Reading external IDs from CSV " + csvFile.getPath());
    TmcfCsvParser parser =
        TmcfCsvParser.init(
            args.fileGroup.getTmcf().getPath(),
            csvFile.getPath(),
            args.fileGroup.delimiter(),
            dummyLog);
    if (parser == null) return;
    Mcf.McfGraph g;
    while ((g = parser.parseNextRow()) != null) {
      for (var idAndNode : g.getNodesMap().entrySet()) {
        idResolver.submitNode(idAndNode.getValue());
      }
      if (!dummyLog.trackStatus(1, "rows processed")) {
        System.err.println("Too Many Errors ::\n" + dummyLog.dumpLog());
        throw new DCTooManyFailuresException("encountered too many failures");
      }
    }
  }

  // If statCheck is not null, Add stats from graphs and check for any value inconsistencies. Return
  // false if there are value inconsistencies found. All stats will still be added even if there are
  // value inconsistencies.
  private boolean processStats(List<Mcf.McfGraph> graphs) {
    if (statChecker == null) return true;
    boolean errorFound = false;
    for (Mcf.McfGraph g : graphs) {
      statChecker.extractStatsFromGraph(g);
      errorFound |= statChecker.checkSvObsInGraph(g);
    }
    return errorFound;
  }

  private void checkStats() throws IOException, InterruptedException {
    if (statChecker == null) return;
    logger.info("Performing stats checks");
    statChecker.check();
    statChecker.fetchSamplePlaceNames(httpClient);
  }

  private static class DCTooManyFailuresException extends Exception {
    public DCTooManyFailuresException(String message) {
      super(message);
    }
  }
}
