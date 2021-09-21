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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
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

  // TODO: Produce output MCF files in files corresponding to the input (like prod).
  // We separate output MCF based on *.mcf vs. TMCF/CSV inputs because they often represent
  // different things.  Input MCFs have schema/StatVars while TMCF/CSVs have stats.
  public enum OutputFileType {
    // Output MCF where *.mcf inputs get resolved into.
    INSTANCE_MCF_NODES,
    // Output MCF where failed nodes from *.mcf inputs flow into.
    FAILED_INSTANCE_MCF_NODES,
    // Output MCF where TMCF/CSV inputs get resolved into.
    TABLE_MCF_NODES,
    // Output MCF where TMCF/CSV failed nodes flow into.
    FAILED_TABLE_MCF_NODES,
  };

  public enum ResolutionMode {
    NONE,
    LOCAL,
    FULL,
  };

  static class Args {
    public boolean doExistenceChecks = false;
    public ResolutionMode resolutionMode = ResolutionMode.NONE;
    public boolean doStatChecks = false;
    public List<String> samplePlaces = null;
    public boolean verbose = false;
    public FileGroup fileGroup = null;
    public Map<OutputFileType, Path> outputFiles = null;
    public int numThreads = 1;
    LogWrapper logCtx = null;
  }

  public static Integer process(Args args) throws IOException {
    Integer retVal = 0;
    try {
      Processor processor = new Processor(args);

      // Load all the instance MCFs into memory, so we can do existence checks, resolution, etc.
      if (args.doExistenceChecks) {
        logger.info("Loading Instance MCF files into memory");
      } else {
        logger.info("Loading and Checking Instance MCF files");
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

      if (args.resolutionMode != ResolutionMode.NONE) {
        if (args.resolutionMode == ResolutionMode.FULL) {
          // Find external IDs from in-memory MCF nodes and CSVs, and map them to DCIDs.
          processor.lookupExternalIds();
        }

        // Having looked up the external IDs, resolve the instances.
        logger.info("Resolving Instance MCF nodes");
        Mcf.McfGraph resolvedGraph = processor.resolveNodes();

        // Add stats from resolved graph to statChecker.
        processor.addStats(List.of(resolvedGraph));

        // Resolution for table nodes will happen inside processTables().
      } else {
        // Add stats from graphs from nodesForVariousChecks to statChecker.
        processor.addStats(processor.nodesForVariousChecks);
      }

      if (!args.fileGroup.getCsvs().isEmpty()) {
        // Process all the tables.
        if (args.resolutionMode == ResolutionMode.FULL) {
          logger.info("Re-loading, Checking and Resolving Table MCF files");
        } else if (args.resolutionMode == ResolutionMode.LOCAL) {
          logger.info("Loading, Checking and Resolving Table MCF files");
        } else {
          logger.info("Loading and Checking Table MCF files");
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
    args.logCtx.persistLog();
    return retVal;
  }

  private Processor(Args args) {
    this.args = args;
    if (args.doExistenceChecks) {
      existenceChecker =
          new ExistenceChecker(HttpClient.newHttpClient(), args.verbose, args.logCtx);
    }
    if (args.resolutionMode == ResolutionMode.FULL) {
      idResolver = new ExternalIdResolver(HttpClient.newHttpClient(), args.verbose, args.logCtx);
    }
    statVarState = new StatVarState(args.logCtx);
    if (args.doStatChecks) {
      Set<String> samplePlaces =
          args.samplePlaces == null ? null : new HashSet<>(args.samplePlaces);
      statChecker = new StatChecker(args.logCtx, samplePlaces, args.verbose);
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
    McfParser parser = McfParser.init(type, file.getPath(), false, args.logCtx);
    Mcf.McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      n = McfMutator.mutate(n.toBuilder(), args.logCtx);

      if (existenceChecker != null && type == Mcf.McfType.INSTANCE_MCF) {
        // Add instance MCF nodes to ExistenceChecker.  We load all the nodes up first
        // before we check them later in checkNodes().
        existenceChecker.addLocalGraph(n);
      } else {
        McfChecker.check(n, existenceChecker, statVarState, args.logCtx);
      }
      if (existenceChecker != null
          || args.resolutionMode != ResolutionMode.NONE
          || statChecker != null) {
        nodesForVariousChecks.add(n);
      }

      numNodesProcessed++;
      if (!args.logCtx.trackStatus(1, "nodes processed")) {
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
    List<Future<Void>> futures = execService.invokeAll(cbs);

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
            args.logCtx);
    // If there were too many failures when initializing the parser, parser will be null and we
    // don't want to continue processing.
    if (parser == null) {
      throw new DCTooManyFailuresException("processTables encountered too many failures");
    }
    WriterPair writerPair =
        new WriterPair(
            OutputFileType.TABLE_MCF_NODES, OutputFileType.FAILED_TABLE_MCF_NODES, csvFile);
    Mcf.McfGraph g;
    long numNodesProcessed = 0, numRowsProcessed = 0;
    while ((g = parser.parseNextRow()) != null) {
      g = McfMutator.mutate(g.toBuilder(), args.logCtx);

      // This will set counters/messages in logCtx.
      boolean success = McfChecker.check(g, existenceChecker, statVarState, args.logCtx);
      if (success) {
        args.logCtx.incrementInfoCounterBy("NumRowSuccesses", 1);
      }
      if (args.resolutionMode != ResolutionMode.NONE) {
        g = resolveCommon(g, writerPair);
      } else {
        if (args.outputFiles != null) {
          writerPair.writeSuccess(g);
        }
      }

      // This will extract and save time series info from the relevant nodes from g and save it
      // to statChecker.
      if (statChecker != null) {
        statChecker.extractSeriesInfoFromGraph(g);
      }
      numNodesProcessed += g.getNodesCount();
      numRowsProcessed++;
      if (!args.logCtx.trackStatus(1, "rows processed")) {
        throw new DCTooManyFailuresException("encountered too many failures");
      }
    }
    logger.info(
        "Checked "
            + (args.resolutionMode != ResolutionMode.NONE ? "and Resolved " : "")
            + "CSV {} ({}"
            + " rows, {} nodes)",
        csvFile.getName(),
        numRowsProcessed,
        numNodesProcessed);
    writerPair.close();
  }

  // Encloses a pair of writers for success and corresponding failure types, and creates the file
  // on-demand when a write comes in.
  class WriterPair {
    private final OutputFileType successType;
    private final OutputFileType failureType;
    private final File csvFile;
    private BufferedWriter successWriter = null;
    private BufferedWriter failureWriter = null;

    public WriterPair(OutputFileType successType, OutputFileType failureType, File csvFile)
        throws IOException {
      this.successType = successType;
      this.failureType = failureType;
      this.csvFile = csvFile;
    }

    public void writeSuccess(Mcf.McfGraph g) throws IOException {
      if (successWriter == null) {
        successWriter = newWriter(successType);
      }
      successWriter.write(McfUtil.serializeMcfGraph(g, false));
    }

    public void writeFailure(Mcf.McfGraph g) throws IOException {
      if (failureWriter == null) {
        failureWriter = newWriter(failureType);
      }
      failureWriter.write(McfUtil.serializeMcfGraph(g, false));
    }

    public void close() throws IOException {
      if (failureWriter != null) failureWriter.close();
      if (successWriter != null) successWriter.close();
    }

    private BufferedWriter newWriter(OutputFileType type) throws IOException {
      String filePath = args.outputFiles.get(type).toString();
      if (csvFile != null) {
        String fileSuffix = FilenameUtils.removeExtension(csvFile.getName()) + ".csv";
        filePath = FilenameUtils.removeExtension(filePath) + "_" + fileSuffix;
      }
      return new BufferedWriter(new FileWriter(filePath));
    }
  }

  // Called only when existenceChecker is enabled.
  private void checkNodes() throws IOException, InterruptedException, DCTooManyFailuresException {
    long numNodesChecked = 0;
    for (Mcf.McfGraph n : nodesForVariousChecks) {
      McfChecker.check(n, existenceChecker, statVarState, args.logCtx);
      numNodesChecked += n.getNodesCount();
      numNodesChecked++;
      if (!args.logCtx.trackStatus(n.getNodesCount(), "nodes checked")) {
        throw new DCTooManyFailuresException("checkNodes encountered too many failures");
      }
    }
    existenceChecker.drainRemoteCalls();
  }

  // Called only when resolution is enabled.
  private Mcf.McfGraph resolveNodes() throws IOException {
    var writerPair =
        new WriterPair(
            OutputFileType.INSTANCE_MCF_NODES, OutputFileType.FAILED_INSTANCE_MCF_NODES, null);
    var result = resolveCommon(McfUtil.mergeGraphs(nodesForVariousChecks), writerPair);
    writerPair.close();
    return result;
  }

  private Mcf.McfGraph resolveCommon(Mcf.McfGraph mcfGraph, WriterPair writerPair)
      throws IOException {
    McfResolver resolver = new McfResolver(mcfGraph, args.verbose, idResolver, args.logCtx);
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

    logger.info("Loading and Processing External IDs from Table MCF files");

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
    List<Future<Void>> futures = execService.invokeAll(cbs);

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

  // add stats from graphs to statChecker if statChecker is not null
  private void addStats(List<Mcf.McfGraph> graphs) {
    if (statChecker == null) return;
    for (Mcf.McfGraph g : graphs) {
      statChecker.extractSeriesInfoFromGraph(g);
    }
  }

  private void checkStats() {
    if (args.verbose) logger.info("Performing stats checks");
    if (statChecker == null) return;
    statChecker.check();
  }

  private static class DCTooManyFailuresException extends Exception {
    public DCTooManyFailuresException() {}

    public DCTooManyFailuresException(String message) {
      super(message);
    }
  }
}
