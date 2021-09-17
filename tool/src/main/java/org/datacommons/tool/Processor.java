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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.*;

public class Processor {
  private static final Logger logger = LogManager.getLogger(Processor.class);
  private final LogWrapper logCtx;
  private final FileGroup fileGroup;
  private final boolean verbose;
  // Set only for "genmcf"
  private final Map<OutputFileType, Path> outputFiles;
  private ExistenceChecker existenceChecker;
  private ExternalIdResolver idResolver;
  private StatChecker statChecker;
  private final Map<OutputFileType, BufferedWriter> writers = new HashMap<>();
  private final List<Mcf.McfGraph> nodesForVariousChecks = new ArrayList<>();

  // TODO: Produce output MCF files in files corresponding to the input (like prod).
  // We separate output MCF based on *.mcf vs. TMCF/CSV inputs because they often represent
  // different things.  Input MCFs have schema/StatVars while TMCF/CSVs have stats.
  public enum OutputFileType {
    // Output MCF where *.mcf inputs get resolved into.
    NODES,
    // Output MCF where failed nodes from *.mcf inputs flow into.
    FAILED_NODES,
    // Output MCF where TMCF/CSV inputs get resolved into.
    TABLE_NODES,
    // Output MCF where TMCF/CSV failed nodes flow into.
    FAILED_TABLE_NODES,
  };

  static class Args {
    public boolean doExistenceChecks = false;
    public boolean doResolution = false;
    public boolean doStatChecks = false;
    public List<String> samplePlaces = null;
    public boolean verbose = false;
    public FileGroup fileGroup = null;
    public Map<OutputFileType, Path> outputFiles = null;
    LogWrapper logCtx = null;
  }

  public static Integer process(Args args) throws IOException {
    Integer retVal = 0;
    try {
      Processor processor = new Processor(args);

      // Load all the instance MCFs into memory, so we can do existence checks, resolution, etc.
      processor.processNodes(Mcf.McfType.INSTANCE_MCF);

      // Perform existence checks.
      if (args.doExistenceChecks) {
        // NOTE: If doExistenceChecks is true, we do a checkNodes() call *after* all instance MCF
        // files are processed (via processNodes). This is so that the newly added schema, StatVar,
        // etc. are known to the Existence Checker first, before existence checks are performed.
        processor.checkNodes();
      }

      if (args.doResolution) {
        // Find external IDs from in-memory MCF nodes and CSVs, and map them to DCIDs.
        processor.lookupExternalIds();

        // Having looked up the external IDs, resolve the instances.
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
        processor.processTables();
      } else if (args.fileGroup.getTmcfs() != null) {
        // Sanity check the TMCF nodes.
        processor.processNodes(Mcf.McfType.TEMPLATE_MCF);
      }

      // We've been adding stats to statChecker all along, now do the actual check.
      if (args.doStatChecks) {
        processor.checkStats();
      }

      if (args.outputFiles != null) {
        processor.closeFiles();
      }
    } catch (DCTooManyFailuresException | InterruptedException | IOException ex) {
      // Regardless of the failures, we will dump the logCtx and exit.
      retVal = -1;
    }
    args.logCtx.persistLog(false);
    return retVal;
  }

  private Processor(Args args) {
    this.logCtx = args.logCtx;
    this.outputFiles = args.outputFiles;
    this.verbose = args.verbose;
    this.fileGroup = args.fileGroup;
    if (args.doExistenceChecks) {
      existenceChecker = new ExistenceChecker(HttpClient.newHttpClient(), verbose, logCtx);
    }
    if (args.doResolution) {
      idResolver = new ExternalIdResolver(HttpClient.newHttpClient(), verbose, logCtx);
    }
    if (args.doStatChecks) {
      Set<String> samplePlaces =
          args.samplePlaces == null ? null : new HashSet<>(args.samplePlaces);
      statChecker = new StatChecker(logCtx, samplePlaces, verbose);
    }
  }

  private void processNodes(Mcf.McfType type)
      throws IOException, DCTooManyFailuresException, InterruptedException {
    if (type == Mcf.McfType.INSTANCE_MCF) {
      for (var f : fileGroup.getMcfs()) {
        processNodes(type, f);
      }
    } else {
      for (var f : fileGroup.getTmcfs()) {
        processNodes(type, f);
      }
    }
  }

  private void processNodes(Mcf.McfType type, File file)
      throws IOException, DCTooManyFailuresException, InterruptedException {
    long numNodesProcessed = 0;
    if (verbose) logger.info("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    logCtx.setLocationFile(file.getName());
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      n = McfMutator.mutate(n.toBuilder(), logCtx);

      if (existenceChecker != null && type == Mcf.McfType.INSTANCE_MCF) {
        // Add instance MCF nodes to ExistenceChecker.  We load all the nodes up first
        // before we check them later in checkNodes().
        existenceChecker.addLocalGraph(n);
      } else {
        McfChecker.check(n, existenceChecker, logCtx);
      }
      if (existenceChecker != null || idResolver != null || statChecker != null) {
        nodesForVariousChecks.add(n);
      }

      numNodesProcessed++;
      logCtx.provideStatus(numNodesProcessed, "nodes processed");
      if (logCtx.loggedTooManyFailures()) {
        throw new DCTooManyFailuresException("processNodes encountered too many failures");
      }
    }
    logger.info("Checked {} with {} nodes", file.getName(), numNodesProcessed);
  }

  private void processTables()
      throws IOException, DCTooManyFailuresException, InterruptedException {
    if (verbose) logger.info("TMCF " + fileGroup.getTmcf().getName());
    for (File csvFile : fileGroup.getCsvs()) {
      if (verbose) logger.info("Checking CSV " + csvFile.getPath());
      TmcfCsvParser parser =
          TmcfCsvParser.init(
              fileGroup.getTmcf().getPath(), csvFile.getPath(), fileGroup.delimiter(), logCtx);
      // If there were too many failures when initializing the parser, parser will be null and we
      // don't want to continue processing.
      if (logCtx.loggedTooManyFailures()) {
        throw new DCTooManyFailuresException("processTables encountered too many failures");
      }
      Mcf.McfGraph g;
      long numNodesProcessed = 0, numRowsProcessed = 0;
      while ((g = parser.parseNextRow()) != null) {
        g = McfMutator.mutate(g.toBuilder(), logCtx);

        // This will set counters/messages in logCtx.
        boolean success = McfChecker.check(g, existenceChecker, logCtx);
        if (success) {
          logCtx.incrementCounterBy("NumRowSuccesses", 1);
        }
        if (idResolver != null) {
          g = resolveCommon(g, OutputFileType.TABLE_NODES, OutputFileType.FAILED_TABLE_NODES);
        } else {
          if (outputFiles != null) {
            writeGraph(OutputFileType.TABLE_NODES, g);
          }
        }

        // This will extract and save time series info from the relevant nodes from g and save it
        // to statChecker.
        if (statChecker != null) {
          statChecker.extractSeriesInfoFromGraph(g);
        }
        numNodesProcessed += g.getNodesCount();
        numRowsProcessed++;
        logCtx.provideStatus(numRowsProcessed, "rows processed");
        if (logCtx.loggedTooManyFailures()) {
          throw new DCTooManyFailuresException("processTables encountered too many failures");
        }
      }
      logger.info(
          "Checked CSV {} ({} rows, {} nodes)",
          csvFile.getName(),
          numRowsProcessed,
          numNodesProcessed);
    }
    if (existenceChecker != null) existenceChecker.drainRemoteCalls();
  }

  // Called only when existenceChecker is enabled.
  private void checkNodes() throws IOException, InterruptedException, DCTooManyFailuresException {
    long numNodesChecked = 0;
    logger.info("Performing existence checks");
    logCtx.setLocationFile("");
    for (Mcf.McfGraph n : nodesForVariousChecks) {
      McfChecker.check(n, existenceChecker, logCtx);
      numNodesChecked += n.getNodesCount();
      numNodesChecked++;
      logCtx.provideStatus(numNodesChecked, "nodes checked");
      if (logCtx.loggedTooManyFailures()) {
        throw new DCTooManyFailuresException("checkNodes encountered too many failures");
      }
    }
    existenceChecker.drainRemoteCalls();
  }

  // Called only when resolution is enabled.
  private Mcf.McfGraph resolveNodes() throws IOException {
    return resolveCommon(
        McfUtil.mergeGraphs(nodesForVariousChecks),
        OutputFileType.NODES,
        OutputFileType.FAILED_NODES);
  }

  private Mcf.McfGraph resolveCommon(
      Mcf.McfGraph mcfGraph, OutputFileType successFile, OutputFileType failureFile)
      throws IOException {
    McfResolver resolver = new McfResolver(mcfGraph, verbose, idResolver, logCtx);
    resolver.resolve();
    if (outputFiles != null) {
      var resolved = resolver.resolvedGraph();
      if (!resolved.getNodesMap().isEmpty()) {
        writeGraph(successFile, resolved);
      }
      var failed = resolver.failedGraph();
      if (!failed.getNodesMap().isEmpty()) {
        writeGraph(failureFile, failed);
      }
    }
    return resolver.resolvedGraph();
  }

  // Process all the CSV tables to load all external IDs.
  private void lookupExternalIds() throws IOException, InterruptedException {
    LogWrapper dummyLog = new LogWrapper(Debug.Log.newBuilder(), Path.of("."));
    for (var g : nodesForVariousChecks) {
      for (var idAndNode : g.getNodesMap().entrySet()) {
        idResolver.submitNode(idAndNode.getValue());
      }
    }
    for (File csvFile : fileGroup.getCsvs()) {
      if (verbose) logger.info("Reading external IDs from CSV " + csvFile.getPath());
      TmcfCsvParser parser =
          TmcfCsvParser.init(
              fileGroup.getTmcf().getPath(), csvFile.getPath(), fileGroup.delimiter(), dummyLog);
      if (parser == null) continue;
      Mcf.McfGraph g;
      while ((g = parser.parseNextRow()) != null) {
        for (var idAndNode : g.getNodesMap().entrySet()) {
          idResolver.submitNode(idAndNode.getValue());
        }
      }
    }
    idResolver.drainRemoteCalls();
  }

  private void writeGraph(OutputFileType type, Mcf.McfGraph graph) throws IOException {
    var writer = writers.getOrDefault(type, null);
    if (writer == null) {
      var fileString = outputFiles.get(type).toString();
      logger.info("Writing to file " + fileString + " of type " + type.name());
      writer = new BufferedWriter(new FileWriter(fileString));
      writers.put(type, writer);
    }
    writer.write(McfUtil.serializeMcfGraph(graph, false));
  }

  private void closeFiles() throws IOException {
    // Close any file that was written to.
    for (var kv : writers.entrySet()) {
      kv.getValue().close();
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
    if (verbose) logger.info("Performing stats checks");
    statChecker.check();
  }

  private static class DCTooManyFailuresException extends Exception {
    public DCTooManyFailuresException() {}

    public DCTooManyFailuresException(String message) {
      super(message);
    }
  }
}
