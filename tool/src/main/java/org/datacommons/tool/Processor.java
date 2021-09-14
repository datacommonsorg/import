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
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private final Map<OutputFileType, BufferedWriter> writers;
  private final List<Mcf.McfGraph> nodesForVariousChecks;
  private ExistenceChecker existenceChecker;
  private ExternalIdResolver idResolver;

  // TODO: Produce output MCF files in files corresponding to the input (like prod).
  public enum OutputFileType {
    TABLES,
    TABLES_FAILURE,
    NODES,
    NODES_FAILURE,
  };

  static class Args {
    public boolean doExistenceChecks = false;
    public boolean doResolution = false;
    public boolean verbose = false;
    public FileGroup fileGroup = null;
    public Map<OutputFileType, BufferedWriter> writers = null;
    LogWrapper logCtx = null;
  }

  public static Integer process(Args args) throws IOException {
    Integer retVal = 0;
    try {
      Processor processor = new Processor(args);
      if (args.doResolution) {
        processor.lookupExternalIds();
      }
      // Process all the instance MCF first, so that we can add the nodes for Existence Check.
      processor.processNodes(Mcf.McfType.INSTANCE_MCF);
      if (args.doExistenceChecks) {
        processor.checkNodes();
      }
      if (args.doResolution) {
        processor.resolveNodes();
      }
      if (!args.fileGroup.getCsvs().isEmpty()) {
        processor.processTables();
      } else if (args.fileGroup.getTmcfs() != null) {
        processor.processNodes(Mcf.McfType.TEMPLATE_MCF);
      }
    } catch (DCTooManyFailuresException | InterruptedException | IOException ex) {
      // Regardless of the failures, we will dump the logCtx and exit.
      retVal = -1;
    }
    if (args.writers != null) {
      for (var writer : args.writers.entrySet()) {
        writer.getValue().close();
      }
    }
    args.logCtx.persistLog(false);
    return retVal;
  }

  // NOTE: If doExistenceChecks is true, then it is important that the caller perform a
  // checkNodes() call *after* all instance MCF files are processed (via processNodes). This is
  // so that the newly added schema, StatVar, etc. are fully known to the Existence Checker first,
  // before existence checks are performed.
  private Processor(Args args) {
    this.logCtx = args.logCtx;
    this.writers = args.writers;
    this.verbose = args.verbose;
    this.fileGroup = args.fileGroup;
    nodesForVariousChecks = new ArrayList<>();
    if (args.doExistenceChecks) {
      existenceChecker = new ExistenceChecker(HttpClient.newHttpClient(), verbose, logCtx);
    }
    if (args.doResolution) {
      idResolver = new ExternalIdResolver(HttpClient.newHttpClient(), verbose, logCtx);
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
      if (existenceChecker != null || idResolver != null) {
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
          resolveAndWrite(g);
        } else {
          if (writers != null) {
            writers.get(OutputFileType.TABLES).write(McfUtil.serializeMcfGraph(g, false));
          }
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
  private void resolveNodes() throws IOException {
    resolveAndWrite(McfUtil.mergeGraphs(nodesForVariousChecks));
  }

  private void resolveAndWrite(Mcf.McfGraph mcfGraph) throws IOException {
    McfResolver resolver = new McfResolver(mcfGraph, verbose, idResolver, logCtx);
    resolver.resolve();
    if (writers != null) {
      var resolved = resolver.resolvedGraph();
      if (!resolved.getNodesMap().isEmpty()) {
        writers.get(OutputFileType.NODES).write(McfUtil.serializeMcfGraph(resolved, false));
      }
      var failed = resolver.failedGraph();
      if (!failed.getNodesMap().isEmpty()) {
        writers.get(OutputFileType.NODES_FAILURE).write(McfUtil.serializeMcfGraph(failed, false));
      }
    }
  }

  // Process all the CSV tables to load all external IDs.
  private void lookupExternalIds() throws IOException, InterruptedException {
    LogWrapper dummyLog = new LogWrapper(Debug.Log.newBuilder(), Path.of("."));
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

  private static class DCTooManyFailuresException extends Exception {
    public DCTooManyFailuresException() {}

    public DCTooManyFailuresException(String message) {
      super(message);
    }
  }
}
