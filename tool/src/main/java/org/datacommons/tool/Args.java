package org.datacommons.tool;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.util.Strings;
import org.datacommons.proto.Debug;
import org.datacommons.util.FileGroup;

// Class representing the command line arguments to dc-import tool. Largely used as a struct.
class Args {
  public boolean doExistenceChecks = false;
  public ResolutionMode resolutionMode = ResolutionMode.NONE;
  public boolean doStatChecks = false;
  public List<String> samplePlaces = null;
  public boolean verbose = false;
  public FileGroup fileGroup = null;
  public Map<OutputFileType, Path> outputFiles = null;
  public int numThreads = 1;
  public Path outputDir = null;
  public boolean generateSummaryReport = true;
  public boolean checkObservationAbout = false;

  public String toString() {
    StringBuilder argStr = new StringBuilder();
    if (outputFiles != null) {
      argStr.append("genmcf with");
    } else {
      argStr.append("lint with");
    }
    argStr.append(" existence-checks=" + doExistenceChecks);
    argStr.append(", resolution=" + resolutionMode.name());
    argStr.append(", num-threads=" + numThreads);
    argStr.append(", stat-checks=" + doStatChecks);
    if (samplePlaces != null) {
      argStr.append(", sample-places=" + Strings.join(samplePlaces, ':'));
    }
    argStr.append(", observation-about=" + checkObservationAbout);
    return argStr.toString();
  }

  public Debug.CommandArgs toProto() {
    Debug.CommandArgs.Builder argsBuilder = Debug.CommandArgs.newBuilder();
    argsBuilder.setExistenceChecks(doExistenceChecks);
    argsBuilder.setNumThreads(numThreads);
    if (resolutionMode == ResolutionMode.NONE) {
      argsBuilder.setResolution(Debug.CommandArgs.ResolutionMode.RESOLUTION_MODE_NONE);
    } else if (resolutionMode == ResolutionMode.LOCAL) {
      argsBuilder.setResolution(Debug.CommandArgs.ResolutionMode.RESOLUTION_MODE_LOCAL);
    } else if (resolutionMode == ResolutionMode.FULL) {
      argsBuilder.setResolution(Debug.CommandArgs.ResolutionMode.RESOLUTION_MODE_FULL);
    }
    argsBuilder.setStatChecks(doStatChecks);
    if (samplePlaces != null) argsBuilder.addAllSamplePlaces(samplePlaces);
    argsBuilder.setObservationAbout(checkObservationAbout);
    return argsBuilder.build();
  }

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
  }

  public enum ResolutionMode {
    NONE,
    LOCAL,
    FULL,
  }
}
