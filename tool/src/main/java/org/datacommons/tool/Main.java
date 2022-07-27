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

import java.io.File;
import java.util.List;
import picocli.CommandLine;

@CommandLine.Command(
    name = "dc-import",
    mixinStandardHelpOptions = true,
    version = "dc-import 0.1",
    description = "Tool for use in developing datasets for Data Commons.",
    subcommands = {Lint.class, GenMcf.class})
class Main {
  @CommandLine.Option(
      names = {"-o", "--output-dir"},
      description =
          "Specifies the directory to write output files."
              + "Default is dc_generated/ within current working directory.",
      defaultValue = "dc_generated",
      scope = CommandLine.ScopeType.INHERIT)
  public File outputDir;

  @CommandLine.Option(
      names = {"--verbose"},
      description = "Print verbose log. Defaults to false.",
      defaultValue = "false",
      scope = CommandLine.ScopeType.INHERIT)
  public boolean verbose;

  @CommandLine.Option(
      names = {"-e", "--existence-checks"},
      defaultValue = "true",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Checks DCID references to schema nodes against the KG and locally. "
              + "If this flag is set, then calls will be made to the Staging API server, "
              + "and instance MCFs get fully loaded into memory. Defaults to true.")
  public boolean doExistenceChecks;

  @CommandLine.Option(
      names = {"-r", "--resolution"},
      defaultValue = "LOCAL",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Specifies the mode of resolution to use: ${COMPLETION-CANDIDATES}. "
              + "\n\nLOCAL only resolves local references and generates DCIDs. "
              + "Notably, this mode does not resolve the external IDs against the DC KG. "
              + "\n\nFULL resolves external IDs (such as ISO) in DC, local references, "
              + "and generated DCIDs. Note that FULL mode may be slower since it makes (batched) "
              + "DC Recon API calls and performs two passes over the provided CSV files. "
              + "You should only use this if you have to resolve location entities via external IDs. "
              + "\n\nNONE does not resolve references. Use this only if all inputs have DCIDs defined. "
              + "You rarely want to use this mode.")
  public Args.ResolutionMode resolutionMode = Args.ResolutionMode.NONE;

  @CommandLine.Option(
      names = {"-s", "--stat-checks"},
      defaultValue = "true",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Checks integrity of time series by checking for holes, variance in values, etc. "
              + "A set of counters detailing the results of the checks will be logged in report.json. "
              + "For every such counter, the tool will provide a few exemplar cases to help the user "
              + "understand and resolve the issue(s). "
              + "Defaults to true.")
  public boolean doStatChecks;

  @CommandLine.Option(
      names = {"--allow-non-numeric-obs-values"},
      defaultValue = "false",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Allows non-numeric (text or reference) values for StatVarObservation "
              + "value field. If false, non-numeric values will raise an error (Sanity_SVObs_Value_NotANumber). If true, "
              + "these values will be allowed and relevant StatChecks might be performed "
              + "(depending on the value of --stat-checks). "
              + "Defaults to false.")
  public boolean allowNonNumericStatVarObservation;

  @CommandLine.Option(
      names = {"--check-measurement-result"},
      defaultValue = "false",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Checks DCID references from StatVarObservation nodes if the StatisticalVariable "
              + "they are measuring has `statType: measurementResult`. "
              + "If the StatVar definition exists in the local MCF files provided, that will be used. "
              + "Otherwise, API requests to the Data Commons KG will be made synchronously per unknown StatVar. "
              + "Only nodes in sample places are subject to this check. "
              + "Defaults to `false`.")
  public boolean checkMeasurementResult;

  @CommandLine.Option(
      names = {"-p", "--sample-places"},
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Specifies a list of place dcids to run stats check on."
              + "This flag should only be set if --stat-checks is true. "
              + "If --stat-checks is true and this flag is not set, 5 sample "
              + "places are picked for roughly each distinct place type.")
  public List<String> samplePlaces;

  @CommandLine.Option(
      names = {"-n", "--num-threads"},
      defaultValue = "1",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Specifies the number of concurrent threads used for processing CSVs. "
              + "You need multiple CSVs to take advantage of concurrent processing. "
              + "Defaults to true.")
  public int numThreads;

  @CommandLine.Option(
      names = {"-sr", "--summary-report"},
      defaultValue = "true",
      scope = CommandLine.ScopeType.INHERIT,
      description = "Generates a summary report in html format. Defaults to true.")
  public boolean generateSummaryReport;

  @CommandLine.Option(
      names = {"-ep", "--existence-checks-place"},
      defaultValue = "false",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Specifies whether to perform existence checks for places found in "
              + "the `observationAbout` property in StatVarObservation nodes."
              + "Defaults to true.")
  public boolean checkObservationAbout;

  public static void main(String... args) {
    System.exit(
        new CommandLine(new Main()).setCaseInsensitiveEnumValuesAllowed(true).execute(args));
  }
}
