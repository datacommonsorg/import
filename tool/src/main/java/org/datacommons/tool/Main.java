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
          "Directory to write output files. Default is dc_generated/ within current"
              + " working directory.",
      defaultValue = "dc_generated",
      scope = CommandLine.ScopeType.INHERIT)
  public File outputDir;

  @CommandLine.Option(
      names = {"--verbose"},
      description = "Print verbose log.",
      defaultValue = "false",
      scope = CommandLine.ScopeType.INHERIT)
  public boolean verbose;

  @CommandLine.Option(
      names = {"-e", "--existence-checks"},
      defaultValue = "true",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Check DCID references to schema nodes against the KG and locally. If set, then "
              + "calls will be made to the Staging API server, and instance MCFs get fully "
              + "loaded into memory.")
  public boolean doExistenceChecks;

  // TODO: Default to true after some trials.
  @CommandLine.Option(
      names = {"-r", "--resolution"},
      defaultValue = "false",
      scope = CommandLine.ScopeType.INHERIT,
      description = "Resolves local references and generates node DCIDs.")
  public boolean doResolution;

  // TODO: Default to true after some trials.
  @CommandLine.Option(
      names = {"-s", "--stat-checks"},
      defaultValue = "false",
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "Checks integrity of time series by checking for holes, variance in values, etc.")
  public boolean doStatChecks;

  @CommandLine.Option(
      names = {"-p", "--sample-places"},
      scope = CommandLine.ScopeType.INHERIT,
      description =
          "List of place dcids to run stats check on. This should only be set if "
              + "--stat-checks is true. If --stat-checks is true and this is not set, 5 sample places "
              + "are picked for roughly each distinct place type.")
  public List<String> samplePlaces;

  public static void main(String... args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }
}
