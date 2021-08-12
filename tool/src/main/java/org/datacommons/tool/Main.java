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

import picocli.CommandLine;

import java.io.File;

// TODO: Add e2e tests once Debug.Log is fully plumbed in.
@CommandLine.Command(
    name = "dc-import",
    mixinStandardHelpOptions = true,
    version = "dc-import 0.1",
    description = "Tool for use in developing datasets for Data Commons.",
    subcommands = {Lint.class, GenMcf.class})
class Main {
  @CommandLine.Option(
      names = {"-o", "--output-dir"},
      description = "Directory to write output files. Default is current working directory.",
      defaultValue = ".",
      scope = CommandLine.ScopeType.INHERIT)
  public File outputDir;

  public static void main(String... args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }
}
