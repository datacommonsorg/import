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

package org.datacommons.server;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

// Command line runner that is to execute the ServerCommand.
@Component
public class CmdRunner implements CommandLineRunner {

  private final ServerCommand serverCommand;

  public CmdRunner(ServerCommand serverCommand) {
    this.serverCommand = serverCommand;
  }

  @Override
  public void run(String... args) throws Exception {
    new CommandLine(serverCommand).execute(args);
  }
}
