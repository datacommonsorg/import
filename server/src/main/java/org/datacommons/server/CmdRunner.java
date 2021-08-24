package org.datacommons.server;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

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
