package org.datacommons.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.io.FilenameUtils;
import org.datacommons.proto.Mcf;
import org.datacommons.util.McfUtil;

// Encloses a pair of writers for success and corresponding failure types, and creates the file
// on-demand when a write comes in.
class WriterPair {
  private final Args args;
  private final Args.OutputFileType successType;
  private final Args.OutputFileType failureType;
  private final File csvFile;
  private BufferedWriter successWriter = null;
  private BufferedWriter failureWriter = null;

  public WriterPair(
      Args args, Args.OutputFileType successType, Args.OutputFileType failureType, File csvFile)
      throws IOException {
    this.args = args;
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

  private BufferedWriter newWriter(Args.OutputFileType type) throws IOException {
    String filePath = args.outputFiles.get(type).toString();
    if (csvFile != null) {
      String fileSuffix = FilenameUtils.removeExtension(csvFile.getName()) + ".csv";
      filePath = FilenameUtils.removeExtension(filePath) + "_" + fileSuffix;
    }
    return new BufferedWriter(new FileWriter(filePath));
  }
}
