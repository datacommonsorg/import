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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.util.FileGroup;
import org.datacommons.util.LogWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

// Defines the command line argument and execute startup operations.
@Component
@Command(name = "command", mixinStandardHelpOptions = true)
public class ServerCommand implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(ServerCommand.class);
  // Always download to /tmp folder.
  private static final String DEST_PATH = "/tmp";

  @Parameters(
      arity = "1..*",
      description =
          ("List of input files. The file extensions are used to infer the format. "
              + "Valid extensions include .tmcf for Template MCF, "
              + ".csv for tabular text files delimited by comma (overridden with -d), and .tsv "
              + "for tab-delimited tabular files."
              + "The files are GCP object names when --bucket are specified."))
  private File[] files;

  @Option(
      names = {"-b", "--bucket"},
      description = "GCS bucket to hold the tmcf and csv/tsv files ")
  private String bucket;

  // injected by picocli
  @Spec Model.CommandSpec spec;
  // Dependency injection of ObservationRepository instance.
  @Autowired private ObservationRepository observationRepository;

  @Override
  public Integer call() throws IOException, InterruptedException, StorageException {
    processGcsFiles();
    FileGroup fg = FileGroup.Build(files, spec, logger);
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), new File(".").toPath());
    Processor processor = new Processor(logCtx);
    processor.processTables(fg.GetTmcf(), fg.GetCsv(), ',', observationRepository);
    return 0;
  }

  // If input files are from GCS, download the files locally and update
  // file paths.
  private void processGcsFiles() throws StorageException {
    if (bucket == "") {
      return;
    }
    Storage storage = StorageOptions.newBuilder().build().getService();
    File[] newFiles = new File[files.length];
    int i = 0;
    for (File file : files) {
      logger.info("Reading GCS file {}/{} ...", bucket, file.getPath());
      Blob blob = storage.get(BlobId.of(bucket, file.getPath()));
      Path localFile = Paths.get(DEST_PATH, file.getName());
      blob.downloadTo(localFile);
      newFiles[i] = localFile.toFile();
      i++;
    }
    files = newFiles;
  }
}
