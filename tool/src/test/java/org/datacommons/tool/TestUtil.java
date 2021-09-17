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

import com.google.common.truth.Expect;
import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.datacommons.proto.Debug;
import org.junit.rules.TemporaryFolder;

// Common set of utils used in e2e tests.
public class TestUtil {
  public static void assertReportFilesAreSimilar(
      Expect expect, File directory, String expected, String actual) throws IOException {
    String testCase = directory.getName();
    Debug.Log expectedLog = reportToProto(expected).build();
    Debug.Log actualLog = reportToProto(actual).build();
    expect.that(expectedLog.getLevelSummaryMap()).isEqualTo(actualLog.getLevelSummaryMap());
    expect
        .about(ProtoTruth.protos())
        .that(actualLog.getEntriesList())
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(expectedLog.getEntriesList());
    expect
        .about(ProtoTruth.protos())
        .that(actualLog.getStatsCheckSummaryList())
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(expectedLog.getStatsCheckSummaryList());
  }

  private static Debug.Log.Builder reportToProto(String report)
      throws InvalidProtocolBufferException {
    Debug.Log.Builder logBuilder = Debug.Log.newBuilder();
    JsonFormat.parser().merge(report, logBuilder);
    return logBuilder;
  }

  public static Path getTestFilePath(TemporaryFolder testFolder, String directory, String fileName)
      throws IOException {
    return Paths.get(testFolder.getRoot().getPath(), directory, fileName);
  }

  public static Path getOutputFilePath(String parentDirectoryPath, String fileName)
      throws IOException {
    return Path.of(parentDirectoryPath, "output", fileName);
  }

  public static String readStringFromPath(Path filePath) throws IOException {
    File file = new File(filePath.toString());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }
}
