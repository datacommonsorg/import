// Copyright 2025 Google LLC
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

package org.datacommons.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.datacommons.proto.Debug;

/** Utility class for capturing runtime metadata about the import tool execution. */
public class RuntimeMetadataUtil {

  // Tool version - should match version in Main.java and pom.xml
  private static final String TOOL_VERSION = "0.1-alpha.1";

  /**
   * Creates a RuntimeMetadata proto with system information.
   *
   * @param startTimeMillis The start time of the process in epoch milliseconds
   * @return RuntimeMetadata proto populated with system information
   */
  public static Debug.RuntimeMetadata createRuntimeMetadata(long startTimeMillis) {
    Debug.RuntimeMetadata.Builder builder = Debug.RuntimeMetadata.newBuilder();

    // Set start and end times in epoch milliseconds
    builder.setStartTimeMillis(startTimeMillis);
    builder.setEndTimeMillis(System.currentTimeMillis());

    // Set username
    String username = System.getProperty("user.name");
    if (username != null && !username.isEmpty()) {
      builder.setUsername(username);
    }

    // Set hostname
    try {
      String hostname = InetAddress.getLocalHost().getHostName();
      builder.setHostname(hostname);
    } catch (UnknownHostException e) {
      // Ignore if we can't get hostname
    }

    // Set OS information
    String osName = System.getProperty("os.name");
    if (osName != null && !osName.isEmpty()) {
      builder.setOsName(osName);
    }

    String osVersion = System.getProperty("os.version");
    if (osVersion != null && !osVersion.isEmpty()) {
      builder.setOsVersion(osVersion);
    }

    // Set Java version
    String javaVersion = System.getProperty("java.version");
    if (javaVersion != null && !javaVersion.isEmpty()) {
      builder.setJavaVersion(javaVersion);
    }

    // Set tool version
    builder.setToolVersion(TOOL_VERSION);

    // Try to get git commit hash
    String gitCommitHash = getGitCommitHash();
    if (gitCommitHash != null && !gitCommitHash.isEmpty()) {
      builder.setGitCommitHash(gitCommitHash);
    }

    // Build timestamp could be set during build process
    // For now, we'll use the current ISO timestamp as a placeholder
    String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
    builder.setBuildTimestamp(timestamp);

    return builder.build();
  }

  /**
   * Attempts to get the git commit hash of the current repository.
   *
   * @return Git commit hash or null if unable to retrieve
   */
  private static String getGitCommitHash() {
    try {
      Process process = new ProcessBuilder("git", "rev-parse", "HEAD").start();
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String commitHash = reader.readLine();
      reader.close();

      int exitCode = process.waitFor();
      if (exitCode == 0 && commitHash != null && !commitHash.trim().isEmpty()) {
        return commitHash.trim();
      }
    } catch (IOException | InterruptedException e) {
      // Ignore errors - git might not be available or this might not be a git repo
    }
    return null;
  }
}
