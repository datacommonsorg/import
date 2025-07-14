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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.datacommons.proto.Debug;

/** Utility class for capturing runtime metadata about the import tool execution. */
public class RuntimeMetadataUtil {

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
    builder.setToolVersion(getToolVersion());

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
   * Gets the git commit hash from the JAR manifest or returns a default for local development.
   *
   * @return Git commit hash string
   */
  private static String getGitCommitHash() {
    String manifestHash = getManifestAttribute("Git-Commit-Hash");
    if (manifestHash != null && !manifestHash.isEmpty()) {
      return manifestHash;
    }
    return "local-development";
  }

  /**
   * Reads a custom attribute from the JAR manifest.
   *
   * @param attributeName The name of the manifest attribute to read
   * @return The attribute value or null if not found
   */
  private static String getManifestAttribute(String attributeName) {
    try {
      Class<?> clazz = RuntimeMetadataUtil.class;
      String className = clazz.getSimpleName() + ".class";
      String classPath = clazz.getResource(className).toString();

      if (!classPath.startsWith("jar")) {
        // Not running from a JAR
        return null;
      }

      String manifestPath =
          classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
      java.util.jar.Manifest manifest =
          new java.util.jar.Manifest(new java.net.URI(manifestPath).toURL().openStream());
      java.util.jar.Attributes attrs = manifest.getMainAttributes();

      return attrs.getValue(attributeName);
    } catch (Exception e) {
      // Ignore errors - might not be running from JAR or manifest might not be accessible
      return null;
    }
  }

  /**
   * Gets the tool version from the JAR manifest or returns a default for local development.
   *
   * @return Tool version string
   */
  public static String getToolVersion() {
    Package pkg = RuntimeMetadataUtil.class.getPackage();
    if (pkg != null) {
      String version = pkg.getImplementationVersion();
      if (version != null && !version.isEmpty()) {
        return version;
      }
    }
    return "local-development";
  }
}
