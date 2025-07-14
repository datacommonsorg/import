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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;

/** Utility class for capturing runtime metadata about the import tool execution. */
public class RuntimeMetadataUtil {

  private static final Logger logger = LogManager.getLogger(RuntimeMetadataUtil.class);
  private static final String GIT_PROPERTIES_FILE = "/git.properties";

  /**
   * Gets the username from system properties.
   *
   * @return Optional containing the username, or empty if not available
   */
  public static Optional<String> getUsername() {
    String username = System.getProperty("user.name");
    return (username != null && !username.isEmpty()) ? Optional.of(username) : Optional.empty();
  }

  /**
   * Gets the hostname of the current machine.
   *
   * @return Optional containing the hostname, or empty if unable to determine
   */
  public static Optional<String> getHostname() {
    try {
      String hostname = InetAddress.getLocalHost().getHostName();
      return Optional.of(hostname);
    } catch (UnknownHostException e) {
      logger.info("Unable to determine hostname: " + e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Gets the operating system name.
   *
   * @return Optional containing the OS name, or empty if not available
   */
  public static Optional<String> getOsName() {
    String osName = System.getProperty("os.name");
    return (osName != null && !osName.isEmpty()) ? Optional.of(osName) : Optional.empty();
  }

  /**
   * Gets the operating system version.
   *
   * @return Optional containing the OS version, or empty if not available
   */
  public static Optional<String> getOsVersion() {
    String osVersion = System.getProperty("os.version");
    return (osVersion != null && !osVersion.isEmpty()) ? Optional.of(osVersion) : Optional.empty();
  }

  /**
   * Gets the Java version.
   *
   * @return Optional containing the Java version, or empty if not available
   */
  public static Optional<String> getJavaVersion() {
    String javaVersion = System.getProperty("java.version");
    return (javaVersion != null && !javaVersion.isEmpty())
        ? Optional.of(javaVersion)
        : Optional.empty();
  }

  /**
   * Creates a RuntimeMetadata proto with system information.
   *
   * @param startTimeMillis The start time of the process in epoch milliseconds
   * @param endTimeMillis The end time of the process in epoch milliseconds
   * @param callerClass The class whose package version should be retrieved for tool version
   * @return RuntimeMetadata proto populated with system information
   */
  public static Debug.RuntimeMetadata createRuntimeMetadata(
      long startTimeMillis, long endTimeMillis, Class<?> callerClass) {
    Debug.RuntimeMetadata.Builder builder = Debug.RuntimeMetadata.newBuilder();

    // Set start and end times in epoch milliseconds
    builder.setStartTimeMillis(startTimeMillis);
    builder.setEndTimeMillis(endTimeMillis);

    // Set username
    getUsername().ifPresent(builder::setUsername);

    // Set hostname
    getHostname().ifPresent(builder::setHostname);

    // Set OS information
    getOsName().ifPresent(builder::setOsName);
    getOsVersion().ifPresent(builder::setOsVersion);

    // Set Java version
    getJavaVersion().ifPresent(builder::setJavaVersion);

    // Set tool version using the provided caller class
    builder.setToolVersion(getToolVersion(callerClass));

    // Try to get git commit hash
    getGitCommitHash().ifPresent(builder::setGitCommitHash);

    // Get build timestamp
    getBuildTimestamp().ifPresent(builder::setBuildTimestamp);

    return builder.build();
  }

  /**
   * Gets the git commit hash from the git.properties file.
   *
   * @return Optional containing the git commit hash, or empty if not available
   */
  public static Optional<String> getGitCommitHash() {
    Properties gitProperties = loadGitProperties();
    String commitHash = gitProperties.getProperty("git.commit.id.abbrev");
    if (commitHash == null || commitHash.isEmpty()) {
      commitHash = gitProperties.getProperty("git.commit.id");
    }
    return (commitHash != null && !commitHash.isEmpty())
        ? Optional.of(commitHash)
        : Optional.empty();
  }

  /**
   * Gets the build timestamp from the git.properties file.
   *
   * @return Optional containing the build timestamp, or empty if not available
   */
  public static Optional<String> getBuildTimestamp() {
    Properties gitProperties = loadGitProperties();
    String buildTime = gitProperties.getProperty("git.build.time");
    if (buildTime != null && !buildTime.isEmpty()) {
      return Optional.of(buildTime);
    }
    // Fallback to current time if not available
    return Optional.of(DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
  }

  /**
   * Gets the tool version from the JAR manifest or returns a default for local development. Note:
   * This method extracts the version from the JAR file that contains the provided class.
   *
   * @param callerClass The class whose package version should be retrieved (required, non-null)
   * @return The tool version, or "local-development" if not available
   */
  public static String getToolVersion(Class<?> callerClass) {
    Package pkg = callerClass.getPackage();
    if (pkg != null) {
      String version = pkg.getImplementationVersion();
      if (version != null && !version.isEmpty()) {
        return version;
      }
    }
    return "local-development";
  }

  /**
   * Loads the git.properties file generated during build.
   *
   * @return Properties object, empty if file not found
   */
  private static Properties loadGitProperties() {
    Properties properties = new Properties();
    try (InputStream inputStream =
        RuntimeMetadataUtil.class.getResourceAsStream(GIT_PROPERTIES_FILE)) {
      if (inputStream != null) {
        properties.load(inputStream);
      }
    } catch (IOException e) {
      logger.info("Unable to load git.properties file: " + e.getMessage());
    }
    return properties;
  }
}
