// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.ingestion.pipeline;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
import com.google.spanner.admin.database.v1.CreateDatabaseRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test helper to initialize Spanner database schema using main schema.sql. */
public class SpannerDatabaseInitializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerDatabaseInitializer.class);

  public static void validateOrInitializeDatabase(
      String gcpProjectId,
      String spannerInstanceId,
      String spannerDatabaseId,
      String emulatorHost) {
    DatabaseAdminClient dbAdminClient;
    try {
      if (emulatorHost != null) {
        DatabaseAdminSettings.Builder settingsBuilder = DatabaseAdminSettings.newBuilder();
        settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
        settingsBuilder.setEndpoint(emulatorHost);
        settingsBuilder.setTransportChannelProvider(
            InstantiatingGrpcChannelProvider.newBuilder()
                .setEndpoint(emulatorHost)
                .setChannelConfigurator(io.grpc.ManagedChannelBuilder::usePlaintext)
                .build());
        dbAdminClient = DatabaseAdminClient.create(settingsBuilder.build());
      } else {
        dbAdminClient = DatabaseAdminClient.create();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create DatabaseAdminClient.", e);
    }

    String databasePath =
        String.format(
            "projects/%s/instances/%s/databases/%s",
            gcpProjectId, spannerInstanceId, spannerDatabaseId);

    try {
      dbAdminClient.getDatabase(databasePath);
      LOGGER.info("Spanner database {} already exists.", spannerDatabaseId);
    } catch (com.google.api.gax.rpc.NotFoundException notFoundE) {
      initializeRemoteDatabase(dbAdminClient, gcpProjectId, spannerInstanceId, spannerDatabaseId);
    }

    List<String> currentDdlStatements =
        dbAdminClient.getDatabaseDdl(databasePath).getStatementsList();

    if (!currentDdlStatements.isEmpty()) {
      LOGGER.info("Database is already initialized.");
      return;
    }

    LOGGER.info("Database is empty. Applying DDL statements to existing database.");
    List<String> statementsToApply = readDdlStatements(emulatorHost);

    try {
      dbAdminClient
          .updateDatabaseDdlAsync(
              com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest.newBuilder()
                  .setDatabase(databasePath)
                  .addAllStatements(statementsToApply)
                  .build())
          .get();
      LOGGER.info("Successfully applied DDL statements to existing database.");
    } catch (java.util.concurrent.ExecutionException executionE) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.UNKNOWN, "An error occurred during DDL update.", executionE);
    } catch (InterruptedException interruptedE) {
      LOGGER.error("Operation was interrupted while waiting for DDL update.", interruptedE);
      throw SpannerExceptionFactory.propagateInterrupt(interruptedE);
    }
  }

  private static void initializeRemoteDatabase(
      DatabaseAdminClient dbAdminClient,
      String gcpProjectId,
      String spannerInstanceId,
      String spannerDatabaseId) {
    LOGGER.info("Spanner database {} not found. Creating it now.", spannerDatabaseId);

    CreateDatabaseRequest request =
        CreateDatabaseRequest.newBuilder()
            .setParent(String.format("projects/%s/instances/%s", gcpProjectId, spannerInstanceId))
            .setCreateStatement(String.format("CREATE DATABASE `%s`", spannerDatabaseId))
            .build();

    try {
      dbAdminClient.createDatabaseAsync(request).get();
    } catch (java.util.concurrent.ExecutionException executionE) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.UNKNOWN, "An error occurred during database creation.", executionE);
    } catch (InterruptedException interruptedE) {
      LOGGER.error("Operation was interrupted while waiting for database creation.", interruptedE);
      throw SpannerExceptionFactory.propagateInterrupt(interruptedE);
    }
    LOGGER.info("Successfully created Spanner database {}.", spannerDatabaseId);
  }

  private static List<String> readDdlStatements(String emulatorHost) {
    InputStream inputStream =
        SpannerDatabaseInitializer.class.getClassLoader().getResourceAsStream("schema.sql");
    if (inputStream == null) {
      throw new IllegalStateException("Could not find schema.sql in resources.");
    }
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      List<String> statements = parseDdlStatements(reader);

      // Always filter out templates (embeddings/models) as we don't resolve them in Java tests.
      statements =
          statements.stream()
              .filter(stmt -> !stmt.contains("{{") && !stmt.contains("{%"))
              .collect(Collectors.toList());

      if (emulatorHost != null) {
        statements =
            statements.stream()
                .map(
                    stmt ->
                        stmt.replaceAll(
                            "(?i),?\\s*OPTIONS\\s*\\(\\s*columnar_policy\\s*=\\s*'enabled'\\s*\\)",
                            ""))
                .collect(Collectors.toList());
      }
      return statements;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read schema.sql", e);
    }
  }

  private static List<String> parseDdlStatements(BufferedReader reader) throws IOException {
    String fullText = reader.lines().collect(Collectors.joining("\n"));
    String[] blocksArray = fullText.split("\\n\\s*\\n+", 0);
    return Arrays.stream(blocksArray)
        .map(String::trim)
        .filter(stmt -> !stmt.isEmpty())
        .filter(stmt -> !isCommentsOnly(stmt))
        .map(stmt -> stmt.endsWith(";") ? stmt.substring(0, stmt.length() - 1) : stmt)
        .map(String::trim)
        .collect(Collectors.toList());
  }

  private static boolean isCommentsOnly(String block) {
    return Arrays.stream(block.split("\n"))
        .map(String::trim)
        .filter(line -> !line.isEmpty())
        .allMatch(line -> line.startsWith("--"));
  }
}
