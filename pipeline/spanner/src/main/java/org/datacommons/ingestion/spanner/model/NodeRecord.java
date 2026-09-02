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

package org.datacommons.ingestion.spanner.model;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.datacommons.ingestion.data.Node;

/**
 * Immutable canonical record representing a single row in the Spanner Node table.
 *
 * <p>Serves as the single contract between forward ingestion POJOs and Spanner historical snapshot
 * Structs, eliminating duplication across forward and rollback mutation builders.
 */
public record NodeRecord(
    String subjectId, String value, ByteArray bytes, String name, List<String> types)
    implements Serializable {

  public static final String COL_SUBJECT_ID = "subject_id";
  public static final String COL_VALUE = "value";
  public static final String COL_BYTES = "bytes";
  public static final String COL_NAME = "name";
  public static final String COL_TYPES = "types";
  public static final String COL_LAST_UPDATE_TIMESTAMP = "last_update_timestamp";

  public static final List<String> READ_COLUMNS =
      List.of(COL_SUBJECT_ID, COL_NAME, COL_TYPES, COL_VALUE, COL_BYTES);

  public static final Set<String> WRITABLE_COLUMNS =
      Set.of(COL_SUBJECT_ID, COL_VALUE, COL_BYTES, COL_NAME, COL_TYPES, COL_LAST_UPDATE_TIMESTAMP);

  /** Adapts forward ingestion's domain {@link Node} POJO. */
  public static NodeRecord from(Node node) {
    return new NodeRecord(
        node.getSubjectId(), node.getValue(), node.getBytes(), node.getName(), node.getTypes());
  }

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static NodeRecord from(Struct struct) {
    return new NodeRecord(
        struct.getString(COL_SUBJECT_ID),
        struct.isNull(COL_VALUE) ? null : struct.getString(COL_VALUE),
        struct.isNull(COL_BYTES) ? null : struct.getBytes(COL_BYTES),
        struct.isNull(COL_NAME) ? null : struct.getString(COL_NAME),
        struct.isNull(COL_TYPES) ? null : struct.getStringList(COL_TYPES));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(tableName)
            .set(COL_SUBJECT_ID)
            .to(subjectId)
            .set(COL_LAST_UPDATE_TIMESTAMP)
            .to(Value.COMMIT_TIMESTAMP)
            .set(COL_VALUE)
            .to(value)
            .set(COL_BYTES)
            .to(bytes)
            .set(COL_NAME)
            .to(name)
            .set(COL_TYPES)
            .toStringArray(types)
            .build();
    if (!WRITABLE_COLUMNS.equals(mutation.asMap().keySet())) {
      throw new IllegalStateException(
          "Mutation columns "
              + mutation.asMap().keySet()
              + " do not match WRITABLE_COLUMNS "
              + WRITABLE_COLUMNS);
    }
    return mutation;
  }
}
