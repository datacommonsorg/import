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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.datacommons.ingestion.data.Edge;

/**
 * Immutable canonical record representing a single row in the Spanner Edge table.
 *
 * <p>Serves as the single contract between forward ingestion POJOs and Spanner historical snapshot
 * Structs, eliminating duplication across forward and rollback mutation builders.
 */
public record EdgeRecord(String subjectId, String predicate, String objectId, String provenance)
    implements Serializable {

  public static final String COL_SUBJECT_ID = "subject_id";
  public static final String COL_PREDICATE = "predicate";
  public static final String COL_OBJECT_ID = "object_id";
  public static final String COL_PROVENANCE = "provenance";
  public static final String COL_LAST_UPDATE_TIMESTAMP = "last_update_timestamp";

  public static final List<String> READ_COLUMNS =
      List.of(COL_SUBJECT_ID, COL_PREDICATE, COL_OBJECT_ID, COL_PROVENANCE);

  public static final Set<String> WRITABLE_COLUMNS =
      Set.of(
          COL_SUBJECT_ID, COL_PREDICATE, COL_OBJECT_ID, COL_PROVENANCE, COL_LAST_UPDATE_TIMESTAMP);

  /** Adapts forward ingestion's domain {@link Edge} POJO. */
  public static EdgeRecord from(Edge edge) {
    return new EdgeRecord(
        edge.getSubjectId(), edge.getPredicate(), edge.getObjectId(), edge.getProvenance());
  }

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static EdgeRecord from(Struct struct) {
    return new EdgeRecord(
        struct.getString(COL_SUBJECT_ID),
        struct.getString(COL_PREDICATE),
        struct.getString(COL_OBJECT_ID),
        struct.getString(COL_PROVENANCE));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(tableName)
            .set(COL_SUBJECT_ID)
            .to(subjectId)
            .set(COL_PREDICATE)
            .to(predicate)
            .set(COL_OBJECT_ID)
            .to(objectId)
            .set(COL_PROVENANCE)
            .to(provenance)
            .set(COL_LAST_UPDATE_TIMESTAMP)
            .to(Value.COMMIT_TIMESTAMP)
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
