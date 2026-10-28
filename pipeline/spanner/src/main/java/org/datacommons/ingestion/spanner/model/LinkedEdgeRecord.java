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
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Immutable canonical record representing a single row in the Spanner LinkedEdge table.
 *
 * <p>LinkedEdge materializes transitive closures (currently {@code containedInPlace}) as {@code
 * (predicate, ancestor, child_type, child, provenance)} tuples. The aggregation pipeline writes
 * them under the generated provenance of the import that asserted the containment (the same {@code
 * generated/<import>} scheme as derived {@code Edge} rows), which lets the rollback pipeline scope
 * them by provenance exactly like {@code Edge}.
 */
public record LinkedEdgeRecord(
    String predicate, String ancestor, String childType, String child, String provenance)
    implements Serializable {

  public static final String COL_PREDICATE = "predicate";
  public static final String COL_ANCESTOR = "ancestor";
  public static final String COL_CHILD_TYPE = "child_type";
  public static final String COL_CHILD = "child";
  public static final String COL_PROVENANCE = "provenance";

  public static final List<String> READ_COLUMNS =
      List.of(COL_PREDICATE, COL_ANCESTOR, COL_CHILD_TYPE, COL_CHILD, COL_PROVENANCE);

  public static final Set<String> WRITABLE_COLUMNS =
      Set.of(COL_PREDICATE, COL_ANCESTOR, COL_CHILD_TYPE, COL_CHILD, COL_PROVENANCE);

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static LinkedEdgeRecord from(Struct struct) {
    return new LinkedEdgeRecord(
        struct.getString(COL_PREDICATE),
        struct.getString(COL_ANCESTOR),
        struct.getString(COL_CHILD_TYPE),
        struct.getString(COL_CHILD),
        struct.getString(COL_PROVENANCE));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set(COL_PREDICATE)
        .to(predicate)
        .set(COL_ANCESTOR)
        .to(ancestor)
        .set(COL_CHILD_TYPE)
        .to(childType)
        .set(COL_CHILD)
        .to(child)
        .set(COL_PROVENANCE)
        .to(provenance)
        .build();
  }
}
