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
import org.datacommons.ingestion.spanner.SpannerClient;

/** Immutable canonical record representing a single row in the Spanner KeyValueStore table. */
public record KeyValueStoreRecord(String type, String key, String provenance, String value)
    implements Serializable {

  public static final List<String> READ_COLUMNS =
      List.of(
          SpannerClient.COL_TYPE,
          SpannerClient.COL_KEY,
          SpannerClient.COL_PROVENANCE,
          SpannerClient.COL_VALUE);

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static KeyValueStoreRecord from(Struct struct) {
    return new KeyValueStoreRecord(
        struct.getString(SpannerClient.COL_TYPE),
        struct.getString(SpannerClient.COL_KEY),
        struct.getString(SpannerClient.COL_PROVENANCE),
        struct.isNull(SpannerClient.COL_VALUE) ? null : struct.getJson(SpannerClient.COL_VALUE));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set(SpannerClient.COL_TYPE)
        .to(type)
        .set(SpannerClient.COL_KEY)
        .to(key)
        .set(SpannerClient.COL_PROVENANCE)
        .to(provenance)
        .set(SpannerClient.COL_VALUE)
        .to(value != null ? Value.json(value) : Value.json(null))
        .build();
  }
}
