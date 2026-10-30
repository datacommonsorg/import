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

package org.datacommons.ingestion.pipeline.embedding;

import com.google.cloud.spanner.Statement;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.NodeRecord;

/**
 * Reads and filters candidate {@link NodeRecord}s for a single {@link EmbeddingSpec}.
 *
 * <p>Filters {@code Node} rows by configured {@code node_types}, and when {@link
 * NodeFilterType#ENTITY_TYPES} is specified, filters candidates to valid entity types (types
 * returned by {@link #ENTITY_TYPES_QUERY} unioned with {@link #PLACE_TYPE_ORDER}). Predicate and
 * edge properties are not processed here; they are resolved downstream in {@link
 * GenerateEmbeddingContentFn}.
 */
public final class ReadCandidateNodesFn {

  /**
   * Standard Data Commons place types always included in {@link NodeFilterType#ENTITY_TYPES} even
   * if no observations currently reference them in {@code TimeSeries}.
   */
  public static final List<String> PLACE_TYPE_ORDER =
      List.of(
          "Continent",
          "Ocean",
          "Country",
          "State",
          "AdministrativeArea1",
          "EurostatNUTS1",
          "EurostatNUTS2",
          "AdministrativeArea2",
          "EurostatNUTS3",
          "AdministrativeArea3",
          "AdministrativeArea4",
          "AdministrativeArea5",
          "County",
          "City",
          "Town",
          "Village",
          "Neighborhood",
          "Borough",
          "CensusZipCodeTabulationArea",
          "ZipCode",
          "CensusCountyDivision",
          "CensusTract",
          "CensusBlockGroup",
          "CongressionalDistrict",
          "SchoolDistrict",
          "ElementarySchoolDistrict",
          "SecondarySchoolDistrict",
          "StateComponent",
          "Place");

  /**
   * Spanner SQL query that finds all {@code Class} types that have at least one entity present in
   * {@code TimeSeries}.
   */
  static final String ENTITY_TYPES_QUERY =
      """
      WITH all_type_table AS (
          SELECT DISTINCT
              subject_id AS t
          FROM
              Edge@{FORCE_INDEX=InEdge}
          WHERE
              object_id = 'Class'
              AND predicate = 'typeOf'
      )
      SELECT
          c.t
      FROM
          all_type_table c
      INNER JOIN@{JOIN_METHOD=APPLY_JOIN}
          UNNEST(
              ARRAY(
                  SELECT
                      1
                  FROM
                      Edge@{FORCE_INDEX=InEdge} e
                  INNER JOIN@{JOIN_METHOD=APPLY_JOIN}
                      TimeSeries@{FORCE_INDEX=TimeSeriesByEntity1} ts
                      ON e.subject_id = ts.entity1
                  WHERE
                      e.object_id = c.t
                      AND e.predicate = 'typeOf'
                  LIMIT 1
              )
          )
      ORDER BY
          c.t""";

  private ReadCandidateNodesFn() {}

  /**
   * Step 1 of {@link EmbeddingPipeline}: reads candidate {@link NodeRecord}s (filtered by {@code
   * node_types} from config) and, when {@link NodeFilterType#ENTITY_TYPES} is set, filters them
   * against valid entity types from Spanner and {@link #PLACE_TYPE_ORDER}.
   */
  public static PCollection<NodeRecord> readCandidateNodes(
      Pipeline pipeline, SpannerClient spannerClient, EmbeddingSpec spec) {
    PCollection<NodeRecord> candidateNodes = readNodes(pipeline, spannerClient, spec);
    if (spec.nodeFilterType() == NodeFilterType.ENTITY_TYPES) {
      return filterByEntityTypes(candidateNodes, spannerClient, spec.embeddingLabel());
    }
    return candidateNodes;
  }

  /** Reads {@code Node} rows from Spanner filtered by {@code spec.getNodeTypeNames()}. */
  private static PCollection<NodeRecord> readNodes(
      Pipeline pipeline, SpannerClient spannerClient, EmbeddingSpec spec) {
    String specId = spec.embeddingLabel();
    String cols = String.join(", ", NodeRecord.READ_COLUMNS);
    String sql =
        spannerClient.formatPartitionQuery(
            "SELECT %s FROM %s WHERE %s IS NOT NULL AND %s != ''"
                + " AND EXISTS (SELECT 1 FROM UNNEST(%s) AS t WHERE t IN UNNEST(@node_types))",
            cols,
            spannerClient.getNodeTableName(),
            NodeRecord.COL_NAME,
            NodeRecord.COL_NAME,
            NodeRecord.COL_TYPES);
    Statement stmt =
        Statement.newBuilder(sql).bind("node_types").toStringArray(spec.getNodeTypeNames()).build();

    return pipeline
        .apply("ReadNodes_" + specId, spannerClient.getReadTransform().withQuery(stmt))
        .apply(
            "ParseNodes_" + specId,
            MapElements.into(TypeDescriptor.of(NodeRecord.class)).via(NodeRecord::from));
  }

  /**
   * Reads valid {@code Class} entity types from Spanner via {@link #ENTITY_TYPES_QUERY}, unions
   * them with {@link #PLACE_TYPE_ORDER}, and filters {@code candidateNodes} to keep only valid
   * entity types.
   */
  public static PCollection<NodeRecord> filterByEntityTypes(
      PCollection<NodeRecord> candidateNodes, SpannerClient spannerClient, String specId) {
    PCollection<String> entityTypes =
        candidateNodes
            .getPipeline()
            .apply(
                "ReadEntityTypes_" + specId,
                spannerClient
                    .getReadTransform()
                    .withBatching(false)
                    .withQuery(Statement.of(ENTITY_TYPES_QUERY)))
            .apply(
                "ExtractEntityTypeDcid_" + specId,
                MapElements.into(TypeDescriptors.strings()).via(struct -> struct.getString("t")));

    return filterByEntityTypes(candidateNodes, entityTypes, specId);
  }

  /**
   * Unions {@code entityTypes} with {@link #PLACE_TYPE_ORDER} and joins with {@code candidateNodes}
   * on {@code subject_id} to keep only valid entity types.
   */
  static PCollection<NodeRecord> filterByEntityTypes(
      PCollection<NodeRecord> candidateNodes, PCollection<String> entityTypes, String specId) {
    PCollection<KV<String, NodeRecord>> keyedCandidates =
        candidateNodes.apply(
            "KeyCandidatesBySubjectId_" + specId,
            WithKeys.of(NodeRecord::subjectId).withKeyType(TypeDescriptors.strings()));

    PCollection<String> placeTypes =
        candidateNodes
            .getPipeline()
            .apply("CreatePlaceTypes_" + specId, Create.of(PLACE_TYPE_ORDER));

    PCollection<KV<String, Boolean>> validEntityTypes =
        PCollectionList.of(entityTypes)
            .and(placeTypes)
            .apply("FlattenValidEntityTypes_" + specId, Flatten.pCollections())
            .apply("FilterNonEmptyEntityTypes_" + specId, Filter.by(t -> t != null && !t.isEmpty()))
            .apply("DistinctValidEntityTypes_" + specId, Distinct.create())
            .apply(
                "KeyValidEntityTypes_" + specId,
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.booleans()))
                    .via(type -> KV.of(type, Boolean.TRUE)));

    final TupleTag<NodeRecord> candidateNodeTag = new TupleTag<>();
    final TupleTag<Boolean> validTypeTag = new TupleTag<>();

    return KeyedPCollectionTuple.of(candidateNodeTag, keyedCandidates)
        .and(validTypeTag, validEntityTypes)
        .apply("JoinCandidatesWithEntityTypes_" + specId, CoGroupByKey.create())
        .apply(
            "KeepValidEntityTypeCandidates_" + specId,
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, NodeRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CoGbkResult res = c.element().getValue();
                    if (res.getAll(validTypeTag).iterator().hasNext()) {
                      res.getAll(candidateNodeTag).forEach(c::output);
                    }
                  }
                }));
  }
}
