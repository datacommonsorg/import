# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Spanner query templates and constants for embedding workflows."""

EMBEDDING_JSON_GENERATION = """CASE 
    WHEN COUNT(pred) > 0 THEN
    JSON_OBJECT(
        "subject_id", n.subject_id,
        "name", n.name,
        "properties", JSON_OBJECT(
        ARRAY_AGG(pred IGNORE NULLS),
        ARRAY_AGG(TO_JSON(values) IGNORE NULLS)
        )
    )
    ELSE
    JSON_OBJECT(
        "subject_id", n.subject_id,
        "name", n.name
    )
END"""

TIMESTAMP_CONDITION = "IF(@timestamp IS NOT NULL, n.last_update_timestamp > @timestamp, TRUE)"

EMBEDDING_CONTENT_QUERY_BY_NODE_TYPE = f"""MATCH
(n:Node WHERE "{{node_type}}" IN UNNEST(n.types) AND {{filter_condition}} AND {TIMESTAMP_CONDITION})
OPTIONAL MATCH
(n)-[e: Edge
    WHERE e.predicate IN UNNEST({{predicate_types_list_sql}})]->
(o:Node
    WHERE o.value IS NOT NULL
    AND o.value <> "")
WITH
    n,
    e.predicate AS pred,
    STRING_AGG(o.value, ". ") AS values
GROUP BY n, pred
RETURN
n.subject_id AS subject_id,
n.types AS node_types,
{EMBEDDING_JSON_GENERATION} AS embedding_content
GROUP BY n"""
