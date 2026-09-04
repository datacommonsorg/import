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

"""Data models for ingestion helper configuration and workflows."""

from pydantic import BaseModel


class EmbeddingSpec(BaseModel):
    """Specification for generating and indexing embeddings for graph nodes.

    Attributes:
        embedding_label: Identifier key for the embedding dataset (e.g. 'base_text_embedding').
        model_name: Name of the Spanner ML model endpoint used for vector generation.
        task_type: Embedding task type passed to ML.PREDICT (e.g. 'RETRIEVAL_QUERY').
        node_types: Maps each node type (e.g. 'StatisticalVariable', 'Topic') to the list of
            predicate names (e.g. ['description']) whose connected object values will be embedded.
        node_filter_type: Node filtering strategy ('NoFilter' or 'NLStatisticalVariable').
    """
    embedding_label: str
    model_name: str
    task_type: str
    # Maps each node type to the list of predicate names to be read and embedded.
    node_types: dict[str, list[str]]
    node_filter_type: str
