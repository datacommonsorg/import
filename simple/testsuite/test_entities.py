# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Entities (importType: entities): each CSV row becomes a typed node.

Every test imports case.csv under the shared test provenance as rows of type
Author keyed by author_id, unless it says otherwise (see entities_config()
in lib/builders.py).
"""

from testsuite.lib.builders import assert_records
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases
from testsuite.lib.builders import class_node
from testsuite.lib.builders import csv_file
from testsuite.lib.builders import entities_config
from testsuite.lib.builders import entity
from testsuite.lib.builders import property_node
from testsuite.lib.builders import ref
from testsuite.lib.builders import run_csv


def _rows(result, row_type: str = "Author") -> list[dict]:
  return [n for n in result.nodes if n.get("@type") == f"dcid:{row_type}"]


def test_rows_become_typed_nodes_keyed_by_the_id_column():
  result = run_csv(
      csv_file(["author1", "Jane Doe", "country/USA"],
               ["author2", "Joe Smith", "country/CAN"],
               header=["author_id", "author_name", "author_country"]),
      entities_config())
  assert_records(result.nodes, [
      class_node("Author"),
      property_node("author_id"),
      property_node("author_name"),
      property_node("author_country"),
      entity("author1",
             author_id="author1",
             author_name="Jane Doe",
             author_country="country/USA"),
      entity("author2",
             author_id="author2",
             author_name="Joe Smith",
             author_country="country/CAN"),
  ])


def test_without_an_id_column_ids_are_generated():
  result = run_csv(
      csv_file(["Jane Doe", "country/USA"], ["Joe Smith", "country/CAN"],
               header=["author_name", "author_country"]),
      entities_config(id_column=None))
  assert_records(result.nodes, [
      class_node("Author"),
      property_node("author_name"),
      property_node("author_country"),
      entity("Author_0", author_name="Jane Doe", author_country="country/USA"),
      entity("Author_1", author_name="Joe Smith", author_country="country/CAN"),
  ])


def test_reference_columns_hold_one_reference_per_listed_value():
  """columnsToResolve marks reference columns in entities imports."""
  result = run_csv(
      csv_file(["article1", "author1"], ["article2", "author1,author2"],
               ["article3", "author2, author3"],
               header=["article_id", "article_author"]),
      entities_config("Article",
                      "article_id",
                      columnsToResolve=["article_author"]))
  assert_records(_rows(result, "Article"), [
      entity("article1",
             "Article",
             article_id="article1",
             article_author=ref("author1")),
      entity("article2",
             "Article",
             article_id="article2",
             article_author=[ref("author1"), ref("author2")]),
      entity("article3",
             "Article",
             article_id="article3",
             article_author=[ref("author2"), ref("author3")]),
  ])


VALUES = [
    Case("integer", given="12", expect=12),
    Case("decimal", given="3.5", expect=3.5),
    Case("text", given="Jane Doe", expect="Jane Doe"),
]


@cases(*VALUES)
def test_value(given, expect):
  result = run_csv(csv_file(["author1", given], header=["author_id", "value"]),
                   entities_config())
  assert_records(_rows(result),
                 [entity("author1", author_id="author1", value=expect)])


# A column becomes a Property named after it. Its id is the column name when
# that is a valid dcid, with spaces and dashes turned into underscores;
# otherwise a generated c/prop/<n>.
PROPERTY_IDS = [
    Case("valid dcid is kept", given="papers", expect="papers"),
    Case("space becomes underscore", given="my col", expect="my_col"),
    Case("dash becomes underscore", given="h-index", expect="h_index"),
    Case("non-ASCII gets a generated id", given="Catégorie", expect="c/prop/1"),
]


@cases(*PROPERTY_IDS)
def test_column_property_id(given, expect):
  result = run_csv(csv_file(["author1", "x"], header=["author_id", given]),
                   entities_config())
  assert_records(_rows(result),
                 [entity("author1", author_id="author1", **{expect: "x"})])
  assert property_node(given, expect) in result.nodes
