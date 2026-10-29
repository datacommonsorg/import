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
"""
Entities (importType: entities): each CSV row becomes a typed node.

Every test imports case.csv under the shared test provenance as rows of type
Author keyed by author_id, unless it says otherwise (see entities_config()
in lib/builders.py).
"""

from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases


def test_rows_become_typed_nodes_keyed_by_the_id_column():
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe", "country/USA"],
                 ["author2", "Joe Smith", "country/CAN"],
                 header=["author_id", "author_name", "author_country"]),
      b.entities_config())
  b.assert_records(result.nodes, [
      b.class_node("Author"),
      b.property_node("author_id"),
      b.property_node("author_name"),
      b.property_node("author_country"),
      b.entity("author1",
               author_id="author1",
               author_name="Jane Doe",
               author_country="country/USA"),
      b.entity("author2",
               author_id="author2",
               author_name="Joe Smith",
               author_country="country/CAN"),
  ])


def test_without_an_id_column_ids_are_generated():
  result = b.run_csv(
      b.csv_file(["Jane Doe", "country/USA"], ["Joe Smith", "country/CAN"],
                 header=["author_name", "author_country"]),
      b.entities_config(id_column=None))
  b.assert_records(result.nodes, [
      b.class_node("Author"),
      b.property_node("author_name"),
      b.property_node("author_country"),
      b.entity("Author_0", author_name="Jane Doe",
               author_country="country/USA"),
      b.entity(
          "Author_1", author_name="Joe Smith", author_country="country/CAN"),
  ])


def test_entity_type_name_and_description_from_config():
  config = {
      **b.entities_config(),
      "entities": {
          "Author": {
              "name": "Published Author",
              "description": "A person who wrote an article",
          }
      },
  }
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author_id", "author_name"]),
      config)
  b.assert_records(result.of_type("Class"), [
      b.record("Author",
               "Class",
               name="Published Author",
               description="A person who wrote an article",
               includedIn=[b.ref(b.PROVENANCE),
                           b.ref(b.SOURCE)]),
  ])


def test_reference_columns_hold_one_reference_per_listed_value():
  """In entities imports, columnsToResolve marks reference columns."""
  result = b.run_csv(
      b.csv_file(["article1", "author1"], ["article2", "author1,author2"],
                 ["article3", "author2, author3"],
                 header=["article_id", "article_author"]),
      b.entities_config("Article",
                        "article_id",
                        columnsToResolve=["article_author"]))
  b.assert_records(result.of_type("Article"), [
      b.entity("article1",
               "Article",
               article_id="article1",
               article_author=b.ref("author1")),
      b.entity("article2",
               "Article",
               article_id="article2",
               article_author=[b.ref("author1"),
                               b.ref("author2")]),
      b.entity("article3",
               "Article",
               article_id="article3",
               article_author=[b.ref("author2"),
                               b.ref("author3")]),
  ])


VALUES = [
    Case("integer", given="12", expect=12),
    Case("decimal", given="3.5", expect=3.5),
    Case("text", given="Jane Doe", expect="Jane Doe"),
]


@cases(*VALUES)
def test_value(given, expect):
  result = b.run_csv(
      b.csv_file(["author1", given], header=["author_id", "value"]),
      b.entities_config())
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_id="author1", value=expect)])


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
  result = b.run_csv(b.csv_file(["author1", "x"], header=["author_id", given]),
                     b.entities_config())
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_id="author1", **{expect: "x"})])
  assert b.property_node(given, expect) in result.nodes
