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
Author keyed by author_id, unless it says otherwise, mapping exactly its CSV's
columns (see entities_config() in lib/builders.py).
"""

import pytest
from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases


def test_rows_become_typed_nodes_keyed_by_the_id_column():
  result = b.run(
      {
          "case.csv":
              b.csv_file(["author1", "Jane Doe", "country/USA"],
                         ["author2", "Joe Smith", "country/CAN"],
                         header=["author_id", "author_name", "author_country"]),
          "schema.mcf":
              b.property_mcf("author_id", "author_name", "author_country"),
      }, b.entities_config(mappings=b.AUTHOR_MAPPINGS))
  b.assert_records(result.nodes, [
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
  result = b.run(
      {
          "case.csv":
              b.csv_file(["Jane Doe", "country/USA"],
                         ["Joe Smith", "country/CAN"],
                         header=["author_name", "author_country"]),
          "schema.mcf":
              b.property_mcf("author_name", "author_country"),
      },
      b.entities_config(id_column=None,
                        mappings=b.same_name_mappings("author_name",
                                                      "author_country")))
  b.assert_records(result.nodes, [
      b.property_node("author_name"),
      b.property_node("author_country"),
      b.entity("Author_0", author_name="Jane Doe",
               author_country="country/USA"),
      b.entity(
          "Author_1", author_name="Joe Smith", author_country="country/CAN"),
  ])


def test_no_class_node_is_written_for_the_row_type():
  """Rows reference their type; the Class node itself must come from MCF."""
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author_id", "author_name"]),
      b.entities_config(
          mappings=b.same_name_mappings("author_id", "author_name")))
  b.assert_records(result.of_type("Class"), [])


def test_class_node_for_the_row_type_is_defined_in_mcf():
  result = b.run(
      {
          "case.csv":
              b.csv_file(["author1", "Jane Doe"],
                         header=["author_id", "author_name"]),
          "schema.mcf":
              b.mcf_node("dcid:Author",
                         typeOf="dcs:Class",
                         name='"Published Author"',
                         description='"A person who wrote an article"'),
      },
      b.entities_config(
          mappings=b.same_name_mappings("author_id", "author_name")))
  b.assert_records(result.of_type("Class"), [
      b.record("Author",
               "Class",
               name="Published Author",
               description="A person who wrote an article"),
  ])


_ARTICLE_MAPPINGS = b.same_name_mappings("article_id", "article_author")


def test_reference_columns_hold_one_reference_per_listed_value():
  """In entities imports, columnsToResolve (or referenceColumns) marks reference columns."""
  result = b.run_csv(
      b.csv_file(["article1", "author1"], ["article2", "author1,author2"],
                 ["article3", "author2, author3"],
                 header=["article_id", "article_author"]),
      b.entities_config("Article",
                        "article_id",
                        mappings=_ARTICLE_MAPPINGS,
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


def test_reference_columns_field_marks_reference_columns():
  """ReferenceColumns is accepted as the clearer name for columnsToResolve."""
  result = b.run_csv(
      b.csv_file(["article1", "author1"], ["article2", "author1,author2"],
                 header=["article_id", "article_author"]),
      b.entities_config("Article",
                        "article_id",
                        mappings=_ARTICLE_MAPPINGS,
                        referenceColumns=["article_author"]))
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
      b.entities_config(mappings=b.same_name_mappings("author_id", "value")))
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_id="author1", value=expect)])


# A column's values are written under the property it is mapped to, whatever
# its header.
PREDICATES = [
    Case("header named like its property", given="papers", expect="papers"),
    Case("header with a space", given="my col", expect="my_col"),
    Case("header with a dash", given="h-index", expect="h_index"),
    Case("non-ASCII header", given="Catégorie", expect="category"),
]


@cases(*PREDICATES)
def test_column_is_written_under_its_mapped_property(given, expect):
  result = b.run_csv(
      b.csv_file(["author1", "x"], header=["author_id", given]),
      b.entities_config(mappings={
          "dcid:author_id": "author_id",
          f"dcid:{expect}": given,
      }))
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_id="author1", **{expect: "x"})])


def test_id_column_does_not_need_a_mapping():
  """An unmapped id column only supplies the dcids; it is not a property."""
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author id", "author_name"]),
      b.entities_config(id_column="author id",
                        mappings=b.same_name_mappings("author_name")))
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_name="Jane Doe")])


def test_ignored_column_does_not_need_a_mapping():
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe", "anything"],
                 header=["author_id", "author_name", "notes"]),
      b.entities_config(mappings=b.same_name_mappings("author_id",
                                                      "author_name"),
                        ignoreColumns=["notes"]))
  b.assert_records(
      result.of_type("Author"),
      [b.entity("author1", author_id="author1", author_name="Jane Doe")])


def test_csv_without_column_mappings_fails():
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author_id", "author_name"]),
      b.without_column_mappings(
          b.entities_config(
              mappings=b.same_name_mappings("author_id", "author_name"))))
  b.assert_fails(result,
                 "ValueError",
                 "Missing 'columnMappings' for CSV file 'case.csv' in"
                 " config.json. 'columnMappings' is required for every CSV"
                 " input file",
                 handshake_error_types=("INVALID_CONFIGURATION",))


def test_every_unmapped_column_is_reported_at_once():
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe", "x", "y"],
                 header=["author_id", "author_name", "Catégorie", "h-index"]),
      b.entities_config(mappings=b.same_name_mappings("author_name")))
  b.assert_fails(result,
                 "ValueError",
                 "The CSV file 'case.csv' contains unmapped columns:"
                 " ['Catégorie', 'h-index']. Please map them in"
                 " 'columnMappings' or list them in 'ignoreColumns'",
                 handshake_error_types=("UNMAPPED_COLUMNS",))


def test_columns_mapped_to_the_same_property_fail():
  """dcid:author_name and author_name are one property, so a and b collide."""
  result = b.run_csv(
      b.csv_file(["author1", "Jane", "Doe"], header=["author_id", "a", "b"]),
      b.entities_config(mappings={
          "dcid:author_name": "a",
          "author_name": "b",
      }))
  b.assert_fails(result,
                 "ValueError",
                 "The CSV file 'case.csv' maps several columns to the same"
                 " property: {'author_name': ['a', 'b']}. Map each property"
                 " from one column.",
                 handshake_error_types=("INVALID_CONFIGURATION",))


def test_column_mapped_to_several_properties_fails():
  """Properties are looked up by column, so a second mapping would be lost."""
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author_id", "author_name"]),
      b.entities_config(mappings={
          "dcid:name": "author_name",
          "dcid:alternateName": "author_name",
      }))
  b.assert_fails(result,
                 "ValueError",
                 "The CSV file 'case.csv' maps the same column to several"
                 " properties: {'author_name': ['alternateName', 'name']}. Map"
                 " each column to one property.",
                 handshake_error_types=("INVALID_CONFIGURATION",))


def test_every_mapped_column_missing_from_the_csv_is_reported_at_once():
  """A mapping for a column the CSV doesn't have is a config error too."""
  result = b.run_csv(
      b.csv_file(["author1"], header=["author_id"]),
      b.entities_config(
          mappings=b.same_name_mappings("author_name", "author_country")))
  b.assert_fails(result,
                 "ValueError",
                 "The following expected columns were not found in the CSV:"
                 " ['author_country', 'author_name']",
                 handshake_error_types=("MISSING_REQUIRED_COLUMNS",))


def test_id_column_missing_from_the_csv_fails():
  result = b.run_csv(
      b.csv_file(["Jane Doe"], header=["author_name"]),
      b.entities_config(mappings=b.same_name_mappings("author_name")))
  b.assert_fails(result,
                 "ValueError",
                 "The following expected columns were not found in the CSV:"
                 " ['author_id']",
                 handshake_error_types=("MISSING_REQUIRED_COLUMNS",))


def test_no_property_nodes_are_written_for_columns():
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author_id", "author_name"]),
      b.entities_config(
          mappings=b.same_name_mappings("author_id", "author_name")))
  b.assert_records(result.of_type("Property"), [])


def test_property_node_for_a_column_is_defined_in_mcf():
  result = b.run(
      {
          "case.csv":
              b.csv_file(["author1", "Jane Doe"],
                         header=["author_id", "author_name"]),
          "schema.mcf":
              b.mcf_node("dcid:author_name",
                         typeOf="dcs:Property",
                         name='"Author name"',
                         description='"Full name of the author"'),
      },
      b.entities_config(
          mappings=b.same_name_mappings("author_id", "author_name")))
  b.assert_records(result.of_type("Property"), [
      b.record("author_name",
               "Property",
               name="Author name",
               description="Full name of the author"),
  ])
  b.assert_records(result.of_type("Property"), [
      b.record("author_name",
               "Property",
               name="Author name",
               description="Full name of the author"),
  ])
