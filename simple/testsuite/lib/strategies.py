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
"""Generators for random, valid preprocessor inputs, with their expected output.

Each generator draws a small structured description of the input (what a
person would write), renders it to the file format (MCF text, CSV text), and
states what the preprocessor must produce from the description alone. The
expectation is written from the *specification*, never by reusing the
preprocessor's own parsing rules:

  ids         "dcid:<id>" (dcid:/dcs:/schema: prefixes normalized)
  references  {"@id": "dcid:<id>"}; typeOf becomes "@type"
  text        a quoted MCF value is a JSON string, unchanged
  numbers     an unquoted MCF number, and every observation value, is a JSON
              number with the same numeric value (int vs float spelling is
              not part of the spec: compare with `normalize`)
  dates       observationDate reads back as the date written in the CSV
  multi-value repeated values are dropped; one value is a scalar, several a
              list (compared order-insensitively)

Inputs whose handling is a known bug are not generated; each exclusion names
the case that documents it instead:

  quoted text that looks like a number  -> "quoted number stays a string"
                                           (cases/mcf_nodes.md)
  non-canonical numbers (007, 1e5, inf) -> "numeric spellings" and
                                           "exponent values are emitted as
                                           numbers"
"""

import csv
import dataclasses
import io
import random
import re

from hypothesis import strategies as st

# --- Shared pieces -----------------------------------------------------------

_ID_PART = st.from_regex(r"[A-Za-z][A-Za-z0-9]{0,7}", fullmatch=True)
LOCAL_ID = st.builds(lambda parts: "/".join(parts),
                     st.lists(_ID_PART, min_size=1, max_size=3))
CLASS_NAMES = st.sampled_from(
    ["Thing", "Place", "Country", "Person", "Organization", "Event", "Article"])
# Property names that are not special-cased anywhere in the preprocessor.
PROPERTY = st.from_regex(r"p[A-Z][a-zA-Z0-9]{0,8}", fullmatch=True)

_TEXT_ALPHABET = ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                  "0123456789 ,.-/:()'&%éüßñ東京🚀")
TEXT = st.text(alphabet=_TEXT_ALPHABET, min_size=1, max_size=30).map(
    str.strip).filter(lambda t: t and not looks_numeric(t))
# Canonical spellings only: no leading zeros, no exponent.
INT_TEXT = st.integers(min_value=-10**9, max_value=10**9).map(str)
FLOAT_TEXT = st.builds(lambda i, f, neg: f"{'-' if neg else ''}{i}.{f}",
                       st.integers(0, 10**6),
                       st.from_regex(r"[0-9]{1,4}", fullmatch=True),
                       st.booleans())

# Anything a reader could take for a number (see the module docstring).
_NUMBER_LIKE = re.compile(
    r"^\s*[-+]?(\d[\d_]*)?(\.\d*)?([eE][-+]?\d+)?\s*$"
    r"|^\s*[-+]?(inf|infinity|nan)\s*$", re.IGNORECASE)


def looks_numeric(text: str) -> bool:
  return bool(_NUMBER_LIKE.match(text)) and any(c.isalnum() for c in text)


def normalize(value):
  """Spec-level comparison form: all JSON numbers become floats."""
  if isinstance(value, bool):
    return value
  if isinstance(value, (int, float)):
    return float(value)
  if isinstance(value, list):
    return sorted((normalize(v) for v in value), key=repr)
  if isinstance(value, dict):
    return {k: normalize(v) for k, v in value.items()}
  return value


def value_set(values: list) -> object:
  unique = []
  for v in values:
    if v not in unique:
      unique.append(v)
  return unique[0] if len(unique) == 1 else unique


# --- MCF nodes -----------------------------------------------------------------


@dataclasses.dataclass
class McfValue:
  text: str  # As written in MCF.
  expected: object  # As emitted in JSON-LD.


@st.composite
def mcf_value(draw) -> McfValue:
  kind = draw(st.sampled_from(["ref", "text", "int", "float"]))
  if kind == "ref":
    target = draw(LOCAL_ID)
    prefix = draw(st.sampled_from(["dcid:", "dcs:", "schema:"]))
    return McfValue(f"{prefix}{target}", {"@id": f"dcid:{target}"})
  if kind == "text":
    text = draw(TEXT)
    return McfValue(f'"{text}"', text)
  number = draw(INT_TEXT if kind == "int" else FLOAT_TEXT)
  return McfValue(number, float(number))


@dataclasses.dataclass
class McfNode:
  dcid: str
  subject_style: str  # "dcid", "local" or "dcid_property"
  types: list[str]
  props: dict[str, list[McfValue]]

  def render(self, index: int) -> str:
    if self.subject_style == "dcid_property":
      lines = [f"Node: Local{index}", f'dcid: "{self.dcid}"']
    else:
      lines = [f"Node: dcid:{self.dcid}"]
    lines.append("typeOf: " + ", ".join(f"dcs:{t}" for t in self.types))
    for prop, values in self.props.items():
      refs = [v for v in values if isinstance(v.expected, dict)]
      if refs and len(refs) == len(values):
        lines.append(f"{prop}: " + ", ".join(v.text for v in values))
      else:
        # Mixed or literal values: one line per value (MCF allows repeats).
        lines.extend(f"{prop}: {v.text}" for v in values)
    return "\n".join(lines) + "\n"

  def expected(self) -> dict:
    record = {
        "@id": f"dcid:{self.dcid}",
        "@type": value_set([f"dcid:{t}" for t in self.types]),
    }
    for prop, values in self.props.items():
      record[f"dcid:{prop}"] = value_set([v.expected for v in values])
    return record


@st.composite
def mcf_node(draw, dcid: str) -> McfNode:
  return McfNode(dcid=dcid,
                 subject_style=draw(st.sampled_from(["dcid", "dcid_property"])),
                 types=draw(
                     st.lists(CLASS_NAMES, min_size=1, max_size=2,
                              unique=True)),
                 props=draw(
                     st.dictionaries(PROPERTY,
                                     st.lists(mcf_value(),
                                              min_size=1,
                                              max_size=3),
                                     max_size=5)))


@st.composite
def mcf_file(draw) -> tuple[str, list[dict]]:
  """An MCF file with 1-6 distinct nodes, and their expected records."""
  ids = draw(
      st.lists(LOCAL_ID, min_size=1, max_size=6,
               unique_by=lambda s: s.lower()).filter(lambda ids: not any(
                   i in ("TestSource", "TestProvenance") for i in ids)))
  nodes = [draw(mcf_node(i)) for i in ids]
  text = "\n".join(n.render(k) for k, n in enumerate(nodes))
  return text, [n.expected() for n in nodes]


# --- Observation CSVs ---------------------------------------------------------

ENTITY = st.one_of(
    st.from_regex(r"country/[A-Z]{3}", fullmatch=True),
    st.from_regex(r"geoId/[0-9]{2,5}", fullmatch=True),
    st.builds(lambda s: f"wikidataId/Q{s}", st.integers(1, 10**7)),
)
VARIABLE = st.sampled_from([
    "Count_Person", "Median_Age_Person", "Amount_Debt", "Count_Person_Female",
    "UnemploymentRate_Person"
])
DATE = st.one_of(
    st.integers(1900, 2100).map(str),
    st.builds(lambda y, m: f"{y}-{m:02d}", st.integers(1900, 2100),
              st.integers(1, 12)),
)
VALUE = st.one_of(INT_TEXT, FLOAT_TEXT)


@dataclasses.dataclass
class ObservationCsv:
  text: str
  column_mappings: dict[str, str]
  ignore_columns: list[str]
  expected: list[dict]  # Expected records, without "@id".


@st.composite
def observation_csv(draw) -> ObservationCsv:
  """A variable-per-row CSV with random column names, order and quoting."""
  rows = draw(
      st.lists(st.tuples(ENTITY, VARIABLE, DATE, VALUE),
               min_size=1,
               max_size=12))
  with_unit = draw(st.booleans())
  units = [
      draw(st.sampled_from(["USDollar", "Year", "Percent", ""])) for _ in rows
  ] if with_unit else []
  with_notes = draw(st.booleans())
  notes = [draw(st.text(alphabet='ab ,"\n', max_size=12)) for _ in rows
          ] if with_notes else []

  logical = ["entity", "variable", "date", "value"] + (
      ["unit"] if with_unit else []) + (["notes"] if with_notes else [])
  physical = {
      name: draw(st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True))
      for name in logical
  }
  if len(set(physical.values())) != len(physical):
    physical = {name: f"{name}_col" for name in logical}
  order = draw(st.permutations(logical))
  rng = random.Random(draw(st.integers(0, 2**32)))
  quoting = rng.choice([csv.QUOTE_MINIMAL, csv.QUOTE_ALL])
  line_end = rng.choice(["\n", "\r\n"])

  out = io.StringIO()
  writer = csv.writer(out, quoting=quoting, lineterminator=line_end)
  writer.writerow([physical[c] for c in order])
  for i, (entity, variable, date, value) in enumerate(rows):
    cells = {
        "entity": entity,
        "variable": variable,
        "date": date,
        "value": value
    }
    if with_unit:
      cells["unit"] = units[i]
    if with_notes:
      cells["notes"] = notes[i]
    writer.writerow([cells[c] for c in order])

  mappings = {
      "dcid:observationAbout": physical["entity"],
      "dcid:variableMeasured": physical["variable"],
      "dcid:observationDate": physical["date"],
      "dcid:value": physical["value"],
  }
  if with_unit:
    mappings["dcid:unit"] = physical["unit"]

  expected = []
  for i, (entity, variable, date, value) in enumerate(rows):
    record = {
        "@type": "dcid:StatVarObservation",
        "dcid:observationAbout": {
            "@id": f"dcid:{entity}"
        },
        "dcid:variableMeasured": {
            "@id": f"dcid:{variable}",
            "dcid:observationProperties": ["dcid:observationAbout"],
        },
        "dcid:observationDate": date,
        "dcid:value": float(value),
        "dcid:provenance": {
            "@id": "dcid:TestProvenance"
        },
    }
    if with_unit and units[i]:
      record["dcid:unit"] = {"@id": f"dcid:{units[i]}"}
    expected.append(record)
  return ObservationCsv(
      text=out.getvalue(),
      column_mappings=mappings,
      ignore_columns=[physical["notes"]] if with_notes else [],
      expected=expected)


PROVENANCE_MCF = """Node: dcid:TestSource
typeOf: dcs:Source
name: "Test Source"
url: "https://source.example.org"

Node: dcid:TestProvenance
typeOf: dcs:Provenance
name: "Test Provenance"
source: dcid:TestSource
"""
