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
"""Tests for splitting large observation CSVs and MCF files across workers."""

import logging

import pytest
from stats import file_split
from testsuite.lib import builders as b
from testsuite.lib import canonical


def _assert_same_output(actual: b.Result, expected: b.Result) -> None:
  """Asserts two runs produced identical records, report, handshake, and error."""
  assert canonical.group_by_folder(
      actual.output["records"]) == canonical.group_by_folder(
          expected.output["records"])
  for key in ("ingestion_report", "handshake", "error"):
    assert actual.output[key] == expected.output[key], key


def _plan_csv(data: bytes, parts: int = 4) -> list[file_split.FilePart] | None:
  return file_split.plan_csv_parts(lambda s, e: data[s:e], len(data), parts)


def _plan_mcf(
    data: bytes,
    parts: int = 4,
    window_bytes: int = file_split.WINDOW_BYTES,
) -> list[file_split.FilePart] | None:
  return file_split.plan_mcf_parts(lambda s, e: data[s:e], len(data), parts,
                                   window_bytes)


def test_num_parts_and_plan_csv_parts():
  assert file_split.num_parts(100, max_parts=8, split_bytes=100) == 1
  assert file_split.num_parts(101, max_parts=8, split_bytes=100) == 4
  assert file_split.num_parts(1000, max_parts=8, split_bytes=100) == 8

  data = b"about,variable,date,value\n" + b"".join(
      f"country/C{i},Count_Person,2020,{i}\n".encode() for i in range(20))
  parts = _plan_csv(data, parts=4)
  assert parts is not None and len(parts) == 4
  # Parts tile [header_end, len(data)) contiguously with no gaps or overlaps.
  assert parts[0].start == parts[0].header_end
  assert parts[-1].end == len(data)
  assert all(a.end == b_.start for a, b_ in zip(parts, parts[1:]))

  # Files that must not be split return None.
  for unsplit in [
      b"",
      b"a,b\n",
      b"a,b\n1,2\n",
      b'"col\n1",b\n1,2\n3,4\n',  # multi-line header
      b"a,b\n1,2,extra\n3,4,extra\n",  # implicit index
  ]:
    assert _plan_csv(unsplit) is None, unsplit


@pytest.mark.parametrize("mode", ["disk_input", "gcs_input"])
def test_split_matches_unsplit_records_report_and_collisions(mode, caplog):
  """Split output, report, handshake and cross-part collisions match unsplit."""
  rows = [b.obs_row(f"country/C{i}", value=str(i)) for i in range(12)]
  # Duplicate key far apart so the two rows land in different parts.
  rows.append(b.obs_row("country/C0", value="99"))
  files = b.import_subdir("oecd", b.csv_file(*rows))

  unsplit = b.run_dir(files,
                      mode=mode,
                      runner_kwargs={
                          "import_names": ["oecd"],
                          "max_workers": 4,
                          "split_file_bytes": 10**9,
                      })
  caplog.clear()
  with caplog.at_level(logging.WARNING):
    split = b.run_dir(files,
                      mode=mode,
                      runner_kwargs={
                          "import_names": ["oecd"],
                          "max_workers": 4,
                          "split_file_bytes": 1,
                      })

  assert split.error is None, split.error
  assert split.output == unsplit.output
  summaries = [
      r.getMessage()
      for r in caplog.records
      if r.getMessage().startswith("Observation @ID Collision Summary:")
  ]
  assert len(summaries) == 1
  assert "Total of 1 observation @id collisions" in summaries[0]
  assert "also produced by another part of the file" in summaries[0]


def test_split_falls_back_to_whole_file_when_part_fails(caplog):
  """A split inside a quoted newline deletes partial shards and imports whole."""
  header = b.OBS_HEADER + ["notes"]
  rows = [b.obs_row(f"country/C{i}", value=str(i)) + ["ok"] for i in range(6)]
  # Quoted newlines spanning the middle of the file force a split boundary
  # inside the quoted field so that part fails while other parts write shards.
  rows[3][-1] = "\n".join(f"line{j}" for j in range(40))
  files = {
      "case.csv": b.csv_file(*rows, header=header),
      "provenance.mcf": b.PROVENANCE_MCF,
  }
  config = b.obs_config(b.OBS_MAPPINGS, ignoreColumns=["notes"])

  unsplit = b.run_dir(files,
                      config,
                      runner_kwargs={
                          "max_workers": 4,
                          "split_file_bytes": 10**9,
                      })
  caplog.clear()
  with caplog.at_level(logging.WARNING):
    split = b.run_dir(files,
                      config,
                      runner_kwargs={
                          "max_workers": 4,
                          "split_file_bytes": 1,
                      })

  assert split.error is None, split.error
  _assert_same_output(split, unsplit)
  assert any(
      "failed to import, so the file is imported whole" in r.getMessage()
      for r in caplog.records)


def test_file_whose_part_shards_could_collide_is_not_split():
  """case.csv is not split when case-p000.csv is in the same run."""
  files = {
      **b.import_subdir(
          "oecd",
          b.csv_file(*[
              b.obs_row(f"country/C{i}", value=str(i)) for i in range(6)
          ])),
      "oecd/case-p000.csv":
          b.csv_file(b.obs_row("country/Z", value="42")),
  }
  unsplit = b.run_dir(files,
                      runner_kwargs={
                          "import_names": ["oecd"],
                          "max_workers": 4,
                          "split_file_bytes": 10**9,
                      })
  split = b.run_dir(files,
                    runner_kwargs={
                        "import_names": ["oecd"],
                        "max_workers": 4,
                        "split_file_bytes": 1,
                    })

  assert split.error is None, split.error
  assert split.output == unsplit.output


def test_plan_mcf_parts():
  nodes = b"\n".join(
      f'Node: dcid:N{i}\ntypeOf: dcs:Thing\nname: "Item {i}"\n'.encode()
      for i in range(12))

  # Normal multi-node MCF (and with UTF-8 BOM): header_end == 0.
  for data in (nodes, b"\xef\xbb\xbf" + nodes):
    parts = _plan_mcf(data, parts=4)
    assert parts is not None and len(parts) == 4
    assert all(p.header_end == 0 for p in parts)
    assert parts[0].start == 0
    assert parts[-1].end == len(data)
    assert all(a.end == b_.start for a, b_ in zip(parts, parts[1:]))
    assert all(data[p.start:].startswith(b"Node:") for p in parts[1:])

  # Leading Context:/namespace: block becomes [0, header_end) for every part.
  ctx = b'Context:\nnamespace: "ex=https://example.org/ns/"\n\n'
  data_ctx = ctx + nodes
  parts_ctx = _plan_mcf(data_ctx, parts=4)
  assert parts_ctx is not None and len(parts_ctx) == 4
  assert all(p.header_end == len(ctx) for p in parts_ctx)
  assert parts_ctx[0].start == len(ctx)
  assert parts_ctx[-1].end == len(data_ctx)
  assert all(a.end == b_.start for a, b_ in zip(parts_ctx, parts_ctx[1:]))
  assert all(data_ctx[p.start:].startswith(b"Node:") for p in parts_ctx)

  # Small windows where `\nNode:` straddles window edges match full-window plan.
  for window_bytes in (7, 16):
    assert _plan_mcf(data_ctx, parts=4, window_bytes=window_bytes) == parts_ctx

  # Single-node, header-only, or empty MCFs must not be split.
  for unsplit in [
      b"",
      b"# comment only\n",
      ctx,
      b"Node: dcid:OnlyOne\ntypeOf: dcs:Thing\n",
      ctx + b"Node: dcid:OnlyOne\ntypeOf: dcs:Thing\n",
  ]:
    assert _plan_mcf(unsplit) is None, unsplit


@pytest.mark.parametrize("mode", ["disk_input", "gcs_input"])
def test_mcf_split_matches_unsplit_records_report_and_metadata(mode):
  """Split MCF output, report, handshake, Context, and metadata match unsplit."""
  ctx = 'Context:\nnamespace: "ex=https://example.org/ns/"\n\n'
  nodes = "\n".join(f'Node: dcid:Item{i}\ntypeOf: dcs:Thing\nname: "Item {i}"\n'
                    f"related: ex:Ref{i}\n" for i in range(16))
  files = {"nodes.mcf": ctx + b.PROVENANCE_MCF + "\n" + nodes}

  unsplit = b.run_dir(files,
                      b.MCF_CONFIG,
                      mode=mode,
                      runner_kwargs={
                          "max_workers": 4,
                          "split_file_bytes": 10**9,
                      })
  split = b.run_dir(files,
                    b.MCF_CONFIG,
                    mode=mode,
                    runner_kwargs={
                        "max_workers": 4,
                        "split_file_bytes": 1,
                    })

  assert split.error is None, split.error
  _assert_same_output(split, unsplit)
  assert any("https://example.org/ns/Ref0" in str(r) for r in split.records)


def test_mcf_split_falls_back_to_whole_file_when_part_fails(caplog):
  """A node's dcid in a later part triggers fallback and cleans partial shards."""
  early = "\n".join(
      f'Node: dcid:Early{i}\ntypeOf: dcs:Thing\nname: "Early {i}"\n'
      for i in range(4))
  local_def = 'Node: LocalSplitNode\ntypeOf: dcs:Thing\nname: "Split"\n'
  middle = "\n".join(f'Node: dcid:Mid{i}\ntypeOf: dcs:Thing\nname: "Mid {i}"\n'
                     for i in range(12))
  local_dcid = 'Node: LocalSplitNode\ndcid: "ResolvedSplitNode"\n'
  files = {
      "provenance.mcf": b.PROVENANCE_MCF,
      "nodes.mcf": "\n".join([early, local_def, middle, local_dcid]),
  }

  unsplit = b.run_dir(files,
                      b.MCF_CONFIG,
                      runner_kwargs={
                          "max_workers": 4,
                          "split_file_bytes": 10**9,
                      })
  caplog.clear()
  with caplog.at_level(logging.WARNING):
    split = b.run_dir(files,
                      b.MCF_CONFIG,
                      runner_kwargs={
                          "max_workers": 4,
                          "split_file_bytes": 1,
                      })

  assert split.error is None, split.error
  _assert_same_output(split, unsplit)
  assert any(
      "failed to import, so the file is imported whole" in r.getMessage()
      for r in caplog.records)
