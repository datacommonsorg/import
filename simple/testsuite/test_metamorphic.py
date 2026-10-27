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
"""Metamorphic tests: input changes that must not change the output.

No expected output is needed: for a generated input, each transform below (or
a different execution mode) must produce exactly the same JSON-LD records as
the untransformed input in threaded mode.
"""

import csv
import io
import random

import pytest
from hypothesis import given, note, settings
from hypothesis import strategies as st

from testsuite.lib import canonical, run
from testsuite.lib import strategies as gen


def _rows(text: str) -> tuple[list[str], list[list[str]]]:
    rows = list(csv.reader(io.StringIO(text, newline="")))
    return rows[0], rows[1:]


def _write(header, rows, quoting=csv.QUOTE_MINIMAL, line_end="\n") -> str:
    out = io.StringIO()
    writer = csv.writer(out, quoting=quoting, lineterminator=line_end)
    writer.writerow(header)
    writer.writerows(rows)
    return out.getvalue()


def _shuffle_rows(text, rng):
    header, rows = _rows(text)
    rng.shuffle(rows)
    return {"data.csv": _write(header, rows)}


def _split_into_two_files(text, rng):
    header, rows = _rows(text)
    cut = rng.randint(0, len(rows))
    return {
        "data.csv": _write(header, rows[:cut]),
        "data_part2.csv": _write(header, rows[cut:]),
    }


def _quote_every_field(text, rng):  # pylint: disable=unused-argument
    header, rows = _rows(text)
    return {"data.csv": _write(header, rows, quoting=csv.QUOTE_ALL)}


def _crlf_line_endings(text, rng):  # pylint: disable=unused-argument
    header, rows = _rows(text)
    return {"data.csv": _write(header, rows, line_end="\r\n")}


def _byte_order_mark(text, rng):  # pylint: disable=unused-argument
    header, rows = _rows(text)
    return {"data.csv": "\ufeff" + _write(header, rows)}


def _add_ignored_column(text, rng):
    header, rows = _rows(text)
    pos = rng.randint(0, len(header))
    header = header[:pos] + ["zz_extra"] + header[pos:]
    rows = [r[:pos] + ['x, "y"\nz'] + r[pos:] for r in rows]
    return {"data.csv": _write(header, rows)}


# Each example runs the preprocessor several times, so these tests use a
# fraction of the profile's example budget (ci: 25, nightly: 1000).
_BUDGET = settings().max_examples

TRANSFORMS = {
    f.__name__.lstrip("_"): f
    for f in [
        _shuffle_rows, _split_into_two_files, _quote_every_field,
        _crlf_line_endings, _byte_order_mark, _add_ignored_column
    ]
}


def _config(obs: gen.ObservationCsv, extra_ignored: bool) -> dict:
    return {
        "inputFiles": [
            {
                "pattern": "*.mcf",
                "provenance": "dcid:TestProvenance"
            },
            {
                "pattern":
                "data*.csv",
                "provenance":
                "dcid:TestProvenance",
                "columnMappings":
                obs.column_mappings,
                "ignoreColumns":
                obs.ignore_columns + (["zz_extra"] if extra_ignored else []),
            },
        ]
    }


def _observations(input_files: dict, config: dict, mode: str = "threaded"):
    result = run.run_preprocessor(
        run.write_input_dir(
            {
                "provenance.mcf": gen.PROVENANCE_MCF,
                **input_files
            }, config), mode)
    assert result.output["error"] is None, result.output.get("_traceback")
    return {
        folder: kinds[canonical.OBSERVATIONS]
        for folder, kinds in result.output["folders"].items()
    }


@pytest.mark.parametrize("transform", sorted(TRANSFORMS))
@settings(max_examples=max(5, _BUDGET // 4))
@given(obs=gen.observation_csv(), seed=st.integers(0, 2**32))
def test_csv_transform_keeps_observations(transform, obs, seed):
    note(f"CSV input:\n{obs.text!r}")
    base = _observations({"data.csv": obs.text}, _config(obs, False))
    changed = TRANSFORMS[transform](obs.text, random.Random(seed))
    note(f"transformed: {changed!r}")
    actual = _observations(changed,
                           _config(obs, transform == "add_ignored_column"))
    assert actual == base, (f"{transform} changed the observations:\n" +
                            canonical.diff(base, actual))


@pytest.mark.parametrize("mode", ["multiprocess", "gcs", "tiny_chunks"])
@settings(max_examples=max(3, _BUDGET // 8))
@given(obs=gen.observation_csv())
def test_mode_keeps_observations(mode, obs):
    note(f"CSV input:\n{obs.text!r}")
    config = _config(obs, False)
    base = _observations({"data.csv": obs.text}, config)
    actual = _observations({"data.csv": obs.text}, config, mode)
    assert actual == base, (f"{mode} changed the observations:\n" +
                            canonical.diff(base, actual))
