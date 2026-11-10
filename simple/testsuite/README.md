# Preprocessor test suite

Tests for the `simple/` preprocessor. Most of them check **what it outputs**:
the JSON-LD records, the `report.json` summary and the ingestion-workflow
handshake. These run the real `stats.runner.Runner` hermetically, with an
in-process GCS emulator standing in for the network, so nothing inside the
preprocessor is mocked. Unit tests for self-contained helper modules live
under `unit/`.

```bash
cd simple
python3 -m pytest testsuite/                               # everything, in parallel (-n auto is the default; -n 0 for serial)
python3 -m pytest testsuite/test_observations.py -k about  # one table
python3 -m pytest testsuite/test_e2e.py --modes=gcs_input # one execution mode
python3 -m pytest testsuite/unit/                          # unit tests only
python3 -m pytest testsuite/test_e2e.py --update-expected -n 0  # rewrite expected.json, then review the diff
```

## Three kinds of tests

| Files | What they check | Expected output |
|---|---|---|
| Small tests, one file per input type or shared concern (below) | One small input in, exact records (or an exact error) out | Written in the test, with `lib/builders.py` |
| `test_e2e.py` + `e2e/<dataset>/` | A realistic multi-file import, in every execution mode | `expected.json`, reviewed |
| `unit/`, mirroring the module path (e.g. `unit/util/test_filesystem.py` for `util/filesystem.py`) | One self-contained module (filesystem, file matching, validation, report generation) called directly | Asserted in the test |

Prefer an output-level test. Add a unit test only for a module that has
behavior of its own that is awkward to reach through the runner, such as
filesystem edge cases or a validator's rules.

| Small test file | Covers |
|---|---|
| `test_mcf_nodes.py` | MCF files: values, subjects and types, file structure, parse errors, limits |
| `test_observations.py` | Observation CSVs: observationAbout / variable / date / value cells, columns, observationProperties, dimensions, ids, header errors |
| `test_entities.py` | Entities CSVs: rows, ids, reference columns, values, column mappings, header errors |
| `test_csv_files.py` | CSV formatting (quoting, CRLF, BOM, column and row order, split files), for every CSV import type |
| `test_provenance.py` | Provenances and sources: registered nodes, provenance URL on the provenance node, validation |
| `test_config.py` | config.json: which files are imported, top-level flags, invalid configs |

For a refactor or performance PR, `test_e2e.py` passing with every
`expected.json` unchanged means the output did not change.

Execution modes (`lib/run.py`):

- `disk_input`: input on local disk. Used by every small test.
- `gcs_input`: input on (fake) GCS, as the ingestion workflow runs it.
- `tiny_chunks`: with chunk and shard sizes shrunk so that every boundary is
  crossed even by tiny inputs.

Modes patch the preprocessor in the test process, and worker processes see
those patches only if forked. Runs therefore fail unless the multiprocessing
start method is `fork` (the Linux default up to Python 3.13; not macOS).

## Writing a test

Inputs and expectations are built separately. Input builders write exactly
what the preprocessor reads; expectation builders write the JSON-LD a test
expects. Nothing computes one from the other.

```python
from testsuite.lib import builders as b

def test_basic_node():
  result = b.run_mcf(b.mcf_node("dcid:Thing1", name='"Thing One"'))
  b.assert_records(result.added, [b.record("Thing1", "Thing", name="Thing One")])
```

`result.added` holds only what the test's own input adds: records that the
shared test provenance and source produce on their own are left out. Use
`result.observations` (without the hash ids) and `result.nodes` to look at one
kind of record.

For one rule with many inputs, add a row to a table of `Case`s:

```python
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

VALUES = [
    Case("integer", given="42", expect=42),
    Case("quoted number stays a string", given='"00123"', expect="00123",
         bug='quoted values that look numeric become numbers'),
]

@cases(*VALUES)
def test_value(given, expect):
  result = b.run_mcf(b.mcf_node("dcid:T", prop=given))
  b.assert_records(result.added, [b.record("T", "Thing", prop=expect)])
```

| To build | Use |
|---|---|
| MCF input | `mcf_node(subject, typeOf=..., **props)`, `mcf(*blocks)`, `property_mcf(*names)` |
| CSV input | `csv_file(*rows, header=..., quote_all=, line_end=, bom=)`, `obs_row(...)` |
| Config | `obs_config(mappings, pattern=, **options)`, `entities_config(row_type, id_column, mappings=, **options)` (mappings is required), `same_name_mappings(*columns)`, `OBS_MAPPINGS`, `AUTHOR_MAPPINGS`, `without_column_mappings(config)`, `MCF_CONFIG` |
| Expected records | `record(dcid, type, **props)`, `ref(dcid)`, `obs(...)`, `entity(...)`, `property_node(name)` |
| Running | `run(files, config, mode=, shared_files=)`, `run_mcf(...)`, `run_csv(...)`, `run_dir(...)` |
| Checking | `assert_records(actual, expected)`, `assert_fails(result, type, message, handshake_error_types)` |

## Known bugs

Known bugs are strict xfails, never wrong expectations: `bug="..."` on a
`Case`, `@known_bug(reason=...)` on a test, or an entry in `KNOWN_ISSUES` in
`test_e2e.py`. When a fix lands, the test XPASSes and fails until the marker
is removed.

One exception: an e2e bug that every mode shares is written into
`expected.json` instead, because an xfail would stop checking that whole
aspect. Each one is listed in `GOLDEN_BUGS` in `test_e2e.py`; its fix updates
`expected.json`.
