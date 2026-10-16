# Preprocessor scenario suite

Data-integrity tests for the `simple/` preprocessor. Each scenario is a folder
of inputs plus expectations, run through the real `Runner` in every execution
mode and checked against **what the Data Commons loader would store**. The
design is described in `preprocessor_test_strategy.md` at the repo root.

```bash
cd simple
python3 -m pytest tests/stats/scenarios_test.py          # all scenarios x modes
python3 -m pytest tests/stats/scenario_harness_test.py   # harness self-tests
TEST_MODE=write python3 -m pytest tests/stats/scenarios_test.py -k threaded
                                                         # regenerate goldens
```

## What every run checks

| Layer | Source | Catches |
|---|---|---|
| Golden | `expected/facts.tsv` (generated in `threaded` mode) | Any change in what the loader stores |
| Hand-written expectations | `must_contain`, `must_not_contain`, `must_contain_observation` in `scenario.toml` | A wrong golden being blessed |
| Invariants | `scenario_lib/invariants.py` | Wrongness that a golden would bless (nulls, missing entity, duplicate records, split nodes, ...) |
| Row oracle | `scenario_lib/oracle.py` (`oracle = "vpr"`) | Lost, duplicated or altered rows, derived from the CSV + config alone |
| Error contract | `[error]` table | Failing with the wrong error, the wrong handshake, or after writing output |

Facts are produced by `scenario_lib/facts.py`, a port of the Java
`JsonLdParser` plus the namespace stripping in `GraphReader`. One line is one
stored fact: `folder  subject  predicate  kind  object`. Literals keep their
JSON type (`100` and `"100"` are different stored values).

## Execution modes

Every scenario runs in every mode, and every mode must match the same golden:

| Mode | What changes |
|---|---|
| `threaded` | `use_multiprocessing=False`; goldens are generated here |
| `tiny_chunks` | Shard and CSV chunk sizes shrunk to 1-2 so boundaries are crossed |
| `multiprocess` | `use_multiprocessing=True`, the production path (DC API stub reaches spawned workers through `DC_API_ROOT`) |
| `rdflib_nodes` | `FAST_NODE_EXPORT=false` (legacy node writer) |

DC API calls go to a local HTTP stub (`scenario_lib/dc_api_stub.py`) fed by
the scenario's `[dc_api]` table, so the real `util/dc_client.py` code runs.

## Adding a scenario

1. Create `scenarios/<area>_<feature>/` with `input/` (`config.json`, CSVs,
   `provenance.mcf`) and `scenario.toml`. Areas: `obs_ ent_ evt_ mcf_ cfg_
   csv_ val_ id_ err_ storage_ combo_`.
2. Write `description`, `features`, and at least one hand-written expectation.
   Unknown keys are errors.
3. Add any new feature to `features.toml`. The meta-tests require every
   registered feature to have a scenario.
4. Generate the golden with `TEST_MODE=write`, then **read it**: every line
   must be explainable from the input.

The shared `provenance.mcf` gives the provenance no `url`, which keeps E7 (see
below) isolated to `obs_provenance_url_from_mcf`.

## Known issues

Bugs that exist today are recorded, not hidden. In `scenario.toml`:

```toml
[[known_issues]]
id = "E7"                  # must exist in known_issues.toml
check = "provenance_url"   # the one check this waives
modes = ["multiprocess"]   # default: all modes
```

Waivers are strict. If the check passes, the test fails with "no longer
reproduces", so a fix forces removing the waiver. A waiver never hides other
checks. `known_issues.toml` lists each issue with its loader impact.

## Golden review rules

- A perf-only change must produce **zero** golden diffs in all modes.
- Every golden diff must be explained in the PR description.
