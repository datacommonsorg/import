# Preprocessor test suite

Tests for the `simple/` preprocessor that check **what it outputs**: the
JSON-LD records, the `report.json` summary and the ingestion-workflow
handshake. Every test runs the real `stats.runner.Runner`, hermetically: a
local fake of the DC API and an in-process GCS emulator stand in for the
network, so nothing is mocked inside the preprocessor.

```bash
cd simple
python3 -m pytest testsuite/                          # everything (PR budget)
python3 -m pytest testsuite/test_cases.py -k mcf      # one case file
python3 -m pytest testsuite/test_e2e.py --modes=gcs   # one execution mode
python3 -m pytest testsuite/ --update-expected        # rewrite expectations, then review the diff
HYPOTHESIS_PROFILE=nightly python3 -m pytest testsuite/test_fuzz.py testsuite/test_metamorphic.py
python3 -m pytest testsuite/test_baseline.py --baseline=origin/master   # perf/refactor PRs
```

## Layers

| File | Question it answers | Expected output comes from |
|---|---|---|
| `test_cases.py` + `cases/*.md` | Does this exact input produce exactly these JSON-LD records? | A person, in the case file |
| `test_e2e.py` + `e2e/<dataset>/` | Do these input files produce this output, in every execution mode? | `expected.json`, reviewed |
| `test_fuzz.py` | Do random valid inputs produce the records the rules predict? | `lib/strategies.py` (the model) |
| `test_metamorphic.py` | Do shuffling, splitting, re-quoting, BOM/CRLF or another mode change the output? | The untransformed run |
| `test_baseline.py` | Does this checkout produce the same output as another git ref? | The baseline run |
| `test_limits.py` | Cases too big to write by hand | Inline asserts |

Execution modes (`lib/run.py`): `threaded`, `multiprocess`, `gcs` (GCS input
and output, as in production; the only mode that writes a success handshake),
`tiny_chunks` (every chunk/shard boundary crossed) and `rdflib_nodes`.

## Case files

Markdown. Blocks before the first `## ` heading are the file's defaults; each
`## ` heading is one test:

````markdown
## quoted string containing a comma stays one value

```mcf
Node: dcid:Thing5
typeOf: dcs:Thing
name: "Paris, France"
```

```expect
[{"@id": "dcid:Thing5", "@type": "dcid:Thing", "dcid:name": "Paris, France"}]
```
````

Blocks: `mcf` / `csv` (optional `file=path`), `config` (deep-merged over the
default; `null` deletes a key), `dc_api` (fake DC API graph), `expect` (every
record the case adds to the defaults' output; `"@id": "*"` matches any id),
`expect-error` (`type`, `contains`, `handshake_error_types`). Options:
`> xfail: <reason>` for a known bug (strict: the test fails once it is fixed)
and `> modes: all` or `> modes: multiprocess, gcs`.

See `lib/casefile.py` for the full format.

## Known bugs

Known bugs are strict xfails, never edited goldens: `> xfail:` in a case, or
an entry in `KNOWN_ISSUES` in `test_e2e.py`. When a fix lands, the test
XPASSes and fails until the marker is removed. Fuzz counterexamples become
cases (with `> xfail:` if the fix comes later).
