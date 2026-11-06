# Preprocessor regression check

Runs the preprocessor (`stats.main`) on the same dataset at two versions of
the code, a **baseline** git ref and a **candidate** (by default your working
tree), and compares:

- every output file: byte identity, plus a semantic diff of observations,
  nodes and reports;
- wall time, CPU time (user and sys) and peak memory (max RSS);
- the exit status and the number of ERROR and WARNING log lines.

It writes `report.md` and `report.json`, and exits non-zero when a run fails,
the output differs or performance regresses beyond the thresholds.

Run it before merging any preprocessor change that could affect its output
or performance. For a change that is meant to change the output, it shows
exactly what changed. The testsuite (`simple/testsuite/`) covers small,
crafted inputs; this tool covers real datasets of any size.

## Quick start

```bash
cd simple
python3 -m tools.regression --input_dir=/path/to/input
```

This compares your working tree, uncommitted changes included, with
`upstream/master` on `/path/to/input`. The last lines printed are the verdict
and the path of the report:

```
Verdict: IDENTICAL; perf regressions: none; exit code 0
Report: /tmp/preprocessor-regression-k2j3h4/report.md (and report.json)
```

Run it from the virtualenv you use for the preprocessor. Both sides run with
that interpreter (`--python` to pick another), so a dependency change between
the refs is not picked up.

## Examples

```bash
# Several imports of a local dataset. Arguments after "--" go to stats.main
# on both sides.
python3 -m tools.regression --input_dir=/data/un_input -- --imports=global,unodc,ohchr

# A dataset on GCS, staged to local disk once (read-only rsync).
python3 -m tools.regression --input_dir=gs://my-bucket/path/to/input \
    --work_dir=/mnt/disk/ab1

# Config-driven: no --input_dir. Paths in extra flags must be absolute or
# gs://, because each side runs from its own checkout.
python3 -m tools.regression -- --config_file=gs://my-bucket/path/config.json

# Two refs, such as a branch against master.
python3 -m tools.regression --input_dir=/data/in --baseline=upstream/master \
    --candidate=origin/my-branch

# Run the baseline once, keep its output, then iterate on the candidate.
python3 -m tools.regression --input_dir=/data/in --work_dir=/mnt/disk/ab1 --keep_outputs
python3 -m tools.regression --input_dir=/data/in --work_dir=/mnt/disk/ab2 \
    --baseline_output=/mnt/disk/ab1/baseline/run-1/output

# Timing: three runs per side, reporting medians.
python3 -m tools.regression --input_dir=/data/in --repeats=3
```

`--baseline_output` and `--candidate_output` accept any output directory. For
one written by this tool (`<work_dir>/<side>/run-1/output`), the metrics are
reused from the `run.json` files next to it; for any other, perf is not judged
for that side. Passing both compares two outputs without running anything.
Report paths are compared relative to each side's input root, so the outputs
may come from different copies of the input.

Run `python3 -m tools.regression --help` for every flag.

## How it works

1. A gs:// input is staged to `<work_dir>/input` (see
   [GCS inputs](#gcs-inputs)).
2. Each ref is checked out into a temporary detached `git worktree`, removed
   at the end even on failure or Ctrl-C. Refs that resolve to the same commit
   share one. The working tree candidate runs in place.
3. Each side runs `python -m stats.main --input_dir=... --output_dir=...
   <extra flags>` from its own `simple/`, in the default multiprocess
   (production) mode, without `PYTHONPATH`, `WORKFLOW_EXECUTION_ID` and
   `TEMP_LOCATION`, and with `TMPDIR` in the run directory. With `--repeats`,
   the sides alternate (B1, A1, B2, A2, ...) to spread machine drift.
4. The outputs of run 1 are compared; those of later repeats are deleted
   right away.
5. Run outputs and the staged input are deleted unless you pass
   `--keep_outputs`. Logs, `run.json` files and reports are kept:

```
<work_dir>/report.md, report.json
<work_dir>/<baseline|candidate>/run-<n>/{preprocessor.log, run.json, output/}
```

The code is split into three packages under `tools/regression/`:
`execution/` (code trees, input, measured runs), `comparison/` (the output
diff and its categories, and the perf and run health decisions in
`metrics.py`) and `reporting/` (verdict, `report.md`, `report.json`).
`cli.py` parses the flags and ties them together.

## Reading the report

The report starts with the **verdict** (RUN FAILED, DIFFERENT or IDENTICAL,
the output comparison, the performance status and the exit code), then each
side's code, output directory and log, then:

- **Performance:** each metric's median per side, the delta, the threshold
  and whether it regressed, followed by run 1's exit status, timeout, and
  ERROR and WARNING line counts.
- **Differences:** every category with its count. **DIFF** marks a failing
  category with differences, *info* an informational one.
- **Output summary:** shard, observation and node counts per side, with `≠`
  on values that differ, then the differing observations per folder and node
  @ids per @type, and the most common node property changes and folder moves.
- **Samples:** the first `--samples` examples of each category with
  differences, as JSON. An observation sample shows every baseline and
  candidate record with that key, and its folder.

### How the comparison works

- **Observations** are matched by a semantic key, regardless of folder or
  shard: `variableMeasured` @id, `observationAbout`, `observationDate`,
  `provenance`, `unit`, `measurementMethod`, `observationPeriod`, plus any
  other property not listed here (for example a custom dimension such as
  `dcid:sex`). For each key it checks the value (`value`, `scalingFactor`),
  the other properties (`@id`, `@type`, the `variableMeasured` object), the
  `provenanceUrl`, the folder, and how many times the key is written.
- **Nodes** are matched by @id. All records of an @id, from any folder or
  shard, are merged into one set of values per property first, so splitting
  or duplicating a subject's records does not change its content.
- **Reports:** `process/report.json` and the per-file `report_*.json`,
  without volatile fields. Per-file reports are matched by the input file
  they describe.
- **Shards:** observation shards are matched by name. Node shards, whose
  names end in a random uid, are matched by content within their folder.

### Difference categories

Failing categories change what ingestion loads: the observations and their
values, a node's merged properties, the report, or the run's health.
Informational ones change only the layout of the same graph, or the warnings.
`--strict` makes informational categories fail too.

| Category | Kind | Meaning |
|---|---|---|
| `exit_status_changed` | failing | The exit status differs, or one side timed out. |
| `error_lines_increased` | failing | The candidate logged more ERROR/CRITICAL lines or Python tracebacks. |
| `warning_lines_changed` | informational | The number of WARNING lines differs. |
| `report_json_changed` | failing | A top-level field or an `importFiles` entry of `process/report.json` differs. |
| `worker_reports_changed` | failing | A per-file `report_*.json` entry differs or exists on one side only. |
| `run_dir_name_changed` | failing | The `jsonld/<import name>` run directory, without its timestamp, differs. |
| `jsonld_context_changed` | failing | The set of `@context` values in the shards differs. |
| `shard_set_changed` | informational | An observation shard name exists on one side only, or a folder has a different number of node shards. |
| `shard_bytes_changed` | informational | A same-name observation shard has different bytes, or a node shard has no byte-identical twin in its folder. Expected whenever anything in the shard changes. |
| `obs_only_baseline` | failing | An observation key exists only in the baseline (an observation was dropped). |
| `obs_only_candidate` | failing | An observation key exists only in the candidate (an observation was added). A changed key shows up as one of each. |
| `obs_multiplicity_changed` | failing | A key is written a different number of times (duplicates added or removed). |
| `obs_value_changed` | failing | Same key, different `value` or `scalingFactor`. |
| `obs_properties_changed` | failing | Same key and value, different `@id`, `@type` or `variableMeasured` object. |
| `obs_provenance_url_changed` | failing | Same key, `provenanceUrl` added, removed or changed. |
| `obs_folder_moved` | informational | Same key, written to a different import folder. |
| `node_only_baseline` | failing | A node @id exists only in the baseline. |
| `node_only_candidate` | failing | A node @id exists only in the candidate. |
| `node_properties_changed` | failing | A property of a node, after merging all its records, differs (counted once per @id; samples are per property). |
| `node_folder_moved` | informational | A node @id is written to a different set of folders. For example, global triples such as `dc/g/Root` can land in any folder on master. |
| `node_records_changed` | informational | A node @id is written as a different number of records, for example when a duplicate record is removed or a subject is split over shards. |

The comparison ignores only what is listed in
[comparison/volatile.py](comparison/volatile.py): report timestamps
(`startTime`, `lastUpdate`), the run directory's timestamp suffix, the random
uid in node shard names, and each side's input root in report paths.

## Thresholds and exit codes

| Flag | Default | Applies to |
|---|---|---|
| `--max_wall_regression_pct` | 10 | Wall time |
| `--max_cpu_regression_pct` | 10 | CPU time (user + sys) |
| `--max_rss_regression_pct` | 10 | Max RSS and peak process-tree RSS |
| `--perf_min_seconds` | 30 | Perf is not judged when the baseline's median wall time is shorter than this, or when either side failed or has no metrics. |

Thresholds compare the candidate's median with the baseline's. On noisy
machines, use `--repeats=3` or more.

| Exit code | Verdict | Meaning |
|---|---|---|
| 0 | IDENTICAL | Output identical and no perf regression |
| 5 | RUN FAILED | A run of either side exited non-zero or timed out |
| 3 | DIFFERENT | Output differs (failing categories, or any category with `--strict`) |
| 4 | IDENTICAL | Output identical, but perf regressed beyond a threshold |
| 2 | | Usage error |
| 1 | | The tool itself failed (e.g. a bad ref) |

RUN FAILED takes precedence over DIFFERENT and perf regressions: a failed
run's output is not a valid result, even when both sides fail the same way
and their (empty) outputs compare equal. Every repeat is checked, not just
run 1. The summary line and report name each failed run with its side, exit
code or timeout, and log; the output comparison is still reported for
context. A reused output without `run.json` has no exit status, so it never
counts as a failed run.

A preprocessor crash is a result, not a tool failure: it shows up as RUN
FAILED, plus `exit_status_changed` and the missing output when only one side
crashed.

CPU time and max RSS come from `wait4()`, like `/usr/bin/time -v`, and cover
the preprocessor and its pool workers. Max RSS is the largest single process.
Peak RSS (process tree) is the largest sum over all processes, sampled from
`/proc` every 0.5 s: an upper bound, since pages shared by forked workers are
counted once per process, that can miss short spikes.

## GCS inputs

| `--gcs_input` | What happens | Use when |
|---|---|---|
| `stage` (default) | `gcloud storage rsync` copies the input to local disk once, read-only on GCS, and both sides read the copy. | You want timings of the preprocessor, not the network. Needs disk, and the copy takes time up front. Pass `--stage_dir` to keep and reuse the copy: later runs only sync changes. |
| `direct` | Both sides read gs:// directly, as the ingestion workflow does. | You are changing the GCS read path, or the input is too large to copy. Timings include network variance, so use `--repeats`. |

Outputs are always written to local disk, so upload time is not measured.

## Resources

- **Disk:** each side's output is about the size of its JSON-LD (roughly 2-3
  times the CSV input). The comparison adds about 50 bytes per observation
  plus the size of the node shards, per side. The 19 UN imports (14 GB of
  input, 43.9M observations) write about 36 GB per side.
- **/tmp is often a RAM-backed tmpfs.** The default work directory is under
  `$TMPDIR`. For large datasets, pass `--work_dir` on a real disk, or the
  outputs will use RAM.
- **RAM:** the comparison spills digests to 256 hash buckets on disk and
  holds one bucket at a time; each worker parses one shard at a time. Lower
  `--compare_workers` if memory is tight.
- **Hangs:** `--timeout_minutes` kills a stuck run; it is reported as RUN
  FAILED (and `exit_status_changed` when only one side timed out).

## Caveat: `--freeze_time`

Do not pass `--freeze_time=True` (it is not passed by default). On
`upstream/master` it hangs the default multiprocess mode: the main process
spins while every pool worker waits on a futex, probably because freezegun's
frozen clock breaks the process pool's timeouts. The comparison already
ignores report and run directory timestamps, so the flag is not needed.

## Tests

```bash
cd simple
python3 -m pytest tools/
```

`tests/comparison/compare_test.py` plants each kind of difference in a small
output and checks that it is detected. `tests/comparison/metrics_test.py`
checks the perf thresholds, run health findings and failed runs.
`tests/e2e_test.py` runs the tool HEAD against HEAD on the `kitchen_sink`
testsuite dataset and expects IDENTICAL, and on an empty input, where both
sides fail, and expects RUN FAILED.
