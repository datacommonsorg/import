# PROTOTYPE (benchmark only): in-preprocessor node validation rules.
"""Vectorized PROPERTY_CHECK rules evaluated on each observation / triple batch.

One accumulator per worker process, reset per file; its picklable partial is
returned in ImportProcResult and merged by the coordinator.
"""

from dataclasses import dataclass
from dataclasses import field
import json
import logging
import os
import time

import numpy as np
import pandas as pd

K = 20
DCID_RE = r"[\w&/%)(+\-.:]+"
ISO_DATE_RE = (r"\d{4}(?:-(?:0[1-9]|1[0-2])(?:-(?:0[1-9]|[12]\d|3[01])"
               r"(?:T\d{2}:\d{2}(?::\d{2})?)?)?)?")
ALLOWED_STAT_TYPES = frozenset({
    "measuredValue", "meanValue", "medianValue", "minValue", "maxValue",
    "sumValue", "varianceValue", "stdDeviationValue", "marginOfError",
    "stdErr", "sampleSizeValue", "growthRate", "measurementResult",
    "cumulativeValue", "percentileValue"
})


def _strip_ns(v: str) -> str:
  return v.split(":", 1)[1] if v.startswith(("dcid:", "dcs:", "schema:")) else v


@dataclass
class PropertyCheck:
  """One generic rule: `column` must satisfy `check` (optionally `when`)."""
  rule_id: str
  column: str
  check: str  # required | in | regex | number | iso_date | dcid | max_length
  arg: object = None
  when: list = field(default_factory=list)  # [(column, value)] equality

  def bad_mask(self, s: pd.Series) -> tuple[np.ndarray, int]:
    """Returns (violation mask, number of values checked)."""
    if self.check == "required":
      bad = (s.isna() | s.eq("")).to_numpy()
      return bad, len(s)
    present = (s.notna() & s.ne("")).to_numpy()
    checked = int(present.sum())
    if checked == 0:
      return np.zeros(len(s), dtype=bool), 0
    if self.check == "number":
      bad = pd.to_numeric(s, errors="coerce").isna().to_numpy() & present
      return bad, checked
    # String checks run on distinct values first: entity/variable/date
    # columns repeat a few values per batch, and clean data has no bad ones.
    uniq = pd.unique(s[present])
    u = pd.Series(uniq, dtype=object)
    if self.check in ("regex", "iso_date", "dcid"):
      pat = {
          "iso_date": ISO_DATE_RE,
          "dcid": DCID_RE
      }.get(self.check, self.arg)
      ubad = ~u.str.fullmatch(pat).fillna(False).astype(bool)
    elif self.check == "max_length":
      ubad = u.str.len() > self.arg
    elif self.check == "in":
      ubad = ~u.isin(self.arg)
    else:
      raise ValueError(self.check)
    if not ubad.any():
      return np.zeros(len(s), dtype=bool), checked
    bad = s.isin(set(u[ubad])).to_numpy() & present
    return bad, checked


OBS_RULES = [
    PropertyCheck("dc.obs.entity_required", "entity", "required"),
    PropertyCheck("dc.obs.variable_required", "variable", "required"),
    PropertyCheck("dc.obs.date_required", "date", "required"),
    PropertyCheck("dc.obs.date_format", "date", "iso_date"),
    PropertyCheck("dc.obs.value_required", "value", "required"),
    PropertyCheck("dc.obs.value_numeric", "value", "number"),
    PropertyCheck("dc.dcid.format.entity", "entity", "dcid"),
    PropertyCheck("dc.dcid.format.variable", "variable", "dcid"),
    PropertyCheck("dc.dcid.max_length.entity", "entity", "max_length", 256),
    PropertyCheck("dc.dcid.max_length.variable", "variable", "max_length", 256),
]
OBS_ABOUT = "dcid:observationAbout"
REF_COLUMNS = ["variable", "unit", "measurement_method"]
TRIPLE_RULES = ["dc.node.typeof_required", "dc.sv.stat_type_valid"]


def hash_distinct(values) -> np.ndarray:
  """64-bit hashes of the distinct non-empty strings in `values`."""
  u = pd.unique(np.asarray(values, dtype=object))
  u = u[pd.notna(u)]
  u = u[u != ""]
  if len(u) == 0:
    return np.empty(0, dtype=np.uint64)
  return pd.util.hash_array(u.astype(object), categorize=False)


class RuleAccumulator:

  def __init__(self):
    self.stats = {
        r: {
            "checked": 0,
            "violations": 0,
            "samples": []
        } for r in [x.rule_id for x in OBS_RULES] + TRIPLE_RULES
    }
    self.refs: list[np.ndarray] = []
    self.defined: list[np.ndarray] = []
    self.typed: list[np.ndarray] = []
    self.untyped: dict[int, str] = {}  # hash -> subject (candidates)
    self.svs: list[np.ndarray] = []
    self.bad_stat: dict[int, tuple[str, str, str]] = {}  # hash->(subj,val,file)
    self.stat_checked_subjects: list[np.ndarray] = []
    self.seconds = 0.0

  def _add(self, rule_id, bad, checked, samples_fn):
    st = self.stats[rule_id]
    st["checked"] += checked
    n = int(bad.sum())
    if n:
      st["violations"] += n
      need = K - len(st["samples"])
      if need > 0:
        st["samples"].extend(samples_fn(np.flatnonzero(bad)[:need]))

  # ---- observations -------------------------------------------------------
  def check_observations(self, df: pd.DataFrame, file: str,
                         entity_cols: list[str]) -> pd.DataFrame:
    """Before filter_invalid_observation_values. Returns df unchanged.

    At this point observationAbout and other entity dimensions are still
    raw columns named by their property (e.g. "dcid:observationAbout"); the
    `entity` column is only added later, empty.
    """
    t0 = time.perf_counter()
    n = len(df)
    for rule in OBS_RULES:
      col = OBS_ABOUT if rule.column == "entity" else rule.column
      s = df[col] if col in df.columns else pd.Series(
          [None] * n, index=df.index, dtype=object)
      bad, checked = rule.bad_mask(s)
      self._add(
          rule.rule_id, bad, checked, lambda idx, s=s: [{
              "file": file,
              "row": int(df.index[i]) + 2,
              "value": None if pd.isna(s.iloc[i]) else str(s.iloc[i])
          } for i in idx])
    # Entity dimension references (namespaces stripped on distinct values).
    for col in entity_cols:
      if col in df.columns:
        u = pd.unique(df[col].to_numpy())
        u = [_strip_ns(v) for v in u if isinstance(v, str) and v]
        if u:
          self.refs.append(hash_distinct(u))
    self.seconds += time.perf_counter() - t0
    return df

  def collect_obs_refs(self, df: pd.DataFrame) -> None:
    """After namespaces are stripped, on the rows that are written."""
    t0 = time.perf_counter()
    for col in REF_COLUMNS:
      if col in df.columns:
        self.refs.append(hash_distinct(df[col].to_numpy()))
    self.seconds += time.perf_counter() - t0

  # ---- triples ------------------------------------------------------------
  def check_triples(self, triples, file: str) -> None:
    t0 = time.perf_counter()
    if not triples:
      return
    subj = np.array([t.subject_id for t in triples], dtype=object)
    pred = np.array([t.predicate for t in triples], dtype=object)
    oid = np.array([t.object_id for t in triples], dtype=object)
    oval = np.array([t.object_value for t in triples], dtype=object)

    codes, usubj = pd.factorize(subj)
    uh = pd.util.hash_array(usubj.astype(object), categorize=False)
    self.defined.append(uh)
    self.refs.append(hash_distinct(oid))

    is_type = pred == "typeOf"
    typed = np.zeros(len(usubj), dtype=bool)
    typed[codes[is_type]] = True
    self.typed.append(uh[typed])
    for i in np.flatnonzero(~typed):
      self.untyped.setdefault(int(uh[i]), (str(usubj[i]), file))

    obj = np.where(oid != "", oid, oval)
    is_sv = np.zeros(len(usubj), dtype=bool)
    sv_rows = is_type & (pd.Series(obj).map(_strip_ns).to_numpy()
                         == "StatisticalVariable")
    is_sv[codes[sv_rows]] = True
    self.svs.append(uh[is_sv])
    st_rows = np.flatnonzero(pred == "statType")
    if len(st_rows):
      vals = pd.Series(obj[st_rows]).map(_strip_ns)
      self.stat_checked_subjects.append(uh[codes[st_rows]])
      for j in np.flatnonzero(~vals.isin(ALLOWED_STAT_TYPES).to_numpy()):
        r = st_rows[j]
        self.bad_stat.setdefault(int(uh[codes[r]]),
                                 (str(subj[r]), str(vals.iloc[j]), file))
    self.seconds += time.perf_counter() - t0

  # ---- partial / merge ----------------------------------------------------
  @staticmethod
  def _u(arrs):
    return np.unique(np.concatenate(arrs)) if arrs else np.empty(
        0, dtype=np.uint64)

  def partial(self) -> dict:
    return {
        "stats": self.stats,
        "refs": self._u(self.refs),
        "defined": self._u(self.defined),
        "typed": self._u(self.typed),
        "untyped": self.untyped,
        "svs": self._u(self.svs),
        "bad_stat": self.bad_stat,
        "stat_checked": self._u(self.stat_checked_subjects),
        "seconds": self.seconds,
    }

  def merge(self, p: dict) -> None:
    for r, st in p["stats"].items():
      m = self.stats[r]
      m["checked"] += st["checked"]
      m["violations"] += st["violations"]
      m["samples"] = (m["samples"] + st["samples"])[:K]
    for k, dst in (("refs", self.refs), ("defined", self.defined),
                   ("typed", self.typed), ("svs", self.svs),
                   ("stat_checked", self.stat_checked_subjects)):
      dst.append(p[k])
    for h, v in p["untyped"].items():
      self.untyped.setdefault(h, v)
    for h, v in p["bad_stat"].items():
      self.bad_stat.setdefault(h, v)
    self.seconds += p["seconds"]

  def finalize(self) -> dict:
    t0 = time.perf_counter()
    defined = self._u(self.defined)
    typed = self._u(self.typed)
    svs = self._u(self.svs)
    refs = self._u(self.refs)
    st = self.stats["dc.node.typeof_required"]
    st["checked"] = len(defined)
    keys = np.fromiter(self.untyped.keys(), dtype=np.uint64,
                       count=len(self.untyped))
    viol = [(int(h), self.untyped[int(h)])
            for h in keys[~np.isin(keys, typed)]]
    st["violations"] = len(viol)
    st["samples"] = [{"file": f, "subject": s} for _, (s, f) in viol[:K]]
    st = self.stats["dc.sv.stat_type_valid"]
    checked = self._u(self.stat_checked_subjects)
    st["checked"] = int(np.isin(checked, svs).sum())
    keys = np.fromiter(self.bad_stat.keys(), dtype=np.uint64,
                       count=len(self.bad_stat))
    bad = [(int(h), self.bad_stat[int(h)]) for h in keys[np.isin(keys, svs)]]
    st["violations"] = len(bad)
    st["samples"] = [{
        "file": f,
        "subject": s,
        "value": val
    } for _, (s, val, f) in bad[:K]]
    unresolved = np.setdiff1d(refs, defined, assume_unique=True)
    self.seconds += time.perf_counter() - t0
    return {
        "rules": self.stats,
        "references": {
            "referenced_distinct": int(len(refs)),
            "defined_distinct": int(len(defined)),
            "referenced_not_defined": int(len(unresolved)),
            "mixer_v2_node_calls_at_1000": int(-(-len(unresolved) // 1000)),
        },
        "rule_seconds_total_all_processes": round(self.seconds, 3),
    }


_current: RuleAccumulator | None = None


def current() -> RuleAccumulator:
  global _current
  if _current is None:
    _current = RuleAccumulator()
  return _current


def take_partial() -> dict:
  global _current
  p = current().partial()
  _current = None
  return p


def report(acc: RuleAccumulator) -> None:
  out = acc.finalize()
  logging.info("PP_RULES %s", json.dumps(out["references"]))
  path = os.environ.get("PP_RULES_REPORT")
  if path:
    with open(path, "w") as f:
      json.dump(out, f, indent=1)
