# Copyright 2026 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""In-preprocessor per-node and per-observation validation rules.

Declarative `PropertyCheck` rules evaluated on every observation and triple
batch as importers stream data (no second pass). Designed for low overhead at
billion-observation scale by grouping rules per column, using cross-chunk
validated-value caches for low/medium-cardinality string columns, and using a
NumPy C fast path for finite-number validation.

One `RuleAccumulator` lives in each worker process, reset per task; its
picklable partial is returned in `ImportProcResult.rules_partial` and merged
by the coordinator.
"""

from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
import json
import logging
import math
import os
import re
import time

import numpy as np
import pandas as pd

K = 20
# Max distinct strings cached per check kind in one worker task.
# Large enough to hold census-tract-scale places (~72-85k) without thrashing.
_MAX_CACHE_SIZE = 1 << 18  # 262,144

DCID_MAX_LENGTH = 256
# Valid DCID body (after stripping an optional namespace prefix): non-empty,
# no whitespace or control characters, only valid DCID characters.
DCID_RE = re.compile(r"[\w&/%)(+\-.:]+")
ISO_DATE_RE = re.compile(
    r"\d{4}(?:-(?:0[1-9]|1[0-2])(?:-(?:0[1-9]|[12]\d|3[01])"
    r"(?:T(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?"
    r"(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)?)?)?)?")
ISO_DURATION_RE = re.compile(
    r"P(?=\d|T)(?:\d+Y)?(?:\d+M)?(?:\d+W)?(?:\d+D)?"
    r"(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?")

ALLOWED_STAT_TYPES = frozenset({
    "measuredValue",
    "meanValue",
    "medianValue",
    "minValue",
    "maxValue",
    "sumValue",
    "varianceValue",
    "stdDeviationValue",
    "marginOfError",
    "stdErr",
    "sampleSizeValue",
    "growthRate",
    "measurementResult",
    "cumulativeValue",
    "percentileValue",
})

_NS_PREFIXES = ("dcid:", "dcs:", "schema:")
_EMPTY_BOOL = np.empty(0, dtype=bool)


def _strip_ns(v: str) -> str:
  return v.split(":", 1)[1] if v.startswith(_NS_PREFIXES) else v


def is_valid_dcid(raw: object) -> bool:
  """Returns True if `raw` is a non-empty valid DCID (after optional ns)."""
  if not isinstance(raw, str):
    return False
  body = _strip_ns(raw)
  return bool(body) and DCID_RE.fullmatch(body) is not None


def is_valid_finite_number(raw: object) -> bool:
  """Returns True if `raw` parses as a finite float (rejects NaN/Inf/bool)."""
  if isinstance(raw, bool):
    return False
  try:
    return math.isfinite(float(raw))
  except (ValueError, TypeError):
    return False


@dataclass(frozen=True)
class PropertyCheck:
  """One generic rule: `column` must satisfy `check` (optionally `when`).

  Supported `check` values:
    - "required": value must be non-null and non-empty.
    - "number": present values must parse as finite real numbers.
    - "dcid": present values must be valid DCIDs (no control chars/whitespace).
    - "max_length": present values (after optional ns strip) have len <= arg.
    - "iso_date": present values match ISO-8601 date/datetime.
    - "iso_duration": present values match ISO-8601 duration (P...).
    - "regex": present values fullmatch regex pattern `arg`.
    - "in": present values are in collection `arg`.
  """
  rule_id: str
  column: str
  check: str
  arg: object = None
  when: tuple = field(default_factory=tuple)  # ((column, value), ...)

  def value_predicate(self):
    """Returns a single-value predicate `f(val) -> bool` for string checks."""
    if self.check == "dcid":
      return is_valid_dcid
    if self.check == "max_length":
      limit = int(self.arg)
      return lambda v: isinstance(v, str) and len(_strip_ns(v)) <= limit
    if self.check == "iso_date":
      return lambda v: isinstance(v, str) and ISO_DATE_RE.fullmatch(
          v) is not None
    if self.check == "iso_duration":
      return lambda v: isinstance(v, str) and ISO_DURATION_RE.fullmatch(
          _strip_ns(v)) is not None
    if self.check == "regex":
      pat = re.compile(self.arg) if isinstance(self.arg, str) else self.arg
      return lambda v: isinstance(v, str) and pat.fullmatch(v) is not None
    if self.check == "in":
      allowed = self.arg
      return lambda v: v in allowed
    if self.check == "number":
      return is_valid_finite_number
    raise ValueError(f"Unsupported check type: {self.check}")


# Special logical column name meaning "all entity dimension columns of the CSV"
# (e.g. dcid:observationAbout and/or custom entity dimensions).
ENTITY_COLUMN = "entity"

OBS_RULES = (
    # (2) Required observation fields
    PropertyCheck("dc.obs.entity_required", ENTITY_COLUMN, "required"),
    PropertyCheck("dc.obs.variable_required", "variable", "required"),
    PropertyCheck("dc.obs.date_required", "date", "required"),
    PropertyCheck("dc.obs.value_required", "value", "required"),
    # (3) Parseable finite numeric values
    PropertyCheck("dc.obs.value_numeric", "value", "number"),
    # (1) Valid DCID format + max length
    PropertyCheck("dc.dcid.format.entity", ENTITY_COLUMN, "dcid"),
    PropertyCheck("dc.dcid.format.variable", "variable", "dcid"),
    PropertyCheck("dc.dcid.max_length.entity", ENTITY_COLUMN, "max_length",
                  DCID_MAX_LENGTH),
    PropertyCheck("dc.dcid.max_length.variable", "variable", "max_length",
                  DCID_MAX_LENGTH),
    # (4) Additional high-value observation validations
    PropertyCheck("dc.obs.date_format", "date", "iso_date"),
    PropertyCheck("dc.obs.period_format", "observation_period", "iso_duration"),
    PropertyCheck("dc.obs.scaling_factor_numeric", "scaling_factor", "number"),
)

REF_COLUMNS = ("unit", "measurement_method")
TRIPLE_RULES = (
    "dc.node.typeof_required",
    "dc.sv.stat_type_valid",
    "dc.dcid.format.node",
    "dc.dcid.max_length.node",
)

# Pre-grouped rules by column for single-pass per-column evaluation.
_OBS_RULES_BY_COL: dict[str, list[PropertyCheck]] = defaultdict(list)
for _r in OBS_RULES:
  _OBS_RULES_BY_COL[_r.column].append(_r)

_RULE_PREDICATES = {
    r.rule_id: r.value_predicate() for r in OBS_RULES if r.check != "required"
}


def hash_distinct(values) -> np.ndarray:
  """64-bit hashes of the distinct non-empty strings in `values`."""
  if len(values) == 0:
    return np.empty(0, dtype=np.uint64)
  arr = values if isinstance(values, np.ndarray) else np.asarray(
      list(values), dtype=object)
  u = pd.unique(arr)
  u = u[pd.notna(u)]
  u = u[u != ""]
  if len(u) == 0:
    return np.empty(0, dtype=np.uint64)
  return pd.util.hash_array(u.astype(object), categorize=False)


def _missing_mask(s: pd.Series) -> tuple[np.ndarray | None, int]:
  """Returns (missing_bool_mask_or_None_if_all_present, missing_count).

  Fast path: if no NA values and `""` is not present, returns (None, 0).
  """
  has_na = bool(s.isna().any())
  if not has_na:
    eq_empty = s.eq("").to_numpy(dtype=bool, na_value=False)
    n_miss = int(eq_empty.sum())
    return (eq_empty, n_miss) if n_miss else (None, 0)
  miss = (s.isna() | s.eq("").fillna(False)).to_numpy(dtype=bool)
  return miss, int(miss.sum())


def _bad_finite_number_mask(s: pd.Series, missing: np.ndarray | None,
                            n: int) -> tuple[np.ndarray | None, int]:
  """Returns (bad_bool_mask_or_None, checked_count) for finite numbers."""
  arr = s.to_numpy()
  if missing is None:
    checked = n
    try:
      nums = arr.astype(np.float64)
      if np.isfinite(nums).all():
        return None, checked
      return ~np.isfinite(nums), checked
    except (ValueError, TypeError):
      pass
    bad = np.fromiter((not is_valid_finite_number(v) for v in arr),
                      dtype=bool,
                      count=n)
    return (bad if bad.any() else None), checked

  present_idx = np.flatnonzero(~missing)
  checked = len(present_idx)
  if checked == 0:
    return None, 0
  sub = arr[present_idx]
  try:
    nums = sub.astype(np.float64)
    if np.isfinite(nums).all():
      return None, checked
    sub_bad = ~np.isfinite(nums)
  except (ValueError, TypeError):
    sub_bad = np.fromiter((not is_valid_finite_number(v) for v in sub),
                          dtype=bool,
                          count=checked)
  if not sub_bad.any():
    return None, checked
  bad = np.zeros(n, dtype=bool)
  bad[present_idx[sub_bad]] = True
  return bad, checked


class RuleAccumulator:
  """Collects validation counts, samples, and DCID reference hashes."""

  def __init__(self):
    self.stats: dict[str, dict] = {
        r: {
            "checked": 0,
            "violations": 0,
            "samples": [],
        } for r in [x.rule_id for x in OBS_RULES] + list(TRIPLE_RULES)
    }
    self.refs: list[np.ndarray] = []
    self.defined: list[np.ndarray] = []
    self.typed: list[np.ndarray] = []
    self.untyped: dict[int, tuple[str, str]] = {}  # hash -> (subject, file)
    self.svs: list[np.ndarray] = []
    self.bad_stat: dict[int, tuple[str, str, str]] = {}  # hash->(subj,val,file)
    self.stat_checked_subjects: list[np.ndarray] = []
    self.seconds: float = 0.0

    # Per-worker cross-chunk caches (not serialized in partial()).
    self._valid_cache: dict[str, set] = {
        r.rule_id: set() for r in OBS_RULES if r.check != "required"
    }
    self._seen_refs: set[str] = set()
    self._checked_node_dcids: set[str] = set()

  def _add_mask(self, rule_id: str, bad: np.ndarray | None, checked: int,
                samples_fn) -> None:
    st = self.stats[rule_id]
    st["checked"] += checked
    if bad is None:
      return
    n_bad = int(bad.sum())
    if n_bad:
      st["violations"] += n_bad
      need = K - len(st["samples"])
      if need > 0:
        st["samples"].extend(samples_fn(np.flatnonzero(bad)[:need]))

  def _record_refs_from_strings(self, values: set) -> None:
    """Adds namespace-stripped distinct strings to the ref cache."""
    seen = self._seen_refs
    for v in values:
      if isinstance(v, str) and v:
        ns = _strip_ns(v)
        if ns and ns not in seen:
          if len(seen) >= _MAX_CACHE_SIZE:
            self.refs.append(hash_distinct(seen))
            seen.clear()
          seen.add(ns)

  def _check_distinct_rules(self, rules: list[PropertyCheck], s: pd.Series,
                            present_uniq: set, missing: np.ndarray | None,
                            checked: int, file: str, row_offset: int,
                            part_index: int | None) -> None:
    """Evaluates distinct-value rules on `s` using cross-chunk caches."""
    for rule in rules:
      if checked == 0:
        continue
      cache = self._valid_cache[rule.rule_id]
      unseen = present_uniq - cache
      if not unseen:
        self.stats[rule.rule_id]["checked"] += checked
        continue
      pred = _RULE_PREDICATES[rule.rule_id]
      bad_vals = set()
      for v in unseen:
        if pred(v):
          if len(cache) >= _MAX_CACHE_SIZE:
            cache.clear()
          cache.add(v)
        else:
          bad_vals.add(v)
      if not bad_vals:
        self.stats[rule.rule_id]["checked"] += checked
        continue
      bad = s.isin(bad_vals).to_numpy(dtype=bool, na_value=False)
      if missing is not None:
        bad &= ~missing
      self._add_mask(
          rule.rule_id, bad, checked,
          lambda idx, s=s: self._make_obs_samples(file, s, idx, row_offset,
                                                  part_index))

  @staticmethod
  def _make_obs_samples(file: str, s: pd.Series | None, idx: np.ndarray,
                        row_offset: int, part_index: int | None) -> list[dict]:
    samples = []
    for i in idx:
      sample = {
          "file": file,
          "row": row_offset + int(i) + 2,
          "value": (None if s is None or pd.isna(s.iloc[i]) else str(s.iloc[i])
                   ),
      }
      if part_index is not None:
        sample["part"] = part_index
      samples.append(sample)
    return samples

  # ---- observations -------------------------------------------------------
  def check_observations(self,
                         df: pd.DataFrame,
                         file: str,
                         entity_cols: list[str],
                         row_offset: int = 0,
                         part_index: int | None = None) -> pd.DataFrame:
    """Validates an observation batch before filtering/serialization.

    Runs right after `_apply_property_defaults`, before custom dimensions are
    serialized and before `filter_invalid_observation_values`, so empty values
    and raw entity columns are visible. Returns `df` unchanged.
    """
    t0 = time.perf_counter()
    n = len(df)
    if n == 0:
      return df

    # 1. Entity dimension columns (supports dcid:observationAbout and custom
    #    entity dimensions). Every row must have at least one non-empty entity,
    #    and every non-empty entity value must satisfy entity DCID rules.
    present_entity_cols = [c for c in entity_cols if c in df.columns]
    entity_rules = _OBS_RULES_BY_COL[ENTITY_COLUMN]
    entity_value_rules = [r for r in entity_rules if r.check != "required"]
    if not present_entity_cols:
      all_bad = np.ones(n, dtype=bool)
      self._add_mask(
          "dc.obs.entity_required", all_bad, n,
          lambda idx: self._make_obs_samples(file, None, idx, row_offset,
                                             part_index))
    else:
      all_missing: np.ndarray | None = None
      any_col_complete = False
      for col in present_entity_cols:
        s = df[col]
        has_na = bool(s.isna().any())
        if not has_na:
          uniq = set(s.tolist())
          if "" not in uniq:
            any_col_complete = True
            self._check_distinct_rules(entity_value_rules, s, uniq, None, n,
                                       file, row_offset, part_index)
            self._record_refs_from_strings(uniq)
            continue
          miss = s.eq("").to_numpy(dtype=bool, na_value=False)
          checked = n - int(miss.sum())
          present_uniq = uniq - {""}
        else:
          miss = (s.isna() | s.eq("").fillna(False)).to_numpy(dtype=bool)
          checked = n - int(miss.sum())
          present_uniq = set(s[~miss].tolist()) if checked > 0 else set()

        if not any_col_complete:
          all_missing = miss if all_missing is None else (all_missing & miss)
        self._check_distinct_rules(entity_value_rules, s, present_uniq, miss,
                                   checked, file, row_offset, part_index)
        self._record_refs_from_strings(present_uniq)

      if any_col_complete or all_missing is None or not all_missing.any():
        self.stats["dc.obs.entity_required"]["checked"] += n
      else:
        first_s = df[present_entity_cols[0]]
        self._add_mask(
            "dc.obs.entity_required", all_missing, n,
            lambda idx, s=first_s: self._make_obs_samples(
                file, s, idx, row_offset, part_index))

    # 2. All other standard columns (variable, date, value, observation_period,
    #    scaling_factor).
    for col, rules in _OBS_RULES_BY_COL.items():
      if col == ENTITY_COLUMN:
        continue
      req_rules = [r for r in rules if r.check == "required"]
      num_rules = [r for r in rules if r.check == "number"]
      str_rules = [r for r in rules if r.check not in ("required", "number")]

      if col not in df.columns:
        for rule in req_rules:
          self._add_mask(
              rule.rule_id, np.ones(n, dtype=bool), n,
              lambda idx: self._make_obs_samples(file, None, idx, row_offset,
                                                 part_index))
        continue

      s = df[col]
      # High-cardinality numeric column (value): try single-pass C float64 +
      # isfinite first. If it succeeds and all values are finite, every row is
      # guaranteed both present (not None/NaN/pd.NA/"") and a finite number.
      if num_rules and not str_rules and col == "value":
        arr = s.to_numpy()
        fast_ok = False
        if arr.dtype != bool:
          try:
            nums = arr.astype(np.float64)
            if np.isfinite(nums).all():
              fast_ok = True
          except (ValueError, TypeError):
            pass
        if fast_ok:
          for rule in req_rules:
            self.stats[rule.rule_id]["checked"] += n
          for rule in num_rules:
            self.stats[rule.rule_id]["checked"] += n
          continue
        miss, _ = _missing_mask(s)
        for rule in req_rules:
          self._add_mask(
              rule.rule_id, miss, n,
              lambda idx, s=s: self._make_obs_samples(file, s, idx, row_offset,
                                                      part_index))
        for rule in num_rules:
          bad, checked = _bad_finite_number_mask(s, miss, n)
          self._add_mask(
              rule.rule_id, bad, checked,
              lambda idx, s=s: self._make_obs_samples(file, s, idx, row_offset,
                                                      part_index))
        continue

      # Low/medium-cardinality columns (variable, date, observation_period,
      # scaling_factor): extract distinct values once and reuse caches.
      has_na = bool(s.isna().any())
      if not has_na:
        uniq = set(s.tolist())
        if "" not in uniq:
          miss = None
          checked = n
          present_uniq = uniq
        elif len(uniq) == 1:
          # Entire column is "" (e.g. default empty observation_period /
          # scaling_factor): O(1) fast path!
          miss = np.ones(n, dtype=bool)
          checked = 0
          present_uniq = set()
        else:
          miss = s.eq("").to_numpy(dtype=bool, na_value=False)
          checked = n - int(miss.sum())
          present_uniq = uniq - {""}
      else:
        miss = (s.isna() | s.eq("").fillna(False)).to_numpy(dtype=bool)
        checked = n - int(miss.sum())
        present_uniq = set(s[~miss].tolist()) if checked > 0 else set()

      for rule in req_rules:
        self._add_mask(
            rule.rule_id, miss, n,
            lambda idx, s=s: self._make_obs_samples(file, s, idx, row_offset,
                                                    part_index))
      if str_rules:
        self._check_distinct_rules(str_rules, s, present_uniq, miss, checked,
                                   file, row_offset, part_index)
      if num_rules:
        self._check_distinct_rules(num_rules, s, present_uniq, miss, checked,
                                   file, row_offset, part_index)
      if col == "variable" and present_uniq:
        self._record_refs_from_strings(present_uniq)

    self.seconds += time.perf_counter() - t0
    return df

  def collect_obs_refs(self, df: pd.DataFrame) -> None:
    """Collects referenced DCIDs (variable, unit, measurement_method)."""
    t0 = time.perf_counter()
    for col in REF_COLUMNS:
      if col not in df.columns:
        continue
      s = df[col]
      if not s.isna().any():
        uniq = set(s.tolist())
        uniq.discard("")
        if uniq:
          self._record_refs_from_strings(uniq)
      else:
        present = s.dropna()
        if len(present):
          uniq = set(present.tolist())
          uniq.discard("")
          if uniq:
            self._record_refs_from_strings(uniq)
    self.seconds += time.perf_counter() - t0

  # ---- triples ------------------------------------------------------------
  def check_triples(self, triples, file: str) -> None:
    """Validates a batch of node triples (typeOf, statType, node DCID)."""
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

    # Validate node subject DCID format and max length (distinct per subject).
    fmt_st = self.stats["dc.dcid.format.node"]
    len_st = self.stats["dc.dcid.max_length.node"]
    checked_nodes = self._checked_node_dcids
    for s_val in usubj:
      s_str = str(s_val) if s_val is not None else ""
      if s_str in checked_nodes:
        continue
      if len(checked_nodes) < _MAX_CACHE_SIZE:
        checked_nodes.add(s_str)
      fmt_st["checked"] += 1
      len_st["checked"] += 1
      if not is_valid_dcid(s_str):
        fmt_st["violations"] += 1
        if len(fmt_st["samples"]) < K:
          fmt_st["samples"].append({"file": file, "subject": s_str})
      if len(_strip_ns(s_str)) > DCID_MAX_LENGTH:
        len_st["violations"] += 1
        if len(len_st["samples"]) < K:
          len_st["samples"].append({"file": file, "subject": s_str})

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
  def _u(arrs: list[np.ndarray]) -> np.ndarray:
    non_empty = [a for a in arrs if len(a)]
    return np.unique(np.concatenate(non_empty)) if non_empty else np.empty(
        0, dtype=np.uint64)

  def partial(self) -> dict:
    if self._seen_refs:
      self.refs.append(hash_distinct(self._seen_refs))
      self._seen_refs.clear()
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
      if len(p[k]):
        dst.append(p[k])
    for h, v in p["untyped"].items():
      self.untyped.setdefault(h, v)
    for h, v in p["bad_stat"].items():
      self.bad_stat.setdefault(h, v)
    self.seconds += p["seconds"]

  def finalize(self) -> dict:
    t0 = time.perf_counter()
    if self._seen_refs:
      self.refs.append(hash_distinct(self._seen_refs))
      self._seen_refs.clear()
    defined = self._u(self.defined)
    typed = self._u(self.typed)
    svs = self._u(self.svs)
    refs = self._u(self.refs)

    st = self.stats["dc.node.typeof_required"]
    st["checked"] = len(defined)
    keys = np.fromiter(self.untyped.keys(),
                       dtype=np.uint64,
                       count=len(self.untyped))
    viol = [(int(h), self.untyped[int(h)])
            for h in keys[~np.isin(keys, typed)]]
    st["violations"] = len(viol)
    st["samples"] = [{"file": f, "subject": s} for _, (s, f) in viol[:K]]

    st = self.stats["dc.sv.stat_type_valid"]
    checked = self._u(self.stat_checked_subjects)
    st["checked"] = int(np.isin(checked, svs).sum())
    keys = np.fromiter(self.bad_stat.keys(),
                       dtype=np.uint64,
                       count=len(self.bad_stat))
    bad = [(int(h), self.bad_stat[int(h)]) for h in keys[np.isin(keys, svs)]]
    st["violations"] = len(bad)
    st["samples"] = [{
        "file": f,
        "subject": s,
        "value": val,
    } for _, (s, val, f) in bad[:K]]

    unresolved = np.setdiff1d(refs, defined, assume_unique=True)
    total_violations = sum(s["violations"] for s in self.stats.values())
    self.seconds += time.perf_counter() - t0
    return {
        "rules": self.stats,
        "total_violations": total_violations,
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


def reset() -> None:
  """Discards the current accumulator so the next call starts fresh."""
  global _current
  _current = None


def take_partial() -> dict:
  global _current
  p = current().partial()
  _current = None
  return p


def report(acc: RuleAccumulator) -> dict:
  out = acc.finalize()
  summary = {
      "total_violations": out["total_violations"],
      "rule_seconds_total_all_processes": out[
          "rule_seconds_total_all_processes"],
      "references": out["references"],
  }
  logging.info("PP_RULES %s", json.dumps(summary))
  path = os.environ.get("PP_RULES_REPORT")
  if path:
    with open(path, "w") as f:
      json.dump(out, f, indent=1)
  return out
