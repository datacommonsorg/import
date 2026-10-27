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
"""Parser and updater for node-level case files (cases/*.md).

A case file is Markdown. Fenced blocks before the first `## ` heading are the
file's defaults; every `## ` heading starts a case. Block types:

  ```mcf [file=NAME]```   MCF input (default name: case.mcf)
  ```csv [file=NAME]```   CSV input (default name: case.csv)
  ```config```            JSON, deep-merged over the default config
                          (dicts merge, null deletes a key, everything
                          else replaces)
  ```dc_api```            JSON knowledge graph for the fake DC API
  ```expect```            JSON list of the JSON-LD records the case's input
                          produces, i.e. every record of the run that is not
                          already produced by the defaults alone.
                          "@id": "*" matches any id (for hand-written
                          observation records, whose ids are hashes).
  ```expect-removed```    JSON list of records the defaults alone produce
                          but this case's run does not (e.g. a node the
                          case input changes). Omitted when empty.
  ```expect-error```      JSON {"type": ..., "contains": [...],
                          "handshake_error_types": [...]}

Shared records: an entry of an expect* list may be the string "$<name>",
standing for a record defined once in cases/shared.json or in the file's
```shared``` block (JSON {name: record}). --update-expected writes "$<name>"
for every record that equals a shared one, so repeated nodes (the variable
group roots, the provenance, ...) appear in full only once.

Lines starting with `> ` set options:
  > xfail: <reason>      the case documents a known bug; it must fail
  > modes: all | m1, m2   execution modes (default: threaded)
"""

import dataclasses
import json
import re
from typing import Any

_FENCE_RE = re.compile(r"^```([\w-]+)?([^\n]*)$")
_ATTR_RE = re.compile(r"(\w+)=(\S+)")


@dataclasses.dataclass
class Block:
  kind: str
  attrs: dict[str, str]
  text: str
  start: int  # Line index of the opening fence.
  end: int  # Line index of the closing fence.


@dataclasses.dataclass
class Case:
  name: str
  file: str
  line: int
  blocks: list[Block]
  options: dict[str, str]
  heading_line: int
  last_line: int  # Last line index belonging to the case.

  def blocks_of(self, kind: str) -> list[Block]:
    return [b for b in self.blocks if b.kind == kind]

  def block(self, kind: str) -> Block | None:
    found = self.blocks_of(kind)
    return found[0] if found else None

  @property
  def xfail(self) -> str | None:
    return self.options.get("xfail")


@dataclasses.dataclass
class CaseFile:
  path: str
  lines: list[str]
  defaults: list[Block]
  cases: list[Case]


def parse(path: str) -> CaseFile:
  with open(path) as f:
    lines = f.read().split("\n")
  defaults: list[Block] = []
  cases: list[Case] = []
  current: Case | None = None
  i = 0
  while i < len(lines):
    line = lines[i]
    if line.startswith("## "):
      if current:
        current.last_line = i - 1
      current = Case(name=line[3:].strip(),
                     file=path,
                     line=i + 1,
                     blocks=[],
                     options={},
                     heading_line=i,
                     last_line=i)
      cases.append(current)
    elif line.startswith("> ") and current and ":" in line:
      key, value = line[2:].split(":", 1)
      current.options[key.strip()] = value.strip()
    else:
      match = _FENCE_RE.match(line)
      if match and match.group(1):
        start = i
        i += 1
        body = []
        while i < len(lines) and lines[i].strip() != "```":
          body.append(lines[i])
          i += 1
        block = Block(kind=match.group(1),
                      attrs=dict(_ATTR_RE.findall(match.group(2) or "")),
                      text="\n".join(body) + ("\n" if body else ""),
                      start=start,
                      end=i)
        (current.blocks if current else defaults).append(block)
    i += 1
  if current:
    current.last_line = len(lines) - 1
  names = [c.name for c in cases]
  dupes = {n for n in names if names.count(n) > 1}
  if dupes:
    raise ValueError(f"{path}: duplicate case names {sorted(dupes)}")
  return CaseFile(path=path, lines=lines, defaults=defaults, cases=cases)


def deep_merge(base: Any, override: Any) -> Any:
  if isinstance(base, dict) and isinstance(override, dict):
    out = dict(base)
    for k, v in override.items():
      if v is None:
        out.pop(k, None)  # null deletes a default key.
      else:
        out[k] = deep_merge(base.get(k), v) if k in base else v
    return out
  return override


_ONE_LINE_MAX = 120


def format_records(records: list) -> str:
  """A JSON list with one record per line, when the record is short."""
  if not records:
    return "[]"
  lines = []
  for rec in records:
    one_line = json.dumps(rec, sort_keys=True, ensure_ascii=False)
    if len(one_line) <= _ONE_LINE_MAX:
      lines.append("  " + one_line)
    elif isinstance(rec, dict):
      # One key per line, values inline.
      items = [
          f"    {json.dumps(k)}: {json.dumps(v, sort_keys=True, ensure_ascii=False)}"
          for k, v in sorted(rec.items())
      ]
      lines.append("  {\n" + ",\n".join(items) + "\n  }")
    else:
      lines.append("  " + one_line)
  return "[\n" + ",\n".join(lines) + "\n]"


def write_updates(case_file: CaseFile,
                  updates: dict[str, list[tuple[str, str]]]) -> None:
  """Replaces a case's expect* blocks with new ones.

  updates: {case name: [(block kind, block body), ...]}.
  """
  lines = list(case_file.lines)
  # Apply bottom-up so earlier line indices stay valid.
  for case in sorted(case_file.cases, key=lambda c: -c.heading_line):
    if case.name not in updates:
      continue
    new_block = []
    for kind, body in updates[case.name]:
      new_block += ([""] if new_block else []) + [
          f"```{kind}"
      ] + body.rstrip("\n").split("\n") + ["```"]
    stale = [
        b for b in case.blocks
        if b.kind in ("expect", "expect-error", "expect-removed")
    ]
    for b in sorted(stale, key=lambda b: -b.start):
      del lines[b.start:b.end + 1]
    # Insert after the case's last non-blank line (adjusted for deletions).
    removed = sum(b.end - b.start + 1 for b in stale)
    insert_at = case.last_line - removed
    while insert_at > case.heading_line and not lines[insert_at].strip():
      insert_at -= 1
    lines[insert_at + 1:insert_at + 1] = [""] + new_block
  with open(case_file.path, "w") as f:
    f.write("\n".join(_collapse_blank_lines(lines)))


def _collapse_blank_lines(lines: list[str]) -> list[str]:
  """Keeps at most one blank line in a row outside fenced blocks."""
  out, in_fence = [], False
  for line in lines:
    if line.startswith("```"):
      in_fence = not in_fence if line.strip(
      ) == "```" or not in_fence else in_fence
    if not in_fence and not line.strip() and out and not out[-1].strip():
      continue
    out.append(line)
  return out


SHARED_FILE = "shared.json"


def load_shared(case_file: CaseFile) -> dict[str, dict]:
  """Named records: cases/shared.json, overridden by the file's ```shared```."""
  import os  # pylint: disable=import-outside-toplevel
  shared: dict[str, dict] = {}
  path = os.path.join(os.path.dirname(case_file.path), SHARED_FILE)
  if os.path.exists(path):
    with open(path) as f:
      shared.update(json.load(f))
  for block in case_file.defaults:
    if block.kind == "shared":
      shared.update(json.loads(block.text))
  return shared


def resolve_records(items: list, shared: dict[str, dict], where: str) -> list:
  out = []
  for item in items:
    if isinstance(item, str):
      if not item.startswith("$") or item[1:] not in shared:
        raise ValueError(f"{where}: unknown shared record {item!r}")
      out.append(shared[item[1:]])
    else:
      out.append(item)
  return out


def compact_records(records: list[dict], shared: dict[str, dict]) -> list:
  by_value = {
      json.dumps(rec, sort_keys=True, ensure_ascii=False): name
      for name, rec in shared.items()
  }
  out = []
  for rec in records:
    name = by_value.get(json.dumps(rec, sort_keys=True, ensure_ascii=False))
    out.append(f"${name}" if name else rec)
  # References first (sorted), then full records in their given order.
  refs = sorted(r for r in out if isinstance(r, str))
  return refs + [r for r in out if not isinstance(r, str)]
