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
"""The code trees the two sides run: the working tree or a git worktree."""

import contextlib
import dataclasses
import os
import subprocess
from typing import Iterator

_HERE = os.path.dirname(os.path.abspath(__file__))
_SHORT_SHA = 12


@dataclasses.dataclass(frozen=True)
class CodeTree:
  label: str  # The ref as given, or "working tree".
  commit: str
  path: str  # Repository root of the tree.
  dirty: bool  # Whether simple/ has uncommitted changes (working tree only).

  @property
  def simple_dir(self) -> str:
    return os.path.join(self.path, "simple")

  def describe(self) -> str:
    suffix = " + uncommitted changes" if self.dirty else ""
    return f"{self.label} @ {self.commit[:_SHORT_SHA]}{suffix}"

  def to_json(self) -> dict:
    return dataclasses.asdict(self)


def _git(repo: str, *args: str) -> str:
  return subprocess.run(["git", "-C", repo, *args],
                        check=True,
                        capture_output=True,
                        text=True).stdout.strip()


def repo_root() -> str:
  """Returns the root of the repository this tool is checked out in."""
  return _git(_HERE, "rev-parse", "--show-toplevel")


def working_tree(repo: str) -> CodeTree:
  dirty = bool(_git(repo, "status", "--porcelain", "--", "simple"))
  return CodeTree("working tree", _git(repo, "rev-parse", "HEAD"), repo, dirty)


@contextlib.contextmanager
def checkouts(repo: str, refs: list[str],
              parent_dir: str) -> Iterator[dict[str, CodeTree]]:
  """Checks out each ref into a temporary detached git worktree.

  Refs that resolve to the same commit share one worktree. Every worktree is
  removed on exit.

  Yields:
    The CodeTree of each ref, by ref.
  """
  commits = {ref: _git(repo, "rev-parse", f"{ref}^{{commit}}") for ref in refs}
  paths: dict[str, str] = {}
  try:
    for commit in sorted(set(commits.values())):
      path = os.path.join(parent_dir, f"tree-{commit[:_SHORT_SHA]}")
      _git(repo, "worktree", "add", "--detach", path, commit)
      paths[commit] = path
    yield {
        ref: CodeTree(ref, commit, paths[commit], dirty=False)
        for ref, commit in commits.items()
    }
  finally:
    for path in paths.values():
      _git(repo, "worktree", "remove", "--force", path)
