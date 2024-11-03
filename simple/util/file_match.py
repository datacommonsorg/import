# Copyright 2024 Google LLC
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
import re

import fs.path as fspath
from util.filesystem import File


def match(f: File, pattern: str) -> bool:
  """Returns true if this file's name or path matches a given pattern.

    Pattern rules:
    - A leading double slash means the path must match from the root of the input dir.
    - Protocol (e.g. gs://) can optionally be included in the pattern and will also add the requirement of matching from the root.
    - A single wildcard (*) in the name or protocol portions of a pattern can be zero or more characters.
      - Double wildcards for name or protocol are invalid; use single wildcards instead.
    - A single wildcard (*) in the directory portion of a pattern can be any non-slash character.
    - A double wildcard (**) in the directory portion of a pattern can be any character including slashes, i.e. multiple dir levels.

    Since matching is ultimately done with regular expressions, it may be
    possible to do more complex matching by passing in patterns containing special
    characters, but this behavior is not officially supported and may break at any time.

    Examples:
    - Match any CSV file in any directory or subdirectory: "*.csv"
    - Match any file called foo.csv in any directory or subdirectory: "foo.csv"
    - Match any file in a particular directory of a GCS bucket and its subdirectories: "gs://bucket/dir/**/*"
    - Match any file in any directory called /bar/: "bar/*"
    - Match any file in the root directory: "//*"
    """
  original_pattern = pattern
  full_path = f.full_path()
  dir_path, name = fspath.split(f.path)
  match_from_beginning = False

  # Handle protocol prefix.
  if "://" in pattern:
    if "://" not in full_path:
      return False
    protocol_pattern, pattern = pattern.split("://", 1)
    protocol_regex = _regexify_for_name(protocol_pattern, original_pattern)
    if not _full_match(protocol_regex, full_path.split('://', 1)[0]):
      return False
    match_from_beginning = True

  # Handle leading double slash.
  if pattern.startswith("//"):
    # We'll capture the full match requirement with re.search vs re.match.
    pattern = pattern[2:]
    match_from_beginning = True

  # Strip leading single slash. Patterns with and without one should be equivalent.
  if pattern.startswith("/"):
    pattern = pattern[1:]

  # Handle directory if accounted for in the pattern.
  if "/" in pattern:
    # Split the pattern at the last slash only to get the name part.
    dir_pattern, name_pattern = pattern.rsplit("/", 1)
    # Add a leading slash back to both the dir path and the dir pattern.
    dir_pattern = "/" + dir_pattern
    dir_path = "/" + dir_path
    dir_regex = _regexify_for_dir(dir_pattern, original_pattern)
    name_regex = _regexify_for_name(name_pattern, original_pattern)
    if match_from_beginning:
      dir_regex = "^" + dir_regex
    dir_regex += "$"
    if re.search(dir_regex, dir_path) is None:
      return False
  else:
    name_regex = _regexify_for_name(pattern, original_pattern)
    if match_from_beginning and dir_path != "":
      return False

  # Handle file name.
  return _full_match(name_regex, name)


def _regexify_for_dir(pattern: str, original_pattern: str) -> str:
  if "**/**" in pattern:
    raise ValueError(
        f"Adjacent double wildcards in the directory portion of a pattern are not supported. Pattern: {original_pattern}"
    )
  # 1. Remove and split at double wildcard segments
  between_doubles = pattern.split("/**")
  regex_segments = []
  for segment in between_doubles:
    # 2. Remove and split at slashes
    between_slashes = segment.split("/")
    # 3. Regexify periods
    between_slashes = [s.replace(".", r"\.") for s in between_slashes]
    # 4. Regexify single wildcards:
    #    A single wildcard can be any character but a slash.
    between_slashes = [s.replace("*", r"[^\/]+") for s in between_slashes]
    # 5. Add back and regexify slashes:
    #    A slash must be a literal slash.
    #    (EXCEPTION: Slash after a double wildcard.)
    with_slashes = (r"\/").join(between_slashes)
    regex_segments.append(with_slashes)
  # 6. Add back and regexify double wildcards:
  #    A double wildcard can be any character.
  regex = ".*".join(regex_segments)
  return regex


def _regexify_for_name(pattern: str, original_pattern: str) -> str:
  if "**" in pattern:
    raise ValueError(
        f"Adjacent double wildcards in the name or protocol portion of a pattern are not supported. Pattern: {original_pattern}"
    )
  return pattern.replace(".", r"\.").replace("*", ".*")


def _full_match(regex: str, value: str) -> bool:
  return re.search(f"^{regex}$", value) is not None
