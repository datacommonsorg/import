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
"""Files on local disk or Google Cloud Storage (GCS), by path or gs:// URL.

open_dir(path) returns a Dir, whose open_dir(), open_file() and all_files()
return the dirs and files under it. A Dir or File is only a path: making one
doesn't access storage, and there is nothing to close.

GCS has no directories, only object names: nothing here writes the "dir/"
marker objects that some tools use as directories, and listing skips them.

Modules: files (Dir, File, open_dir, write_file), paths (path strings),
local (local disk) and gcs (GCS, its client and ranged reads).
"""

from util.filesystem.files import Dir
from util.filesystem.files import File
from util.filesystem.files import open_dir
from util.filesystem.files import write_file
from util.filesystem.gcs import gcs_client
from util.filesystem.gcs import GCS_RETRY
from util.filesystem.gcs import GcsObject
from util.filesystem.paths import is_gcs_path
from util.filesystem.paths import is_within
from util.filesystem.paths import join_path

__all__ = [
    "Dir",
    "File",
    "GCS_RETRY",
    "GcsObject",
    "gcs_client",
    "is_gcs_path",
    "is_within",
    "join_path",
    "open_dir",
    "write_file",
]
