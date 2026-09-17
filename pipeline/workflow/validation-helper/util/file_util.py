# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Lightweight local and GCS file utilities for validation-helper."""

import csv
import fnmatch
import glob
import os
import shutil
from typing import List, Tuple, Union

from google.cloud import storage


def _split_gcs_path(gcs_path: str) -> Tuple[str, str]:
    """Splits gs://bucket/path/to/blob into ('bucket', 'path/to/blob')."""
    path = gcs_path[len('gs://'):]
    parts = path.split('/', 1)
    bucket_name = parts[0]
    blob_name = parts[1] if len(parts) > 1 else ''
    return bucket_name, blob_name


def file_is_local(filename: str) -> bool:
    """Returns True if the filename is a local path (not gs://)."""
    return bool(filename) and not filename.startswith('gs://')


def file_is_csv(filename: str) -> bool:
    """Returns True if the filename has a .csv extension."""
    return bool(filename) and filename.endswith('.csv')


class FileIO:
    """Context manager supporting both local files and GCS gs:// blobs."""

    def __init__(self,
                 filename: str,
                 mode: str = 'r',
                 encoding: str = 'utf-8',
                 errors: str = None,
                 **kwargs):
        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.errors = errors
        self._file = None

    def __enter__(self):
        open_kwargs = {}
        if 'b' not in self.mode:
            open_kwargs['encoding'] = self.encoding
            if self.errors:
                open_kwargs['errors'] = self.errors
        if self.filename.startswith('gs://'):
            bucket_name, blob_name = _split_gcs_path(self.filename)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            self._file = blob.open(self.mode, **open_kwargs)
        else:
            if 'w' in self.mode or 'a' in self.mode:
                os.makedirs(os.path.dirname(os.path.abspath(self.filename)),
                            exist_ok=True)
            self._file = open(self.filename, self.mode, **open_kwargs)
        return self._file

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()


def file_get_matching(pattern: Union[str, List[str]]) -> List[str]:
    """Returns matching files for a local path/glob or gs:// wildcard pattern."""
    if not pattern:
        return []
    if isinstance(pattern, list):
        results = []
        for p in pattern:
            results.extend(file_get_matching(p))
        return results
    if ',' in pattern:
        results = []
        for p in pattern.split(','):
            p_stripped = p.strip()
            if p_stripped:
                results.extend(file_get_matching(p_stripped))
        return results
    if pattern.startswith('gs://'):
        bucket_name, blob_pattern = _split_gcs_path(pattern)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        if '*' not in pattern and '?' not in pattern:
            blob = bucket.blob(blob_pattern)
            return [pattern] if blob.exists() else []
        prefix = blob_pattern.split('*', 1)[0]
        matches = []
        for blob in client.list_blobs(bucket, prefix=prefix):
            if fnmatch.fnmatch(blob.name, blob_pattern):
                matches.append(f'gs://{bucket_name}/{blob.name}')
        return sorted(matches)
    else:
        matched = sorted(glob.glob(pattern, recursive=True))
        if not matched and os.path.exists(pattern):
            return [pattern]
        return matched


def file_copy(src: str, dst: str) -> None:
    """Copies a file between local and/or GCS paths."""
    with FileIO(src, 'rb') as in_f, FileIO(dst, 'wb') as out_f:
        shutil.copyfileobj(in_f, out_f)


def file_load_csv_dict(filename: str,
                       key_column_name: str = None,
                       key_index: bool = False) -> dict:
    """Loads a CSV file into a dictionary of dictionaries."""
    result = {}
    with FileIO(filename, 'r') as f:
        reader = csv.DictReader(f)
        first_col = reader.fieldnames[0] if reader.fieldnames else None
        for idx, row in enumerate(reader):
            if key_index:
                key = idx
            elif key_column_name and key_column_name in row:
                key = row[key_column_name]
            elif first_col and first_col in row:
                key = row[first_col]
            else:
                key = idx
            result[key] = dict(row)
    return result


def file_write_csv_dict(data_dict: dict,
                        filename: str,
                        key_column_name: str = None) -> None:
    """Writes a dictionary of dictionaries to a CSV file."""
    if not data_dict:
        return
    fieldnames = []
    for row in data_dict.values():
        if isinstance(row, dict):
            for k in row.keys():
                if k not in fieldnames:
                    fieldnames.append(k)
    with FileIO(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data_dict.values():
            if isinstance(row, dict):
                writer.writerow(row)
