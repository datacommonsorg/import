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

import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional


Row = Dict[str, Any]
Mapper = Callable[[Row], Optional[Row]]


@dataclass
class AggregationSpec:
    name: str
    source: Any
    sink: Any
    mapper: Optional[Mapper] = None


class SpannerSqlSource:

    def __init__(self,
                 database: Any,
                 sql: str,
                 params: Optional[Dict[str, Any]] = None,
                 param_types: Optional[Dict[str, Any]] = None) -> None:
        self.database = database
        self.sql = sql
        self.params = params or {}
        self.param_types = param_types or {}

    def read(self) -> Iterable[Row]:
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(self.sql,
                                           params=self.params,
                                           param_types=self.param_types)
            field_names = None
            for row in results:
                if isinstance(row, dict):
                    yield row
                    continue

                if field_names is None:
                    fields = getattr(results, "fields", None)
                    if fields:
                        field_names = [field.name for field in fields]

                if field_names:
                    yield dict(zip(field_names, row))
                else:
                    yield {"value": row}


class LocalJsonlSource:

    def __init__(self, path: str) -> None:
        self.path = path

    def read(self) -> Iterable[Row]:
        with open(self.path, 'r', encoding='utf-8') as jsonl_file:
            for line in jsonl_file:
                line = line.strip()
                if line:
                    yield json.loads(line)


class SpannerTableSink:

    def __init__(self,
                 database: Any,
                 table: str,
                 columns: List[str],
                 batch_size: int = 1000) -> None:
        self.database = database
        self.table = table
        self.columns = columns
        self.batch_size = batch_size

    def start(self) -> None:
        pass

    def write_many(self, rows: List[Row]) -> None:
        if not rows:
            return

        values = [[row[column] for column in self.columns] for row in rows]
        # Use a mutation pool here if this sink needs higher write throughput.
        with self.database.batch() as batch:
            batch.insert_or_update(table=self.table,
                                   columns=self.columns,
                                   values=values)

    def close(self) -> None:
        pass


class LocalJsonlSink:

    def __init__(self, path: str, batch_size: int = 1000) -> None:
        self.path = path
        self.batch_size = batch_size
        self._file = None

    def start(self) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._file = open(self.path, 'w', encoding='utf-8')

    def write_many(self, rows: List[Row]) -> None:
        if not self._file:
            raise RuntimeError("LocalJsonlSink must be started before writing")
        for row in rows:
            self._file.write(json.dumps(row, sort_keys=True) + '\n')

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None


class AggregationRunner:

    def run(self, spec: AggregationSpec) -> int:
        mapper = spec.mapper or _identity_mapper
        batch = []
        row_count = 0
        batch_size = getattr(spec.sink, "batch_size", 1000)

        spec.sink.start()
        try:
            for row in spec.source.read():
                mapped_row = mapper(row)
                if mapped_row is None:
                    continue

                batch.append(mapped_row)
                if len(batch) >= batch_size:
                    spec.sink.write_many(batch)
                    row_count += len(batch)
                    batch = []

            if batch:
                spec.sink.write_many(batch)
                row_count += len(batch)

            return row_count
        finally:
            spec.sink.close()


def _identity_mapper(row: Row) -> Row:
    return row
