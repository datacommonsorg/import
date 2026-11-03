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
"""Ingestion summary report for DCP preprocessor runs.

Each preprocessor run writes one ingestion_report.json describing what was
read and written: run metadata, then per import folder its files, provenances,
observations and nodes.

Layout:
  model/              Data models that define the report's shape.
  summary_generator   Collects counts during a run and writes the report.
"""
