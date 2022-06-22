/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export enum MappingType {
  COLUMN,
  COLUMN_HEADERS,
  CONSTANT
}

interface Column {
  // header title
  header: string;
  // the column number (leftmost column will be 1)
  columnNum: number;
}

export interface MappingVal {
  type: MappingType;
  // Column that holds the mapping values. Should be set if type is
  // MappingType.COLUMN
  column?: Column;
  // If column is set, the values in the column correspond to this property in
  // the KG
  valueProperty?: string;
  // List of column headers that act as the mapping values. Should be set if
  // type is MappingType.COLUMN_HEADERS
  headers?: Column[];
  // Constant value as the mapping value. Should be set if type is
  // MappingType.CONSTANT
  constant?: string;
}

export interface Mapping {
  place: MappingVal,
  statVars: MappingVal,
  dates: MappingVal,
  units?: MappingVal
}