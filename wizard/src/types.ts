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
  COLUMN = "column",
  COLUMN_HEADER = "columnHeader",
  CONSTANT = "constant"
}

export enum MappedThing {
  PLACE = "place",
  STAT_VAR = "statVar",
  DATE = "date",
  UNIT = "unit"
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

export type Mapping = Record<MappedThing, MappingVal>;

export interface CsvData {}


// Types used for Detection.

// DetectedFormat denotes the format type associated with some detected type..
export interface DetectedFormat {
  propertyName: string,
  displayName: string,
}

// Denotes a level of confidence in the detection.
// It can be associated with any detected type.
export enum ConfidenceLevel {
  Uncertain,
  Low,
  High,
}

export interface DetectedDetails {
  // The type detected.
  detectedType: string;

  // (Optional) The format detected.
  detectedFormat?: DetectedFormat;

  // The level of confidence associated with the detection.
  confidence: ConfidenceLevel;
}
