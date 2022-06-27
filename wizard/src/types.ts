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

// CvsData should contain the minimum sufficient data from the
// data csv file which will be used for all processing, e.g. column detection,
// and display/rendering.
export interface CsvData {
  // This field should dictate the fixed (internal) order of all csv columns.
  // Each value in this array is the column header.
  orderedColumnNames: Array<string>,

  // columnValuesSampled is a map from column name (header) to an extract of the
  // values in the column. This extract could be all of the column's values or
  // a sample. This is the structure that should be used for detection
  // heuristics and other advanced processing.
  // It is assumed that all columns present in the original csv data file will
  // be represented in this structure. All values in the orderedColumnNames
  // array should be present as keys of columnValuesSampled.
  // Note that the length of all column-values need not be the same, e.g. due to
  // the removal of duplicate values.
  columnValuesSampled: Map<string, Array<string>>,

  // rowsForDisplay is a mapping from the row index in the original csv file to
  // the contents of the row.
  // It is also assumed that order of values in the array will correspond to
  // the orderedColumnNames.
  rowsForDisplay: Map<BigInt, Array<string>>,
}


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
