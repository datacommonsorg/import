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

import { MappedThing, Mapping, MappingType } from "../types";

/*
 * Checks the provided mapping and returns an array of errors found in it.
 * On successful check, the returned array is empty.
 *
 * List of checks:
 * (1) MappingVal should have the right field set depending on MappingType
 * (2) The Main Three - place, statvar and date - must be mapped
 * (3) At most one MappedThing can be a COLUMN_HEADER
 * (4) If there are no COLUMN_HEADER mappings, VALUE should be mapped
 *     (a) Conversely, if COLUMN_HEADER mapping exists, VALUE must not be mapped
 * (5) At least one of the mappings must specify a COLUMN or a COLUMN_HEADER
 * (6) PLACE, when mapped as a COLUMN, must specify placeProperty
 */
export function checkMappings(mappings: Mapping): Array<string> {
  var errors = Array<string>();
  for (const mthing of [MappedThing.PLACE, MappedThing.STAT_VAR, MappedThing.DATE]) {
    if (!(mthing in mappings)) {
      // Check #2
      errors.push('Missing required mapping for ' + mthing);
    }
  }

  var colHdrThings = Array<string>();
  var numNonConsts = 0;
  for (const [mthing, mval] of Object.entries(mappings)) {
    if (mval.type == MappingType.COLUMN) {
      if (mval.column == null || mval.column.id == '') {
        // Check #1
        errors.push(mthing + ': missing value for COLUMN type ');
      }
      if (mthing == MappedThing.PLACE) {
        if (mval.placeProperty == null || mval.placeProperty == '') {
          // Check #6
          errors.push('Place mapped as COLUMN type is missing placeProperty');
        }
      }
      numNonConsts++;
    } else if (mval.type == MappingType.COLUMN_HEADER) {
      if (mval.headers == null || mval.headers.length == 0) {
        // Check #1
        errors.push(mthing + ': missing value for COLUMN_HEADER type');
      }
      colHdrThings.push(mthing);
      numNonConsts++;
    } else if (mval.type == MappingType.CONSTANT) {
      if (mval.constant == null || mval.constant.length == 0) {
        // Check #1
        errors.push(mthing + ': missing value for CONSTANT type');
      }
    }
  }
  if (numNonConsts == 0) {
    // Check #5
    errors.push('Atleast one mapping should identify a column');
  }
  if (colHdrThings.length == 0) {
    if (!(MappedThing.VALUE in mappings)) {
      // Check #4
      errors.push('Unable to detect "value" column');
    }
  } else if (colHdrThings.length == 1) {
    if (MappedThing.VALUE in mappings) {
      // Check #4a
      errors.push('Found multiple confusing "value" columns');
    }
  } else {
    // Check #3
    errors.push('Multiple ' + MappingType.COLUMN_HEADER +
                ' mappings found: ' + colHdrThings.join(', '));
  }

  return errors;
}