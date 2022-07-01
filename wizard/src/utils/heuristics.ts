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

import {
  Column,
  ConfidenceLevel,
  CsvData,
  DCProperty,
  DCType,
  DetectedDetails,
  MappedThing,
  Mapping,
  MappingType,
  MappingVal,
} from "../types";
import { PlaceDetector } from "./detect_place";

/**
 * Process all columns and return the one which best represents the detected
 * Place along with its details. If no Place is detected, the return value is
 * null.
 *
 * @param columns is a mapping from column indices to (sampled) column values.
 *  The indices correspond to those in the columnOrder Array.
 * @param columnOrder is an ordered list (Array) of Columns.
 * @param pDetector a PlaceDetector object.
 *
 * @returns a MappingVal object or null if no Place is detected.
 */

function detectPlace(
  cols: Map<number, Array<string>>,
  columnOrder: Array<Column>,
  pDetector: PlaceDetector
): MappingVal {
  // Currently, only countries can be detected as Places.
  // TODO: determine a country property order for detection. For now, all
  // properties for countries are treated as equal.
  const detectedCountries = new Map<number, DetectedDetails>();

  cols.forEach((colVals: Array<string>, colIndex: number) => {
    const pD = pDetector.detect(columnOrder[colIndex].header, colVals);
    if (pD != null && pD.confidence == ConfidenceLevel.High) {
      // Check if the detected Place is a Country.
      if (pD.detectedTypeProperty.dcType.dcid === "Country") {
        detectedCountries.set(colIndex, pD);
      }
    }
  });

  if (detectedCountries.size > 0) {
    // Choose the first detected country columns.
    const ind = 0;
    return {
      type: MappingType.COLUMN, // Place detection is only possible for columns.
      column: columnOrder[ind],
      placeProperty: detectedCountries.get(ind).detectedTypeProperty.dcProperty,
      placeType: detectedCountries.get(ind).detectedTypeProperty.dcType,
    };
  }
  return null;
}

/**
 * Given a csv, returns the predicted mappings.
 *
 * @param csv a CsvData structure which contains all the necessary information
 *  and data about the user provided usv file.
 * @param pDetector a PlaceDetector object.
 *
 * @returns a Mapping of all columns to their detected details.
 */
export function getPredictions(
  csv: CsvData,
  pDetector: PlaceDetector
): Mapping {
  const m: Mapping = new Map<MappedThing, MappingVal>();

  // Iterate over all columns to determine if a Place is found.
  const placeMVal = detectPlace(
    csv.columnValuesSampled,
    csv.orderedColumns,
    pDetector
  );
  if (placeMVal != null) {
    m.set(MappedThing.PLACE, placeMVal);
  }
  return m;
}
