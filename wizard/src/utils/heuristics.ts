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
  DetectedDetails,
  MappedThing,
  Mapping,
  MappingType,
  MappingVal,
} from "../types";
import * as dd from "./detect_date";
import { PlaceDetector } from "./detect_place";

/**
 * countryOrder is a helper function which returns the column index of the
 * detected country column based on a ranked order of preferred property types.
 * For example, given two columns both of which represent countries, if one of
 * them has ISO codes and the other country numbers, we will prefer the one with
 * ISO codes.
 *
 * The detection order for type:Country is:
 *    1. ISO code
 *    2. Alpha Numberic 3 Letter Abbreviation
 *    3. Numeric code
 *    4. Country name.
 *
 * @param detectedCountries is a mapping from column indices to the DetectedDetails
 *    objects which contain the specifics of the type and property detected.
 *
 * @returns the column index of the most preferred country column.
 */
function countryOrder(detectedCountries: Map<number, DetectedDetails>): number {
  const propOrder = [
    "isoCode",
    "countryAlpha3Code",
    "countryNumericCode",
    "name",
  ];
  const propDetected = new Map<string, number>();

  detectedCountries.forEach((details: DetectedDetails, index: number) => {
    const prop = details.detectedTypeProperty.dcProperty.dcid;
    propDetected.set(prop, index);
  });

  for (let i = 0; i < propOrder.length; i++) {
    if (propDetected.has(propOrder[i])) {
      return propDetected.get(propOrder[i]);
    }
  }
  return null;
}

/**
 * Process all columns and return the one which best represents the detected
 * Place along with its details. If no Place is detected, the return value is
 * null.
 *
 * @param cols is a mapping from column indices to (sampled) column values.
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
    // Get the index of the detected property according to a preference order.
    const index = countryOrder(detectedCountries);
    if (index != null) {
      return {
        type: MappingType.COLUMN, // Place detection is only possible for columns.
        column: columnOrder[index],
        placeProperty:
          detectedCountries.get(index).detectedTypeProperty.dcProperty,
        placeType: detectedCountries.get(index).detectedTypeProperty.dcType,
      };
    }
  }
  return null;
}

function detectDate(
  cols: Map<number, Array<string>>,
  columnOrder: Array<Column>
): MappingVal {
  const detectedDateColumns = new Array<Column>();
  const detectedDateHeaders = new Array<Column>();

  cols.forEach((colVals: Array<string>, colIndex: number) => {
    const col = columnOrder[colIndex];

    // Check if the column header can be parsed as a valid date.
    if (dd.detectColumnHeaderDate(col.header)) {
      detectedDateHeaders.push(col);
    } else if (dd.detectColumnWithDates(col.header, colVals)) {
      detectedDateColumns.push(col);
    }
  });
  // If both detectedDateColumns and detectedDateHeaders are non-empty,
  // return the detectedDateHeaders.
  // If detectedDateHeaders are empty but detectedDateColumns has more
  // than one column, return any (e.g. the first one).
  if (detectedDateHeaders.length > 0) {
    return {
      type: MappingType.COLUMN_HEADER,
      headers: detectedDateHeaders,
    };
  } else if (detectedDateColumns.length > 0) {
    return {
      type: MappingType.COLUMN,
      column: detectedDateColumns[0],
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
  if (placeMVal != null) {
    m.set(MappedThing.PLACE, placeMVal);
  }

  // Iterate over all columns to determine if a Date is found.
  const dateMVal = detectDate(csv.columnValuesSampled, csv.orderedColumns);
  if (dateMVal != null) {
    m.set(MappedThing.DATE, dateMVal);
  }

  return m;
}
