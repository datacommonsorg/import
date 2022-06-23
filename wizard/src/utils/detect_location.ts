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
import { DetectedDetails, ConfidenceLevel } from "../types";

enum LocationType {
  // None indicates a non-Location Type.
  None = "None",
  LatLon = "LatLon",
  Country = "Country",
  State = "State",
  Province = "Province",
  Municipality = "Municipality",
  County = "County",
  City = "City",
}

enum CountryFormat {
  Name =  "Full Name",
  ISO =  "ISO Code",
  Alpha2 = "Alpha 2 Code",
  Alpha3 = "Alpha 3 Code",
  Number = "Number",
}

class DetectedLocationDetails implements DetectedDetails {
  // The type detected.
  detectedType: string;

  // (Optional) The format detected.
  detectedFormat?: string;

  // The level of confidence associated with the detection.
  confidence: ConfidenceLevel;

  constructor(t : string, f: string, c: ConfidenceLevel) {
    this.detectedType = t;
    this.detectedFormat = f;
    this.confidence = c;
  }
}

// A LocationDetector objected is meant to be initialized once. It provides
// convenience access to all location types and their supported formats. It also
// supports detecting the location type for each individual column (a header and
// a list of string values).
export class LocationDetector {
  countryNames: Set<string>;
  countryISO2: Set<string>;
  countryISO3: Set<string>;

  constructor() {
    // Placeholder implementation below. These should be read from the CSV file.
    this.countryNames = new Set<string>();
    this.countryISO2 = new Set<string>();
    this.countryISO3 = new Set<string>();
  }

  // Returns a Map of all location types and their supported formats.
  validLocationTypes(): Map<string, Array<string>> {
    var m = new Map<string, Array<string>>();

    const types = Object.values(LocationType);
    for (var t of types) {
      m.set(t, []);
      if (t == LocationType.Country) {
        m.set(t, Object.values(CountryFormat));
      }
    }
    return m;
  }

  // Detecting Location.
  // header: the column header string.
  // column: an array of string values.
  detect(header: string, column: Array<string>): DetectedDetails {
    // Placeholder implementation below.
    return new DetectedLocationDetails(LocationType.None,
                                    CountryFormat.Name,
                                    ConfidenceLevel.Uncertain);
  }
}
