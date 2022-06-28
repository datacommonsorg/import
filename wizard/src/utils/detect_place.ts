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
import { DetectedDetails, DetectedFormat, ConfidenceLevel } from "../types";
import countriesJSON from "./country_mappings.json";

// A PlaceDetector objected is meant to be initialized once. It provides
// convenience access to all place types and their supported formats. It also
// supports detecting the place type for each individual column (a header and
// a list of string values).
export class PlaceDetector {
  countryNames: Set<string>;
  countryISO: Set<string>;
  countryAbbrv3: Set<string>;
  countryNumeric: Set<string>;

  placeTypes: Set<string>;

  static typeFormatMappings = new Map<string, Array<DetectedFormat>>(
    [
      ["Longitude",   [ {propertyName: "name", displayName: "Full Name"},]],
      ["Latitude",    [ {propertyName: "name", displayName: "Full Name"},]],
      ["LatLon",      [ {propertyName: "name", displayName: "Full Name"},]],
      ["Country",     [ {propertyName: "name", displayName: "Full Name"},
                        {propertyName: "isoCode", displayName: "ISO Code"},
                        {propertyName: "countryAlpha3Code", displayName: "Alpha 3 Code"},
                        {propertyName: "countryNumericCode", displayName: "Numeric Code"},
                      ]],
      ["State",       [ {propertyName: "name", displayName: "Full Name"},]],
      ["Province",    [ {propertyName: "name", displayName: "Full Name"},]],
      ["Municipality",[ {propertyName: "name", displayName: "Full Name"},]],
      ["County",      [ {propertyName: "name", displayName: "Full Name"},]],
      ["City",        [ {propertyName: "name", displayName: "Full Name"},]],
    ]);

  constructor() {
    // Set the various class attributes.
    this.preProcessCountries();

    this.placeTypes = new Set<string>();
    for (var key of Array.from(PlaceDetector.typeFormatMappings.keys())) {
      this.placeTypes.add(key.toLowerCase());
    }
  }

  // Returns a Map of all place types and their supported formats.
  validPlaceTypesAndFormats(): Map<string, Array<DetectedFormat>> {
    return PlaceDetector.typeFormatMappings;
  }

  // Process the countriesJSON object to generate the required sets.
  preProcessCountries() {
    this.countryNames = new Set<string>();
    this.countryISO = new Set<string>();
    this.countryAbbrv3 = new Set<string>();
    this.countryNumeric = new Set<string>();

    for(let country of countriesJSON) {
      this.countryNames.add(country.name);

      if (country.iso_code != null) {
        this.countryISO.add(country.iso_code);
      }
      if (country.country_alpha_3_code != null) {
        this.countryAbbrv3.add(country.country_alpha_3_code);
      }
      if (country.country_numeric_code != null) {
        this.countryNumeric.add(country.country_numeric_code);
      }
    }
  }

  // The low confidence column detector simply checks if the column header
  // (string) matches one of the supported place strings in this.placeTypes.
  // The header is converted to lower case and only alphanumeric chars are used.
  // If there is no match, the return value is null.
  detectLowConfidence(header: string): string {
    var h = header.toLowerCase().replace(/[^a-z0-9]/gi,'')

    if (this.placeTypes.has(h)) {
      var place = h.charAt(0).toUpperCase() + h.substr(1);

      // "LatLon" needs special care after the processing above.
      if (place == "Latlon") {
        return "LatLon";
      }
      return place;
    }
    return null;
  }

  // Detecting Place.
  // header: the column header string.
  // column: an array of string values.
  // If nothing is detected, null is returned.
  // Otherwise, the detectedType, the detectedFormat and and confidence level
  // are returned.
  detect(header: string, column: Array<string>): DetectedDetails {

    // High Confidence detection is TBD. For now, only doing Low Confidence
    // detection.
    var lcDetected = this.detectLowConfidence(header);
    if (lcDetected == null) {
      return null;
    }

    return {detectedType: lcDetected,
            detectedFormat: PlaceDetector.typeFormatMappings.get(lcDetected)[0],
            confidence: ConfidenceLevel.Low};
  }
}
