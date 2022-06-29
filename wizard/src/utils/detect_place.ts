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
  ConfidenceLevel,
  DetectedDetails,
  PlaceProperty,
  PlaceType,
  TypeProperty,
} from "../types";
import countriesJSON from "./country_mappings.json";

const MIN_HIGH_CONF_DETECT = 0.9;

// All supported Place types must be encoded below.
const PLACE_TYPES = new Map<string, PlaceType>([
  ["geo", { typeName: "GeoCoordinates", displayName: "Geo Coordinates" }],
  ["state", { typeName: "state", displayName: "State" }],
  ["country", { typeName: "country", displayName: "Country" }],
  ["province", { typeName: "province", displayName: "Province" }],
  ["municipality", { typeName: "municipality", displayName: "Municipality" }],
  ["county", { typeName: "county", displayName: "County" }],
  ["city", { typeName: "city", displayName: "City" }],
]);

// All supported Place properties must be encoded below.
const PLACE_PROPERTIES = new Map<string, PlaceProperty>([
  ["name", { propertyName: "name", displayName: "Name" }],
  ["lon", { propertyName: "longitude", displayName: "Longitude" }],
  ["lat", { propertyName: "latitude", displayName: "Latitude" }],
  ["countryISO", { propertyName: "isoCode", displayName: "ISO Code" }],
  [
    "countryAlpha3",
    { propertyName: "countryAlpha3Code", displayName: "Alpha 3 Code" },
  ],
  [
    "countryNumeric",
    { propertyName: "countryNumericCode", displayName: "Numeric Code" },
  ],
]);

// Helper interface to refer to the place types and place properties.
interface TPKey {
  tKey: string;
  pKey: string;
}

function toAlphaNumeric(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/gi, "");
}

function toTypeProperty(t: PlaceType, p: PlaceProperty): TypeProperty {
  if (p != null) {
    return {
      typeName: t.typeName,
      typeDisplayName: t.displayName,
      propertyName: p.propertyName,
      propertyDisplayName: p.displayName,
    };
  } else {
    return { typeName: t.typeName, typeDisplayName: t.displayName };
  }
}

/**
 * A PlaceDetector objected is meant to be initialized once. It provides
 * convenience access to all place types and their supported formats. It also
 * supports detecting the place type for each individual column (a header and
 * a list of string values).
 */
export class PlaceDetector {
  countryNames: Set<string>;
  countryISO: Set<string>;
  countryAbbrv3: Set<string>;
  countryNumeric: Set<string>;

  // A set of all place types and their associated properties.
  placeTypesAndProperties: Set<TypeProperty>;

  // Mapping between Place types and supported properties associated with each
  // type.
  static keyTypePropertyMappings = new Map<string, Array<TPKey>>([
    ["longitude", [{ tKey: "geo", pKey: "lon" }]],
    ["latitude", [{ tKey: "geo", pKey: "lat" }]],
    ["latlon", [{ tKey: "geo", pKey: "name" }]],
    ["geocoordinates", [{ tKey: "geo", pKey: "name" }]],
    [
      "country",
      [
        { tKey: "country", pKey: "name" },
        { tKey: "country", pKey: "countryISO" },
        { tKey: "country", pKey: "countryAlpha3" },
        { tKey: "country", pKey: "countryNumeric" },
      ],
    ],
    ["state", [{ tKey: "state", pKey: "name" }]],
    ["province", [{ tKey: "province", pKey: "name" }]],
    ["municipality", [{ tKey: "municipality", pKey: "name" }]],
    ["county", [{ tKey: "county", pKey: "name" }]],
    ["city", [{ tKey: "city", pKey: "name" }]],
  ]);

  constructor() {
    // Set the various class attributes.
    this.preProcessCountries();
    this.setValidPlaceTypesAndProperties();
  }

  /**
   * Processes keyTypePropertyMappings to set the placeTypesAndProperties attribute.
   */
  setValidPlaceTypesAndProperties() {
    const tpMap = new Map<string, TypeProperty>();
    const valArray = Array.from(PlaceDetector.keyTypePropertyMappings.values());
    for (const tpKeys of valArray) {
      for (const tp of tpKeys) {
        // Create unique keys using a combination of the type and property.
        const key = tp.tKey + tp.pKey;
        if (tpMap.has(key)) {
          continue;
        }
        tpMap.set(
          key,
          toTypeProperty(
            PLACE_TYPES.get(tp.tKey),
            PLACE_PROPERTIES.get(tp.pKey)
          )
        );
      }
    }
    this.placeTypesAndProperties = new Set(tpMap.values());
  }

  /**
   * Process the countriesJSON object to generate the required sets.
   */
  preProcessCountries() {
    this.countryNames = new Set<string>();
    this.countryISO = new Set<string>();
    this.countryAbbrv3 = new Set<string>();
    this.countryNumeric = new Set<string>();

    for (const country of countriesJSON) {
      this.countryNames.add(toAlphaNumeric(country.name));

      if (country.iso_code != null) {
        this.countryISO.add(toAlphaNumeric(country.iso_code));
      }
      if (country.country_alpha_3_code != null) {
        this.countryAbbrv3.add(toAlphaNumeric(country.country_alpha_3_code));
      }
      if (country.country_numeric_code != null) {
        this.countryNumeric.add(toAlphaNumeric(country.country_numeric_code));
      }
    }
  }

  /**
   * The low confidence column detector simply checks if the column header
   * (string) matches one of the keys in keyTypePropertyMappings.
   * The header is converted to lower case and only alphanumeric chars are used.
   * If there is no match, the return value is null.
   *
   * @param header the name of the column.
   *
   * @return the TypeProperty object (with no PlaceProperty) or null if nothing
   *  can be determined with low confidence.
   */
  detectLowConfidence(header: string): TypeProperty {
    const h = header.toLowerCase().replace(/[^a-z0-9]/gi, "");
    if (PlaceDetector.keyTypePropertyMappings.has(h)) {
      // Use any of the TPKeys in the Array<TPKey> associated with the
      // value associated with 'h'. All the TPKeys associated with 'h' are
      // expected to have the same place type (tKey).
      const typeKey = PlaceDetector.keyTypePropertyMappings.get(h)[0].tKey;

      // Use null for the PlaceProperty because we are only matching types
      // for the low confidence cases.
      return toTypeProperty(PLACE_TYPES.get(typeKey), null);
    }
    return null;
  }

  /**
   * Detecting Place.
   * If nothing is detected, null is returned.
   * Otherwise, the detectedType, the detectedFormat and and confidence level
   * are returned.
   * It is up to the consumer, e.g. in heuristics.ts, to decide whether to
   * pass the low confidence detection back to the user (or not).
   *
   * @param header: the column header string.
   * @param column: an array of string column values.
   *
   * @return the DetectedDetails object (or null).
   */
  detect(header: string, column: Array<string>): DetectedDetails {
    // High Confidence detection is TBD. For now, only doing Low Confidence
    // detection.
    const lcDetected = this.detectLowConfidence(header);
    if (lcDetected == null) {
      return null;
    }

    return {
      detectedTypeProperty: lcDetected,
      confidence: ConfidenceLevel.Low,
    };
  }
}
