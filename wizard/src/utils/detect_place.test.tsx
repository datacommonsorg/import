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
import { ConfidenceLevel, DetectedDetails, DetectedFormat } from "../types";
import { PlaceDetector } from "./detect_place";

// Helper function to determine if two DetectedDetails objects are equal.
function detectedDetailsDeepEquality(
  a: DetectedDetails, b: DetectedDetails): boolean {
  return (
    (a.detectedType == b.detectedType) &&
    (a.confidence == b.confidence) &&
    (a.detectedFormat.propertyName ==  b.detectedFormat.propertyName) &&
    (a.detectedFormat.displayName == b.detectedFormat.displayName)
  );
}

test("placeTypesAndFormats", () => {
  var det = new PlaceDetector();
  var expected = new Map<string, Array<DetectedFormat>>(
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
  expect(det.validPlaceTypesAndFormats()).toEqual(expected);
})

test("placeTypesLower", () => {
  var det = new PlaceDetector();
  var expected = new Set<string>(
      ["longitude", "latitude", "latlon", "country", "state", "province",
        "municipality", "county", "city"]
    );
  expect(det.placeTypes).toEqual(expected);
})

test("placeLowConfidenceDetection", () => {
  var det = new PlaceDetector();

  expect(det.detectLowConfidence("")).toBe(null);
  expect(det.detectLowConfidence(" ")).toBe(null);
  expect(det.detectLowConfidence("continent")).toBe(null);

  expect(det.detectLowConfidence("Country..")).toBe("Country");
  expect(det.detectLowConfidence("Country")).toBe("Country");
  expect(det.detectLowConfidence("cOuntry")).toBe("Country");
  expect(det.detectLowConfidence("COUNTRY")).toBe("Country");
  expect(det.detectLowConfidence("State  ")).toBe("State");
  expect(det.detectLowConfidence("County---")).toBe("County");
  expect(det.detectLowConfidence("city")).toBe("City");

  expect(det.detectLowConfidence("Lat-Lon")).toBe("LatLon");
  expect(det.detectLowConfidence("Lat,Lon")).toBe("LatLon");
  expect(det.detectLowConfidence("Lat Lon")).toBe("LatLon");
  expect(det.detectLowConfidence("LatLon")).toBe("LatLon");
  expect(det.detectLowConfidence("longitude()()")).toBe("Longitude");
  expect(det.detectLowConfidence("Latitude=#$#$%")).toBe("Latitude");
})

test("detectionLowConf", () => {
  var det = new PlaceDetector();

  expect(det.detect("randomColName", ["1", "2", "3"])).toBe(null);
  expect(det.detect("", [])).toBe(null);

  var expected: DetectedDetails = {
          detectedType: "City",
          detectedFormat: {propertyName: "name", displayName: "Full Name"},
          confidence: ConfidenceLevel.Low
        };
  var notExpected: DetectedDetails = {
          detectedType: "City",
          detectedFormat: {propertyName: "name", displayName: "Full Name"},
          confidence: ConfidenceLevel.High // should be Low.
        };

  var got = det.detect("city", ["a", "b", "c"]);

  expect(detectedDetailsDeepEquality(expected, got)).toBe(true);
  expect(detectedDetailsDeepEquality(notExpected, got)).toBe(false);

})


test("countries", () => {
  var det = new PlaceDetector();
  var numCountriesExpected = 271;
  var numIsoCodes = 247;      // 24 countries without ISO codes.
  var numAbbrv3Codes = 246;   // 25 countries without 3-letter abbreviations.
  var numNumeric = 246;       // 25 countries without numeric codes.
  expect(det.countryNames.size).toEqual(numCountriesExpected);
  expect(det.countryISO.size).toEqual(numIsoCodes);
  expect(det.countryAbbrv3.size).toEqual(numAbbrv3Codes);
  expect(det.countryNumeric.size).toEqual(numNumeric);

  // Some other random spot checks.
  // Norway.
  expect(det.countryNames).toContain("Norway");
  expect(det.countryISO).toContain("NO");
  expect(det.countryAbbrv3).toContain("NOR");
  expect(det.countryNumeric).toContain("578");

  // Senegal.
  expect(det.countryNames).toContain("Senegal");
  expect(det.countryISO).toContain("SN");
  expect(det.countryAbbrv3).toContain("SEN");
  expect(det.countryNumeric).toContain("686");

  // Other checks.
  expect(det.countryNames).not.toContain("");
  expect(det.countryNames).not.toContain(null);

  expect(det.countryISO).not.toContain("");
  expect(det.countryISO).not.toContain(null);

  expect(det.countryAbbrv3).not.toContain("");
  expect(det.countryAbbrv3).not.toContain(null);

  expect(det.countryNumeric).not.toContain("");
  expect(det.countryNumeric).not.toContain(null);
  expect(det.countryNumeric).not.toContain(0);
})
