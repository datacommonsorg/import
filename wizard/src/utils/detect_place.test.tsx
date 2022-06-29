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
import _ from "lodash";

test("placeTypesAndFormats", () => {
  const det = new PlaceDetector();
  const expected = new Map<string, Array<DetectedFormat>>(
    [
      ["Longitude",   [ {propertyName: "longitude", displayName: "Longitude"},]],
      ["Latitude",    [ {propertyName: "latitude", displayName: "Latitude"},]],
      ["LatLon",      [ {propertyName: "GeoCoordinates", displayName: "Geo Coordinates"},]],
      ["GeoCoordinates",[ {propertyName: "GeoCoordinates", displayName: "Geo Coordinates"},]],
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
  const det = new PlaceDetector();
  const expected = new Map<string, string>(
    [
      ["longitude", "Longitude"],
      ["latitude", "Latitude"],
      ["latlon", "LatLon"],
      ["geocoordinates", "GeoCoordinates"],
      ["country", "Country"],
      ["state", "State"],
      ["province", "Province"],
      ["municipality", "Municipality"],
      ["county", "County"],
      ["city", "City"]]
    );
  expect(det.placeTypes).toEqual(expected);
})

test("placeLowConfidenceDetection", () => {
  const det = new PlaceDetector();

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
  expect(det.detectLowConfidence("LatLon")).toBe("LatLon");
  expect(det.detectLowConfidence("Geo-coordinates")).toBe("GeoCoordinates");
  expect(det.detectLowConfidence("Geo Coordinates")).toBe("GeoCoordinates");
  expect(det.detectLowConfidence("GeoCoordinates")).toBe("GeoCoordinates");
  expect(det.detectLowConfidence("longitude()()")).toBe("Longitude");
  expect(det.detectLowConfidence("Latitude=#$#$%")).toBe("Latitude");
})

test("detectionLowConf", () => {
  const det = new PlaceDetector();

  expect(det.detect("randomColName", ["1", "2", "3"])).toBe(null);
  expect(det.detect("", [])).toBe(null);

  const expected: DetectedDetails = {
          detectedType: "City",
          detectedFormat: {propertyName: "name", displayName: "Full Name"},
          confidence: ConfidenceLevel.Low
        };
  const notExpected: DetectedDetails = {
          detectedType: "City",
          detectedFormat: {propertyName: "name", displayName: "Full Name"},
          confidence: ConfidenceLevel.High // should be Low.
        };

  const got = det.detect("city", ["a", "b", "c"]);

  expect(_.isEqual(expected, got)).toBe(true);
  expect(_.isEqual(notExpected, got)).toBe(false);
})

test("countryHighConf", () => {
  var det = new PlaceDetector();
  const cases: {
    name: string,
    colArray: Array<string>;
    expected: DetectedFormat;
  }[] = [
    {
      name: "name-detection",
      colArray: ["United states", "Norway", "sri lanka", "new zealand",
                  "south africa", "australia", "Pakistan", "India",
                  "bangladesh", "french Afars and Issas"],
      expected: {propertyName: "name", displayName: "Full Name"},
    },
    {
      name: "iso-detection",
      colArray: ["us", "no", "lk", "nz",
                  "sa", "au", "pk", "in",
                  "bd", "it"],
      expected: {propertyName: "isoCode", displayName: "ISO Code"},
    },
    {
      name: "alpha3-detection",
      colArray: ["usa", "NOR", "lka", "nzl",
                  "zaf", "aus", "pak", "ind",
                  "bgd", "ita"],
      expected: {propertyName: "countryAlpha3Code", displayName: "Alpha 3 Code"},
    },
    {
      name: "numeric-detection",
      colArray: ["840", "578", "144", "554",
                  "710", "36", "586", "356",
                  "50", "380"],
      expected: {propertyName: "countryNumericCode", displayName: "Numeric Code"},
    },
    {
      name: "name-no-detection",
      //No detection due to: united statesssss, norwayyyy and north africa.
      colArray: ["United statesssss", "Norwayyyy", "sri lanka", "new zealand",
                        "north africa", "australia", "Pakistan", "India",
                        "bangladesh", "french Afars and Issas"],
      expected: null,
    },
    {
      name: "iso-no-detection",
      // No detection due to: aa, bb and cc.
      colArray: ["aa", "bb", "cc", "nz",
                "za", "au", "pk", "in",
                "bd", "it"],
      expected: null,
    },
    {
      name: "alpha3-no-detection",
      // No detection due to: aaa, bbb and ccc.
      colArray: ["aaa", "bbb", "ccc", "nzl",
                    "zaf", "aus", "pak", "ind",
                    "bgd", "ita"],
      expected: null,
    },
    {
      name: "numeric-no-detection",
      // No detection due to: 0, 1, 2.
      colArray: ["0", "1", "2", "554",
                    "710", "36", "586", "356",
                    "50", "380"],
      expected: null,
    },
  ];
  for (const c of cases) {
    if (c.expected == null) {
      expect(det.detectCountryHighConf(c.colArray)).toBe(null);
      continue;
    }
    expect(det.detectCountryHighConf(c.colArray).propertyName).toBe(
      c.expected.propertyName);
  }
});



test("countries", () => {
  const det = new PlaceDetector();
  const numCountriesExpected = 271;
  const numIsoCodes = 247;      // 24 countries without ISO codes.
  const numAbbrv3Codes = 246;   // 25 countries without 3-letter abbreviations.
  const numNumeric = 246;       // 25 countries without numeric codes.
  expect(det.countryNames.size).toEqual(numCountriesExpected);
  expect(det.countryISO.size).toEqual(numIsoCodes);
  expect(det.countryAbbrv3.size).toEqual(numAbbrv3Codes);
  expect(det.countryNumeric.size).toEqual(numNumeric);

  // Some other random spot checks.
  // Norway.
  expect(det.countryNames).toContain("norway");
  expect(det.countryISO).toContain("no");
  expect(det.countryAbbrv3).toContain("nor");
  expect(det.countryNumeric).toContain("578");

  // Senegal.
  expect(det.countryNames).toContain("senegal");
  expect(det.countryISO).toContain("sn");
  expect(det.countryAbbrv3).toContain("sen");
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
