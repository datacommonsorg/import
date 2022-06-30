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
import _ from "lodash";

import { ConfidenceLevel, DetectedDetails, TypeProperty } from "../types";
import { PlaceDetector } from "./detect_place";

test("placeTypesAndProperties", () => {
  const det = new PlaceDetector();
  const expected = new Set<TypeProperty>([
    {
      dcType: { dcName: "GeoCoordinates", displayName: "Geo Coordinates" },
      dcProperty: { dcName: "longitude", displayName: "Longitude" },
    },
    {
      dcType: { dcName: "GeoCoordinates", displayName: "Geo Coordinates" },
      dcProperty: { dcName: "latitude", displayName: "Latitude" },
    },
    {
      dcType: { dcName: "GeoCoordinates", displayName: "Geo Coordinates" },
      dcProperty: { dcName: "name", displayName: "Name" },
    },
    {
      dcType: { dcName: "country", displayName: "Country" },
      dcProperty: { dcName: "name", displayName: "Name" },
    },
    {
      dcType: { dcName: "country", displayName: "Country" },
      dcProperty: { dcName: "isoCode", displayName: "ISO Code" },
    },
    {
      dcType: { dcName: "country", displayName: "Country" },
      dcProperty: { dcName: "countryAlpha3Code", displayName: "Alpha 3 Code" },
    },
    {
      dcType: { dcName: "country", displayName: "Country" },
      dcProperty: { dcName: "countryNumericCode", displayName: "Numeric Code" },
    },
    {
      dcType: { dcName: "state", displayName: "State" },
      dcProperty: { dcName: "name", displayName: "Name" },
    },
    {
      dcType: { dcName: "province", displayName: "Province" },
      dcProperty: { dcName: "name", displayName: "Name" },
    },
    {
      dcType: { dcName: "municipality", displayName: "Municipality" },
      dcProperty: { dcName: "name", displayName: "Name" },
    },
    {
      dcType: { dcName: "county", displayName: "County" },
      dcProperty: { dcName: "name", displayName: "Name" },
    },
    {
      dcType: { dcName: "city", displayName: "City" },
      dcProperty: { dcName: "name", displayName: "Name" },
    },
  ]);
  expect(det.placeTypesAndProperties).toEqual(expected);
});

test("placeDetectionKeys", () => {
  const expected = new Set<string>([
    "longitude",
    "latitude",
    "latlon",
    "geocoordinates",
    "country",
    "state",
    "province",
    "municipality",
    "county",
    "city",
  ]);
  const got = new Set(PlaceDetector.columnToTypePropertyMapping.keys());
  expect(got).toEqual(expected);
});

test("countries", () => {
  const det = new PlaceDetector();
  const numCountriesExpected = 271;
  const numIsoCodes = 247; // 24 countries without ISO codes.
  const numAbbrv3Codes = 246; // 25 countries without 3-letter abbreviations.
  const numNumeric = 246; // 25 countries without numeric codes.
  expect(det.countryNames.size).toEqual(numCountriesExpected);
  expect(det.countryISO.size).toEqual(numIsoCodes);
  expect(det.countryAbbrv3.size).toEqual(numAbbrv3Codes);
  expect(det.countryNumeric.size).toEqual(numNumeric); // Some other random spot checks. // Norway.

  expect(det.countryNames).toContain("norway");
  expect(det.countryISO).toContain("no");
  expect(det.countryAbbrv3).toContain("nor");
  expect(det.countryNumeric).toContain("578"); // Senegal.

  expect(det.countryNames).toContain("senegal");
  expect(det.countryISO).toContain("sn");
  expect(det.countryAbbrv3).toContain("sen");
  expect(det.countryNumeric).toContain("686"); // Other checks.

  expect(det.countryNames).not.toContain("");
  expect(det.countryNames).not.toContain(null);

  expect(det.countryISO).not.toContain("");
  expect(det.countryISO).not.toContain(null);

  expect(det.countryAbbrv3).not.toContain("");
  expect(det.countryAbbrv3).not.toContain(null);

  expect(det.countryNumeric).not.toContain("");
  expect(det.countryNumeric).not.toContain(null);
  expect(det.countryNumeric).not.toContain(0);
});

test("placeLowConfidenceDetection", () => {
  const det = new PlaceDetector();

  expect(det.detectLowConfidence("")).toBe(null);
  expect(det.detectLowConfidence(" ")).toBe(null);
  expect(det.detectLowConfidence("continent")).toBe(null);

  const countryType = { dcType: { dcName: "country", displayName: "Country" } };
  const stateType = { dcType: { dcName: "state", displayName: "State" } };
  const countyType = { dcType: { dcName: "county", displayName: "County" } };
  const cityType = { dcType: { dcName: "city", displayName: "City" } };

  expect(_.isEqual(det.detectLowConfidence("Country.."), countryType)).toBe(
    true
  );
  expect(_.isEqual(det.detectLowConfidence("Country"), countryType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("cOuntry"), countryType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("COUNTRY"), countryType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("State  "), stateType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("County---"), countyType)).toBe(
    true
  );
  expect(_.isEqual(det.detectLowConfidence("city"), cityType)).toBe(true);

  const geoType = {
    dcType: { dcName: "GeoCoordinates", displayName: "Geo Coordinates" },
  };
  expect(_.isEqual(det.detectLowConfidence("Lat-Lon"), geoType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("Lat,Lon"), geoType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("LatLon"), geoType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("Geo-coordinates"), geoType)).toBe(
    true
  );
  expect(_.isEqual(det.detectLowConfidence("Geo Coordinates"), geoType)).toBe(
    true
  );
  expect(_.isEqual(det.detectLowConfidence("GeoCoordinates"), geoType)).toBe(
    true
  );
  expect(_.isEqual(det.detectLowConfidence("longitude()()"), geoType)).toBe(
    true
  );
  expect(_.isEqual(det.detectLowConfidence("Latitude=#$#$%"), geoType)).toBe(
    true
  );
});

test("detectionLowConf", () => {
  const det = new PlaceDetector();

  expect(det.detect("randomColName", ["1", "2", "3"])).toBe(null);
  expect(det.detect("", [])).toBe(null);

  const expected: DetectedDetails = {
    detectedTypeProperty: { dcType: { dcName: "city", displayName: "City" } },
    confidence: ConfidenceLevel.Low,
  };
  const notExpected: DetectedDetails = {
    detectedTypeProperty: { dcType: { dcName: "city", displayName: "City" } },
    confidence: ConfidenceLevel.High, // should be Low.
  };

  // Should return low confidence detection of a city.
  const got = det.detect("city", ["a", "b", "c"]);

  expect(_.isEqual(expected, got)).toBe(true);
  expect(_.isEqual(notExpected, got)).toBe(false);
});

test("countryHighConf", () => {
  const det = new PlaceDetector();
  const cases: {
    name: string;
    colArray: Array<string>;
    expected: TypeProperty;
  }[] = [
    {
      name: "name-detection",
      colArray: [
        "United states",
        "Norway",
        "sri lanka",
        "new zealand",
        "south africa",
        "australia",
        "Pakistan",
        "India",
        "bangladesh",
        "french Afars and Issas",
      ],
      expected: {
        dcType: { dcName: "country", displayName: "Country" },
        dcProperty: { dcName: "name", displayName: "Name" },
      },
    },
    {
      name: "iso-detection",
      colArray: ["us", "no", "lk", "nz", "sa", "au", "pk", "in", "bd", "it"],
      expected: {
        dcType: { dcName: "country", displayName: "Country" },
        dcProperty: { dcName: "isoCode", displayName: "ISO Code" },
      },
    },
    {
      name: "alpha3-detection",
      colArray: [
        "usa",
        "NOR",
        "lka",
        "nzl",
        "zaf",
        "aus",
        "pak",
        "ind",
        "bgd",
        "ita",
      ],
      expected: {
        dcType: { dcName: "country", displayName: "Country" },
        dcProperty: {
          dcName: "countryAlpha3Code",
          displayName: "Alpha 3 Code",
        },
      },
    },
    {
      name: "numeric-detection",
      colArray: [
        "840",
        "578",
        "144",
        "554",
        "710",
        "36",
        "586",
        "356",
        "50",
        "380",
      ],
      expected: {
        dcType: { dcName: "country", displayName: "Country" },
        dcProperty: {
          dcName: "countryNumericCode",
          displayName: "Numeric Code",
        },
      },
    },
    {
      name: "numeric-detection-with-null",
      colArray: [
        "840",
        "578",
        "144",
        "554",
        "710",
        "36",
        "586",
        null,
        null,
        null,
      ],
      expected: {
        dcType: { dcName: "country", displayName: "Country" },
        dcProperty: {
          dcName: "countryNumericCode",
          displayName: "Numeric Code",
        },
      },
    },
    {
      name: "name-no-detection", //No detection due to: united statesssss, norwayyyy and north africa.
      colArray: [
        "United statesssss",
        "Norwayyyy",
        "sri lanka",
        "new zealand",
        "north africa",
        "australia",
        "Pakistan",
        "India",
        "bangladesh",
        "french Afars and Issas",
      ],
      expected: null,
    },
    {
      name: "iso-no-detection", // No detection due to: aa, bb and cc.
      colArray: ["aa", "bb", "cc", "nz", "za", "au", "pk", "in", "bd", "it"],
      expected: null,
    },
    {
      name: "alpha3-no-detection", // No detection due to: aaa, bbb and ccc.
      colArray: [
        "aaa",
        "bbb",
        "ccc",
        "nzl",
        "zaf",
        "aus",
        "pak",
        "ind",
        "bgd",
        "ita",
      ],
      expected: null,
    },
    {
      name: "numeric-no-detection", // No detection due to: 0, 1, 2.
      colArray: ["0", "1", "2", "554", "710", "36", "586", "356", "50", "380"],
      expected: null,
    },
  ];
  for (const c of cases) {
    if (c.expected == null) {
      expect(det.detect("", c.colArray)).toBe(null);
      continue;
    }
    expect(det.detect("", c.colArray)).toStrictEqual({
      detectedTypeProperty: c.expected,
      confidence: ConfidenceLevel.High,
    });
  }
});

test("placeDetection", () => {
  const det = new PlaceDetector();
  const colName = "country";

  // High Conf. Detection.
  const colArray = ["USA", "NOR", "ITA"];
  const expectedHighConf = {
    detectedTypeProperty: {
      dcType: { dcName: "country", displayName: "Country" },
      dcProperty: { dcName: "countryAlpha3Code", displayName: "Alpha 3 Code" },
    },
    confidence: ConfidenceLevel.High,
  };

  expect(det.detect(colName, colArray)).toStrictEqual(expectedHighConf);

  // Modifying colArray to ensure country cannot be detected with high conf.
  // But it should be a low confidence detection due to the colName.
  colArray[0] = "randomString";
  const expectedLowConf = {
    detectedTypeProperty: {
      dcType: { dcName: "country", displayName: "Country" },
    },
    confidence: ConfidenceLevel.Low,
  };
  expect(det.detect(colName, colArray)).toStrictEqual(expectedLowConf);

  // Now, use a column name which is not found to result in no detection.
  expect(det.detect("", colArray)).toStrictEqual(null);
});
