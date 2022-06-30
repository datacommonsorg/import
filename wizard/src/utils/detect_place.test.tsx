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

  const got = det.detect("city", ["a", "b", "c"]);

  expect(_.isEqual(expected, got)).toBe(true);
  expect(_.isEqual(notExpected, got)).toBe(false);
});
