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
      dcType: { dcid: "GeoCoordinates", displayName: "Geo Coordinates" },
      dcProperty: { dcid: "longitude", displayName: "Longitude" },
    },
    {
      dcType: { dcid: "GeoCoordinates", displayName: "Geo Coordinates" },
      dcProperty: { dcid: "latitude", displayName: "Latitude" },
    },
    {
      dcType: { dcid: "GeoCoordinates", displayName: "Geo Coordinates" },
      dcProperty: { dcid: "name", displayName: "Name" },
    },
    {
      dcType: { dcid: "country", displayName: "Country" },
      dcProperty: { dcid: "name", displayName: "Name" },
    },
    {
      dcType: { dcid: "country", displayName: "Country" },
      dcProperty: { dcid: "isoCode", displayName: "ISO Code" },
    },
    {
      dcType: { dcid: "country", displayName: "Country" },
      dcProperty: { dcid: "countryAlpha3Code", displayName: "Alpha 3 Code" },
    },
    {
      dcType: { dcid: "country", displayName: "Country" },
      dcProperty: { dcid: "countryNumericCode", displayName: "Numeric Code" },
    },
    {
      dcType: { dcid: "state", displayName: "State" },
      dcProperty: { dcid: "name", displayName: "Name" },
    },
    {
      dcType: { dcid: "province", displayName: "Province" },
      dcProperty: { dcid: "name", displayName: "Name" },
    },
    {
      dcType: { dcid: "municipality", displayName: "Municipality" },
      dcProperty: { dcid: "name", displayName: "Name" },
    },
    {
      dcType: { dcid: "county", displayName: "County" },
      dcProperty: { dcid: "name", displayName: "Name" },
    },
    {
      dcType: { dcid: "city", displayName: "City" },
      dcProperty: { dcid: "name", displayName: "Name" },
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

  const countryType = { dcType: { dcid: "country", displayName: "Country" } };
  const stateType = { dcType: { dcid: "state", displayName: "State" } };
  const countyType = { dcType: { dcid: "county", displayName: "County" } };
  const cityType = { dcType: { dcid: "city", displayName: "City" } };

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
    dcType: { dcid: "GeoCoordinates", displayName: "Geo Coordinates" },
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
    detectedTypeProperty: { dcType: { dcid: "city", displayName: "City" } },
    confidence: ConfidenceLevel.Low,
  };
  const notExpected: DetectedDetails = {
    detectedTypeProperty: { dcType: { dcid: "city", displayName: "City" } },
    confidence: ConfidenceLevel.High, // should be Low.
  };

  const got = det.detect("city", ["a", "b", "c"]);

  expect(_.isEqual(expected, got)).toBe(true);
  expect(_.isEqual(notExpected, got)).toBe(false);
});
