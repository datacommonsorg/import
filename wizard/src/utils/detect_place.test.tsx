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

function lowConfType(tName: string, tDisplay: string): TypeProperty {
  return {
    typeName: tName,
    typeDisplayName: tDisplay,
  };
}

test("placeTypesAndProperties", () => {
  const det = new PlaceDetector();
  const expected = new Set<TypeProperty>([
    {
      typeName: "GeoCoordinates",
      typeDisplayName: "Geo Coordinates",
      propertyName: "longitude",
      propertyDisplayName: "Longitude",
    },
    {
      typeName: "GeoCoordinates",
      typeDisplayName: "Geo Coordinates",
      propertyName: "latitude",
      propertyDisplayName: "Latitude",
    },
    {
      typeName: "GeoCoordinates",
      typeDisplayName: "Geo Coordinates",
      propertyName: "name",
      propertyDisplayName: "Name",
    },
    {
      typeName: "country",
      typeDisplayName: "Country",
      propertyName: "name",
      propertyDisplayName: "Name",
    },
    {
      typeName: "country",
      typeDisplayName: "Country",
      propertyName: "isoCode",
      propertyDisplayName: "ISO Code",
    },
    {
      typeName: "country",
      typeDisplayName: "Country",
      propertyName: "countryAlpha3Code",
      propertyDisplayName: "Alpha 3 Code",
    },
    {
      typeName: "country",
      typeDisplayName: "Country",
      propertyName: "countryNumericCode",
      propertyDisplayName: "Numeric Code",
    },
    {
      typeName: "state",
      typeDisplayName: "State",
      propertyName: "name",
      propertyDisplayName: "Name",
    },
    {
      typeName: "province",
      typeDisplayName: "Province",
      propertyName: "name",
      propertyDisplayName: "Name",
    },
    {
      typeName: "municipality",
      typeDisplayName: "Municipality",
      propertyName: "name",
      propertyDisplayName: "Name",
    },
    {
      typeName: "county",
      typeDisplayName: "County",
      propertyName: "name",
      propertyDisplayName: "Name",
    },
    {
      typeName: "city",
      typeDisplayName: "City",
      propertyName: "name",
      propertyDisplayName: "Name",
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
  const got = new Set(PlaceDetector.keyTypePropertyMappings.keys());
  expect(got).toEqual(expected);
});

test("placeLowConfidenceDetection", () => {
  const det = new PlaceDetector();

  expect(det.detectLowConfidence("")).toBe(null);
  expect(det.detectLowConfidence(" ")).toBe(null);
  expect(det.detectLowConfidence("continent")).toBe(null);

  const countryType = lowConfType("country", "Country");
  expect(_.isEqual(det.detectLowConfidence("Country.."), countryType)).toBe(
    true
  );
  expect(_.isEqual(det.detectLowConfidence("Country"), countryType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("cOuntry"), countryType)).toBe(true);
  expect(_.isEqual(det.detectLowConfidence("COUNTRY"), countryType)).toBe(true);
  expect(
    _.isEqual(det.detectLowConfidence("State  "), lowConfType("state", "State"))
  ).toBe(true);
  expect(
    _.isEqual(
      det.detectLowConfidence("County---"),
      lowConfType("county", "County")
    )
  ).toBe(true);
  expect(
    _.isEqual(det.detectLowConfidence("city"), lowConfType("city", "City"))
  ).toBe(true);

  const geoType = lowConfType("GeoCoordinates", "Geo Coordinates");
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
    detectedTypeProperty: lowConfType("city", "City"),
    confidence: ConfidenceLevel.Low,
  };
  const notExpected: DetectedDetails = {
    detectedTypeProperty: lowConfType("city", "City"),
    confidence: ConfidenceLevel.High, // should be Low.
  };

  const got = det.detect("city", ["a", "b", "c"]);

  expect(_.isEqual(expected, got)).toBe(true);
  expect(_.isEqual(notExpected, got)).toBe(false);
});
