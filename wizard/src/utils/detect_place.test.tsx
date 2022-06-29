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
import { DetectedFormat } from "../types";
import { PlaceDetector } from "./detect_place";

test("placeTypesAndFormats", () => {
  const det = new PlaceDetector();
  const expected = new Map<string, Array<DetectedFormat>>([
    ["None", [{ propertyName: "name", displayName: "Full Name" }]],
    ["LatLon", [{ propertyName: "name", displayName: "Full Name" }]],
    [
      "Country",
      [
        { propertyName: "name", displayName: "Full Name" },
        { propertyName: "isoCode", displayName: "ISO Code" },
        { propertyName: "countryAlpha3Code", displayName: "Alpha 3 Code" },
        { propertyName: "countryNumericCode", displayName: "Numeric Code" },
      ],
    ],
    ["State", [{ propertyName: "name", displayName: "Full Name" }]],
    ["Province", [{ propertyName: "name", displayName: "Full Name" }]],
    ["Municipality", [{ propertyName: "name", displayName: "Full Name" }]],
    ["County", [{ propertyName: "name", displayName: "Full Name" }]],
    ["City", [{ propertyName: "name", displayName: "Full Name" }]],
  ]);
  expect(det.validPlaceTypesAndFormats()).toEqual(expected);
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
});
