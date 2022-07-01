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

import {
  Column,
  MappedThing,
  Mapping,
  MappingType,
  MappingVal,
} from "../types";
import { PlaceDetector } from "./detect_place";
import * as heuristics from "./heuristics";

test("detectCountry", () => {
  const det = new PlaceDetector();
  const cols = new Map<number, Array<string>>([
    [0, ["USA", "ITA"]], // This column should be detected as a Place (country).
    [1, ["fdf"]],
    [2, ["dfds"]],
  ]);
  const colCountry = { id: "a0", header: "a", columnIdx: 0 };
  const colOther1 = { id: "a0", header: "a", columnIdx: 1 };
  const colOther2 = { id: "a0", header: "a", columnIdx: 2 };

  const csv = {
    orderedColumns: [colCountry, colOther1, colOther2],
    columnValuesSampled: cols,
    rowsForDisplay: new Map<bigint, Array<string>>(),
  };

  const expected: Mapping = new Map<MappedThing, MappingVal>([
    [
      MappedThing.PLACE,
      {
        type: MappingType.COLUMN,
        column: colCountry,
        placeProperty: {
          dcid: "countryAlpha3Code",
          displayName: "Alpha 3 Code",
        },
        placeType: { dcid: "Country", displayName: "Country" },
      },
    ],
  ]);

  const got = heuristics.getPredictions(csv, det);
  expect(got).toStrictEqual(expected);
});

test("detectCountryTwoColumns", () => {
  const det = new PlaceDetector();
  const cols = new Map<number, Array<string>>([
    // One of the two columns below should be detected. They are both countries.
    [0, ["USA", "ITA"]],
    [1, ["USA"]],
    [2, ["dfds"]],
  ]);

  const colCountry = { id: "a0", header: "a", columnIdx: 0 };
  const colCountryOther = { id: "a0", header: "a", columnIdx: 1 };
  const colOther2 = { id: "a0", header: "a", columnIdx: 2 };

  const csv = {
    orderedColumns: [colCountry, colCountryOther, colOther2],
    columnValuesSampled: cols,
    rowsForDisplay: new Map<bigint, Array<string>>(),
  };

  const got = heuristics.getPredictions(csv, det).get(MappedThing.PLACE);
  expect(got.type).toStrictEqual(MappingType.COLUMN);
  expect(got.placeProperty).toStrictEqual({
    dcid: "countryAlpha3Code",
    displayName: "Alpha 3 Code",
  });
  expect(got.placeType).toStrictEqual({
    dcid: "Country",
    displayName: "Country",
  });
  expect(got.column.header).toStrictEqual("a");
});
