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
  CsvData,
  DCProperty,
  MappedThing,
  Mapping,
  MappingType,
  MappingVal,
  RowNumber,
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
    rowsForDisplay: new Map<RowNumber, Array<string>>(),
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
    rowsForDisplay: new Map<RowNumber, Array<string>>(),
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

test("countryDetectionOrder", () => {
  const det = new PlaceDetector();

  const colISO = "iso";
  const colAlpha3 = "alpha3";
  const colNumber = "number";
  const colName = "name";
  const colISOMistake = "isoMistake";

  const colVals = new Map<string, Array<string>>([
    ["iso", ["US", "IT"]],
    ["alpha3", ["USA", "ITA"]],
    ["number", ["840", "380"]],
    ["name", ["United States", "italy "]],
    ["isoMistake", ["U", "ITA"]],
  ]);

  const cases: {
    name: string;
    orderedColNames: Array<string>;
    expectedCol: Column;
    expectedProp: DCProperty;
  }[] = [
    {
      name: "all-properties",
      orderedColNames: [colISO, colAlpha3, colNumber, colName],
      expectedCol: { id: colISO + 0, header: colISO, columnIdx: 0 },
      expectedProp: {
        dcid: "isoCode",
        displayName: "ISO Code",
      },
    },
    {
      name: "iso-missing",
      orderedColNames: [colNumber, colName, colAlpha3],
      expectedCol: { id: colAlpha3 + 2, header: colAlpha3, columnIdx: 2 },
      expectedProp: {
        dcid: "countryAlpha3Code",
        displayName: "Alpha 3 Code",
      },
    },
    {
      name: "iso-alpha3-missing",
      orderedColNames: [colNumber, colName],
      expectedCol: { id: colNumber + 0, header: colNumber, columnIdx: 0 },
      expectedProp: {
        dcid: "countryNumericCode",
        displayName: "Numeric Code",
      },
    },
    {
      name: "only-name",
      orderedColNames: [colName],
      expectedCol: { id: colName + 0, header: colName, columnIdx: 0 },
      expectedProp: {
        dcid: "name",
        displayName: "Name",
      },
    },
    {
      name: "none-found",
      orderedColNames: [],
      expectedCol: null,
      expectedProp: null,
    },
    {
      name: "all-properties-iso-with-typos",
      orderedColNames: [colISOMistake, colAlpha3, colNumber, colName],
      expectedCol: { id: colAlpha3 + 1, header: colAlpha3, columnIdx: 1 },
      expectedProp: {
        dcid: "countryAlpha3Code",
        displayName: "Alpha 3 Code",
      },
    },
  ];
  for (const c of cases) {
    const colValsSampled = new Map<number, Array<string>>();
    const orderedCols = new Array<Column>();
    for (let i = 0; i < c.orderedColNames.length; i++) {
      const cName = c.orderedColNames[i];
      colValsSampled.set(i, colVals.get(cName));

      orderedCols.push({ id: cName + i, header: cName, columnIdx: i });
    }

    const csv = {
      orderedColumns: orderedCols,
      columnValuesSampled: colValsSampled,
      rowsForDisplay: new Map<RowNumber, Array<string>>(),
    };
    const got = heuristics.getPredictions(csv, det);
    if (c.expectedProp == null) {
      expect(got.size).toBe(0);
      continue;
    }

    const expected = new Map<MappedThing, MappingVal>([
      [
        MappedThing.PLACE,
        {
          type: MappingType.COLUMN,
          column: c.expectedCol,
          placeProperty: c.expectedProp,
          placeType: { dcid: "Country", displayName: "Country" },
        },
      ],
    ]);
    expect(got).toStrictEqual(expected);
  }
});
