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

import { MappingVal, Mapping, MappingType, MappedThing } from "../types";
import { checkMappings } from "./validation";
 
test("Fail_MalformedMappingVal", () => {
  const input:Mapping = new Map([
    [MappedThing.PLACE, {
            type: MappingType.COLUMN,
    }],
    [MappedThing.STAT_VAR, {
            type: MappingType.COLUMN_HEADER,
            headers: []
    }],
    [MappedThing.DATE, {
            type: MappingType.CONSTANT,
            constant: '',
    }],
  ]);
  const expected = [
    "place: missing value for COLUMN type ",
    "Place mapped as COLUMN type is missing placeProperty",
    "statvar: missing value for COLUMN_HEADER type",
    "date: missing value for CONSTANT type",
  ];
  expect(checkMappings(input)).toEqual(expected);
});

test("Fail_MissingRequiredPropsAndMultipleColumnHeaders", () => {
  const input:Mapping = new Map([
    [MappedThing.UNIT, {
            type: MappingType.COLUMN_HEADER,
            headers: [{ id: 'a', header: 'a', columnIdx: 1}],
    }],
    [MappedThing.VALUE, {
            type: MappingType.COLUMN_HEADER,
            headers: [{ id: 'b', header: 'b', columnIdx: 2}],
    }],
  ]);
  const expected = [
    "Missing required mapping for place",
    "Missing required mapping for statvar",
    "Missing required mapping for date",
    "Multiple columnHeader mappings found: unit, value"
  ];
  expect(checkMappings(input)).toEqual(expected);
});

test("Fail_ValueMissing", () => {
  const input:Mapping = new Map([
    [MappedThing.PLACE, {
            type: MappingType.COLUMN,
            column: { id: 'iso', header: 'iso', columnIdx: 1},
            placeProperty: 'isoCode',
    }],
    [MappedThing.STAT_VAR, {
            type: MappingType.COLUMN,
            column: {id: 'indicators', header: 'indicators', columnIdx: 2},
    }],
    [MappedThing.DATE, {
            type: MappingType.CONSTANT,
            constant: '2019',
    }],
  ]);
  const expected = [
    "Unable to detect 'value' column",
  ];
  expect(checkMappings(input)).toEqual(expected);
});

test("Pass_DateInColumnHeader", () => {
  const input:Mapping = new Map([
    [MappedThing.PLACE, {
            type: MappingType.COLUMN,
            column: { id: 'iso', header: 'iso', columnIdx: 1},
            placeProperty: 'isoCode',
    }],
    [MappedThing.STAT_VAR, {
            type: MappingType.COLUMN,
            column: {id: 'indicators', header: 'indicators', columnIdx: 2},
    }],
    [MappedThing.DATE, {
            type: MappingType.COLUMN_HEADER,
            headers: [
                { id: '2018', header: '2018', columnIdx: 3},
                { id: '2019', header: '2019', columnIdx: 4},
            ],
    }],
    [MappedThing.UNIT, {
            type: MappingType.CONSTANT,
            constant: 'USDollar',
    }],
  ]);
  const expected = [];
  expect(checkMappings(input)).toEqual(expected);
});

test("Pass_NoColumnHeader", () => {
  const input:Mapping = new Map([
    [MappedThing.PLACE, {
            type: MappingType.COLUMN,
            column: { id: 'iso', header: 'iso', columnIdx: 1},
            placeProperty: 'isoCode',
    }],
    [MappedThing.STAT_VAR, {
            type: MappingType.COLUMN,
            column: {id: 'indicators', header: 'indicators', columnIdx: 2},
    }],
    [MappedThing.DATE, {
            type: MappingType.COLUMN,
            column: {id: 'date', header: 'date', columnIdx: 3},
    }],
    [MappedThing.VALUE, {
            type: MappingType.COLUMN,
            column: {id: 'val', header: 'val', columnIdx: 4},
    }],
    [MappedThing.UNIT, {
            type: MappingType.CONSTANT,
            constant: 'USDollar',
    }],
  ]);
  const expected = [];
  expect(checkMappings(input)).toEqual(expected);
});