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

import { MappedThing, Mapping, MappingType } from "../types";
import { generateTranslationMetadataJson } from "./file_generation";

test("generateTranslationMetadataJson", () => {
  const dateColHeader1 = { id: "2022-100", header: "2022-10", columnIdx: 0 };
  const dateColHeader2 = { id: "20211", header: "2021-10", columnIdx: 1 };
  const dateCol = { id: "c2", header: "c", columnIdx: 2 };
  const countryCol = { id: "d3", header: "d", columnIdx: 3 };
  const predictedMapping: Mapping = new Map([
    [
      MappedThing.PLACE,
      {
        type: MappingType.COLUMN,
        column: countryCol,
        placeProperty: {
          dcid: "name",
          displayName: "name",
        },
        placeType: { dcid: "Country", displayName: "Country" },
      },
    ],
    [
      MappedThing.DATE,
      {
        type: MappingType.COLUMN,
        column: dateCol,
      },
    ],
  ]);
  const correctedMapping: Mapping = new Map([
    [
      MappedThing.PLACE,
      {
        type: MappingType.COLUMN,
        column: countryCol,
        placeProperty: {
          dcid: "countryAlpha3Code",
          displayName: "Alpha 3 Code",
        },
        placeType: { dcid: "Country", displayName: "Country" },
      },
    ],
    [
      MappedThing.DATE,
      {
        type: MappingType.COLUMN_HEADER,
        headers: [dateColHeader1, dateColHeader2],
      },
    ],
  ]);

  const cases: {
    name: string;
    prediction: Mapping;
    correctedMapping: Mapping;
    expected: string;
  }[] = [
    {
      name: "empty prediction",
      prediction: new Map(),
      correctedMapping: correctedMapping,
      expected:
        '{"predictions":{},"correctedMapping":{"place":{"type":"column","column":{"id":"d3","header":"d","columnIdx":3},"placeProperty":{"dcid":"countryAlpha3Code","displayName":"Alpha 3 Code"},"placeType":{"dcid":"Country","displayName":"Country"}},"date":{"type":"columnHeader","headers":[{"id":"2022-100","header":"2022-10","columnIdx":0},{"id":"20211","header":"2021-10","columnIdx":1}]}}}',
    },
    {
      name: "empty correctedMapping",
      prediction: predictedMapping,
      correctedMapping: new Map(),
      expected:
        '{"predictions":{"place":{"type":"column","column":{"id":"d3","header":"d","columnIdx":3},"placeProperty":{"dcid":"name","displayName":"name"},"placeType":{"dcid":"Country","displayName":"Country"}},"date":{"type":"column","column":{"id":"c2","header":"c","columnIdx":2}}},"correctedMapping":{}}',
    },
    {
      name: "both empty",
      prediction: new Map(),
      correctedMapping: new Map(),
      expected: '{"predictions":{},"correctedMapping":{}}',
    },
    {
      name: "both non empty",
      prediction: predictedMapping,
      correctedMapping: correctedMapping,
      expected:
        '{"predictions":{"place":{"type":"column","column":{"id":"d3","header":"d","columnIdx":3},"placeProperty":{"dcid":"name","displayName":"name"},"placeType":{"dcid":"Country","displayName":"Country"}},"date":{"type":"column","column":{"id":"c2","header":"c","columnIdx":2}}},"correctedMapping":{"place":{"type":"column","column":{"id":"d3","header":"d","columnIdx":3},"placeProperty":{"dcid":"countryAlpha3Code","displayName":"Alpha 3 Code"},"placeType":{"dcid":"Country","displayName":"Country"}},"date":{"type":"columnHeader","headers":[{"id":"2022-100","header":"2022-10","columnIdx":0},{"id":"20211","header":"2021-10","columnIdx":1}]}}}',
    },
  ];

  for (const c of cases) {
    const result = generateTranslationMetadataJson(
      c.prediction,
      c.correctedMapping
    );
    try {
      expect(result).toEqual(c.expected);
    } catch (e) {
      console.log("test failed for: " + c.name);
      throw e;
    }
  }
});
