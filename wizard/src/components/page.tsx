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
import React, { useRef, useState } from "react";

import { CsvData, Mapping } from "../types";
import { PlaceDetector } from "../utils/detect_place";
import { UploadSection } from "./upload_section";

export function Page(): JSX.Element {
  const [csv, setCsv] = useState<CsvData>(null);
  const [predictedMapping, setPredictedMapping] = useState<Mapping>(null);
  const placeDetector = useRef(new PlaceDetector());

  return (
    <>
      <UploadSection
        onCsvProcessed={(csv) => setCsv(csv)}
        onPredictionRetrieved={(prediction) => setPredictedMapping(prediction)}
        placeDetector={placeDetector.current}
      />
    </>
  );
}
