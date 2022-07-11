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
import { Mapping, MappingVal } from "../types";

// convert Mapping object to a regular javascript object so it can be converted
// into a JSON string
function mappingToObject(mapping: Mapping): Record<string, MappingVal> {
  const obj = {};
  mapping.forEach((mappingVal, mappedThing) => {
    obj[mappedThing] = mappingVal;
  });
  return obj;
}

/**
 * Generates the translation metadata json string given the predicted mappings
 * and correct mappings
 * @param predictions
 * @param correctedMapping
 * @returns
 */
export function generateTranslationMetadataJson(
  predictions: Mapping,
  correctedMapping: Mapping
): string {
  const translationMetadata = {
    predictions: mappingToObject(predictions),
    correctedMapping: mappingToObject(correctedMapping),
  };
  return JSON.stringify(translationMetadata);
}
