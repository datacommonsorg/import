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

import { CsvData, Mapping } from "../types";

/**
 * Generates the cleaned csv given the original csv and the correct mappings
 * @param csv
 * @param mappings
 * @returns
 */
export function generateCleanedCSV(csv: CsvData, mappings: Mapping): string {
  return "";
}

/**
 * Generates the stat vars mcf given the original csv and the correct mappings
 * @param csv
 * @param mappings
 * @returns
 */
export function generateSvMCF(csv: CsvData, mappings: Mapping): string {
  return "";
}

/**
 * Generates the translation metadata given the predicted mappings and correct
 * mappings
 * @param predictions
 * @param corrections
 * @returns
 */
export function generateTranslationMetadata(
  predictions: Mapping,
  corrections: Mapping
): string {
  return "";
}