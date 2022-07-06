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
const MIN_HIGH_CONF_DETECT = 0.9;

/**
 * A DateDetector objected is meant to be initialized once. It provides
 * convenience access to date detectors.
 */
export class DateDetector {
  detectDate(header: string): boolean {
    const d = Date.parse(header);
    if (!Number.isNaN(d) && d >= 0) {
      return true;
    }
    return false;
  }

  /**
   * detectColumnHeaderDate returns true if 'header' can be parsed as valid Date
   * objects.
   *
   * @param header: the column header string.
   *
   * @returns a boolean which is true if 'header' can be parsed as valid Date
   *         object. It returns false otherwise.
   */
  detectColumnHeaderDate(header: string): boolean {
    return this.detectDate(header);
  }

  /**
   * detectColumnWithDates returns true if > 90% of the non-empty string 'values'
   * can be parsed as valid Date objects. It returns false otherwise.
   *
   * @param header: the column header string.
   * @param values: an array of string column values.
   *
   * @returns a boolean which is true if > 90% of values can be parsed as valid
   *          date objects. It returns false otherwise.
   */
  detectColumnWithDates(header: string, values: Array<string>): boolean {
    let detected = 0;
    let total = 0;

    for (const d of values) {
      if (d && d.length > 0) {
        total++;
        if (this.detectDate(d) === true) {
          detected++;
        }
      }
    }
    return detected > MIN_HIGH_CONF_DETECT * total ? true : false;
  }
}
