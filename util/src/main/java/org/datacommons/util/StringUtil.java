// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.util;

import java.util.List;
import java.util.Map;
import java.util.Stack;

// Common set of utils to handle strings
public class StringUtil {

  // Splits a line using the given delimiter and places the columns into "columns". Delimiters
  // within symbolPairs will not be split on. Characters can be escaped, but those characters will
  // be replicated in the output columns rather than being consumed (ie. using \" to escape a double
  // quote would pass this unchanged).
  public static boolean SplitStructuredLineWithEscapes(
      String line, char delimiter, Map<Character, Character> symbolPairs, List<String> columns) {
    Stack<Character> expectedToClose = new Stack<>();
    int startIdx = 0;
    boolean inEscape = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (inEscape) {
        inEscape = false;
      } else if (c == '\\') {
        inEscape = true;
      } else if (expectedToClose.isEmpty() && c == delimiter) {
        columns.add(line.substring(startIdx, i));
        startIdx = i + 1;
      } else if (!expectedToClose.isEmpty() && c == expectedToClose.peek()) {
        expectedToClose.pop();
      } else if (symbolPairs.containsKey(c)) {
        expectedToClose.push(symbolPairs.get(c));
      } else if (symbolPairs.containsValue(c)) {
        return false;
      }
    }
    columns.add(line.substring(startIdx));
    return expectedToClose.empty();
  }
}
