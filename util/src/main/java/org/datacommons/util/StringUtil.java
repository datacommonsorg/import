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

// Common set of utils to handle strings
public class StringUtil {

  // Splits a line using the given delimiter and places the columns into "columns". Delimiters
  // within an expression (within a pair of expressionSymbol). Characters can be escaped, but those
  // characters will be replicated in the output columns rather than being consumed (ie. using \" to
  // escape a double quote would pass this unchanged).
  public static boolean SplitStructuredLineWithEscapes(
      String line, char delimiter, char expressionSymbol, List<String> columns) {
    boolean inExpression = false;
    int startIdx = 0;
    boolean inEscape = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (inEscape) {
        // c is an escaped character so don't handle specially below
        inEscape = false;
      } else if (c == '\\') {
        // next character is going to be an escaped character
        inEscape = true;
      } else if (!inExpression && c == delimiter) {
        // delimiter outside of expression, so split line here
        columns.add(line.substring(startIdx, i));
        startIdx = i + 1;
      } else if (c == expressionSymbol) {
        // If we're in an expression, close the expression. If we're not in an expression,
        // open a new expression.
        inExpression = !inExpression;
      }
    }
    columns.add(line.substring(startIdx));
    // all opened expressions must be closed.
    return !inExpression;
  }
}
