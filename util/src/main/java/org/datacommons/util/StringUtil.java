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

import java.time.LocalDateTime;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.text.StringEscapeUtils;

// Common set of utils to handle strings
public class StringUtil {
  // From https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatterBuilder.html
  private static final List<String> DATE_PATTERNS =
      List.of(
          "yyyy",
          "yyyy-MM",
          "yyyyMM",
          "yyyy-M",
          "yyyy-MM-dd",
          "yyyyMMdd",
          "yyyy-M-d",
          "yyyy-MM-dd'T'HH:mm",
          "yyyy-MM-dd'T'HH:mm:ss",
          "yyyy-MM-dd'T'HH:mm:ss.SSS",
          "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

  // The Java API does not match 20071, 2007101, so add these for compatibility with CPP
  // implementation.
  private static final List<String> EXTRA_DATE_PATTERNS = List.of("^\\d{5}$", "^\\d{7}$");

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

  public static boolean isNumber(String val) {
    try {
      long l = Long.parseLong(val);
      return true;
    } catch (NumberFormatException e) {
    }
    try {
      long l = Long.parseUnsignedLong(val);
      return true;
    } catch (NumberFormatException e) {
    }
    try {
      double d = Double.parseDouble(val);
      return true;
    } catch (NumberFormatException e) {
    }
    return false;
  }

  public static boolean isBool(String val) {
    String v = val.toLowerCase();
    return v.equals("true") || v.equals("1") || v.equals("false") || v.equals("0");
  }

  public static boolean isValidISO8601Date(String dateValue) {
    for (String pattern : DATE_PATTERNS) {
      try {
        DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH).parse(dateValue);
        return true;
      } catch (DateTimeParseException ex) {
        // Pass through
      }
    }
    for (String pattern : EXTRA_DATE_PATTERNS) {
      if (Pattern.matches(pattern, dateValue)) {
        return true;
      }
    }
    return false;
  }

  public static LocalDateTime getValidISO8601Date(String dateValue) {
    // TODO: handle the extra date patterns
    for (String pattern : DATE_PATTERNS) {
      try {
        DateTimeFormatter dateFormat =
            new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .toFormatter(Locale.ENGLISH);
        return LocalDateTime.parse(dateValue, dateFormat);
      } catch (DateTimeParseException ex) {
        // Pass through
      }
    }
    return null;
  }

  // Splits a string using the delimiter character. A field is not split if the delimiter is within
  // a pair of double
  // quotes. If "includeEmpty" is true, then empty field is included.
  //
  // For example "1,2,3" will be split into ["1","2","3"] but "'1,234',5" will be
  // split into ["1,234", "5"].
  public static final class SplitAndStripArg {
    public char delimiter = ',';
    public boolean includeEmpty = false;
    public boolean stripEnclosingQuotes = true;
    public boolean stripEscapesBeforeQuotes = false;
  }

  // NOTE: We do not strip enclosing quotes in this function.
  public static List<String> splitAndStripWithQuoteEscape(
      String orig, SplitAndStripArg arg, LogCb logCb) throws AssertionError {
    List<String> results = new ArrayList<>();
    if (orig.contains("\n")) {
      if (logCb != null) {
        logCb.logError("StrSplit_MultiToken", "Found a new-line in value");
      }
      return results;
    }
    List<String> parts = new ArrayList<>();
    if (!SplitStructuredLineWithEscapes(orig, arg.delimiter, '"', parts)) {
      if (logCb != null) {
        logCb.logError(
            "StrSplit_BadQuotesInToken", "Found token with incorrectly double-quoted value");
      }
      return results;
    }
    for (String s : parts) {
      String ss = arg.stripEnclosingQuotes ? stripEnclosingQuotePair(s.trim()) : s.trim();
      // After stripping whitespace some terms could become empty.
      if (arg.includeEmpty || !ss.isEmpty()) {
        if (arg.stripEscapesBeforeQuotes) {
          // replace instances of \" with just "
          results.add(ss.replaceAll("\\\\\"", "\""));
        } else {
          results.add(ss);
        }
      }
    }
    if (results.isEmpty()) {
      if (logCb != null) {
        logCb.logError("StrSplit_EmptyToken", "Empty value found");
      }
      return results;
    }
    return results;
  }

  public static String stripEnclosingQuotePair(String val) {
    if (val.length() > 1) {
      if (val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"') {
        return val.length() == 2 ? "" : val.substring(1, val.length() - 1);
      }
    }
    return val;
  }

  public static String msgToJson(Message msg) throws InvalidProtocolBufferException {
    // Without the un-escaping something like 'Node' shows up as \u0027Node\u0027
    return StringEscapeUtils.unescapeJson(JsonFormat.printer().print(msg));
  }
}
