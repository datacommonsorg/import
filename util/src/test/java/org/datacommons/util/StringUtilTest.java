package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class StringUtilTest {

  @Test
  public void funcSplitStructuredLineWithEscapes() {
    char delimiter = ',';
    Map<Character, Character> symbolPairs = Map.of('"', '"');
    List<String> result = new ArrayList<>();

    // no symbol pairs, just split across delimiter
    result.clear();
    assertTrue(StringUtil.SplitStructuredLineWithEscapes("o ne,two,three", delimiter, '"', result));
    assertThat(result).containsExactly("o ne", "two", "three");

    // when delimiter within symbol pair, do not split on the delimiter
    result.clear();
    assertTrue(
        StringUtil.SplitStructuredLineWithEscapes(
            "'one, two', three, \"four, five\"", delimiter, '"', result));
    assertThat(result).containsExactly("'one", " two'", " three", " \"four, five\"");

    // escaped values will be returned with escape character
    result.clear();
    assertTrue(
        StringUtil.SplitStructuredLineWithEscapes(
            "\"{ \\\"type\\\": \\\"feature\\\" }\"", delimiter, '"', result));
    assertThat(result).containsExactly("\"{ \\\"type\\\": \\\"feature\\\" }\"");

    // symbol pairs must be closed
    result.clear();
    assertFalse(
        StringUtil.SplitStructuredLineWithEscapes(
            "\"{ \\\"type\\\": \\\"feature\\\" }", delimiter, '"', result));
  }

  @Test
  public void funcSplitAndStripWithQuoteEscape() {
    StringUtil.SplitAndStripArg arg = new StringUtil.SplitAndStripArg();
    assertThat(StringUtil.splitAndStripWithQuoteEscape("one,two,three", arg, null))
        .containsExactly("one", "two", "three");

    // Single quote (unterminated or otherwise) should be preserved.
    assertThat(StringUtil.splitAndStripWithQuoteEscape("\"O'Brien\", 20", arg, null))
        .containsExactly("O'Brien", "20");

    // Whitespace surrounding ',' is excluded, but within is included.
    assertThat(StringUtil.splitAndStripWithQuoteEscape(" o ne, two ,th ree", arg, null))
        .containsExactly("o ne", "two", "th ree");

    // One pair of double quotes are removed.
    assertThat(StringUtil.splitAndStripWithQuoteEscape(" '\"one\"',two,\"three\"", arg, null))
        .containsExactly("'\"one\"'", "two", "three");

    // Comma within double quotes are not split.
    assertThat(
            StringUtil.splitAndStripWithQuoteEscape("'one, two', three, \"four, five\"", arg, null))
        .containsExactly("'one", "two'", "three", "four, five");

    // Empty strings are by default removed.
    assertThat(StringUtil.splitAndStripWithQuoteEscape("one,   ,two, \"\" , three", arg, null))
        .containsExactly("one", "two", "three");

    // Empty strings are kept when specifically requested.
    arg = new StringUtil.SplitAndStripArg();
    arg.includeEmpty = true;
    assertThat(StringUtil.splitAndStripWithQuoteEscape("one,   ,two, \"\" , three", arg, null))
        .containsExactly("one", "", "two", "", "three");

    // Strings that are escaped normally show up with escape character.
    arg = new StringUtil.SplitAndStripArg();
    arg.includeEmpty = false;
    assertThat(
            StringUtil.splitAndStripWithQuoteEscape(
                "\"{ \\\"type\\\": \\\"feature\\\" }\"", arg, null))
        .containsExactly("{ \\\"type\\\": \\\"feature\\\" }");

    // Strings that are escaped when stripping of escaped quotes is requested
    arg = new StringUtil.SplitAndStripArg();
    arg.includeEmpty = false;
    arg.stripEscapesBeforeQuotes = true;
    assertThat(
            StringUtil.splitAndStripWithQuoteEscape(
                "\"{ \\\"type\\\": \\\"feature\\\" }\"", arg, null))
        .containsExactly("{ \"type\": \"feature\" }");
  }

  @Test
  public void funcISO8601Date() {
    // Year.
    assertTrue(StringUtil.isValidISO8601Date("2017"));
    assertFalse(StringUtil.isValidISO8601Date("201"));

    // Year + Month.
    assertTrue(StringUtil.isValidISO8601Date("2017-01"));
    assertTrue(StringUtil.isValidISO8601Date("2017-1"));
    assertTrue(StringUtil.isValidISO8601Date("201701"));
    assertTrue(StringUtil.isValidISO8601Date("20171"));
    assertFalse(StringUtil.isValidISO8601Date("2017-Jan"));

    // Year + Month + Day.
    assertTrue(StringUtil.isValidISO8601Date("2017-1-1"));
    assertTrue(StringUtil.isValidISO8601Date("2017-11-09"));
    assertTrue(StringUtil.isValidISO8601Date("20171109"));
    assertTrue(StringUtil.isValidISO8601Date("2017119"));
    assertFalse(StringUtil.isValidISO8601Date("2017-Nov-09"));

    // Year + Month + Day + Time.
    assertTrue(StringUtil.isValidISO8601Date("2017-11-09T22:00"));
    assertFalse(StringUtil.isValidISO8601Date("2017-11-09D22:00"));

    // Year + Month + Day + Time.
    assertTrue(StringUtil.isValidISO8601Date("2017-11-09T22:00:01"));
  }

  @Test
  public void funcGetISO8601Date() {
    // Year.
    LocalDateTime expected = LocalDateTime.of(2017, 1, 1, 0, 0);
    assertEquals(expected, StringUtil.getValidISO8601Date("2017"));
    assertNull(StringUtil.getValidISO8601Date("201"));

    // Year + Month.
    expected = LocalDateTime.of(2017, 2, 1, 0, 0);
    assertEquals(expected, StringUtil.getValidISO8601Date("2017-02"));
    assertEquals(expected, StringUtil.getValidISO8601Date("2017-2"));
    assertEquals(expected, StringUtil.getValidISO8601Date("201702"));
    assertNull(StringUtil.getValidISO8601Date("2017-Jan"));

    // Year + Month + Day.
    expected = LocalDateTime.of(2017, 2, 9, 0, 0);
    assertEquals(expected, StringUtil.getValidISO8601Date("2017-2-9"));
    expected = LocalDateTime.of(2017, 11, 9, 0, 0);
    assertEquals(expected, StringUtil.getValidISO8601Date("2017-11-09"));
    assertEquals(expected, StringUtil.getValidISO8601Date("20171109"));
    assertNull(StringUtil.getValidISO8601Date("2017-Nov-09"));

    // Year + Month + Day + Time.
    expected = LocalDateTime.of(2017, 11, 9, 22, 2);
    assertEquals(expected, StringUtil.getValidISO8601Date("2017-11-09T22:02"));
    assertNull(StringUtil.getValidISO8601Date("2017-11-09D22:02"));

    // Year + Month + Day + Time.
    expected = LocalDateTime.of(2017, 11, 9, 22, 0, 1);
    assertEquals(expected, StringUtil.getValidISO8601Date("2017-11-09T22:00:01"));
  }

  @Test
  public void funcIsNumber() {
    assertTrue(StringUtil.isNumber("1e10"));
    assertTrue(StringUtil.isNumber("1.95996"));
    assertTrue(StringUtil.isNumber("10"));
    assertTrue(StringUtil.isNumber("-10"));
    assertTrue(StringUtil.isNumber("-.0010"));
    assertFalse(StringUtil.isNumber("-.0010x"));
    assertFalse(StringUtil.isNumber("0xdeadbeef"));
    assertFalse(StringUtil.isNumber("dc/234"));
  }

  @Test
  public void funcIsBool() {
    assertTrue(StringUtil.isBool("true"));
    assertTrue(StringUtil.isBool("FALSE"));
    assertTrue(StringUtil.isBool("1"));
    assertTrue(StringUtil.isBool("0"));
    assertFalse(StringUtil.isBool("110"));
    assertFalse(StringUtil.isBool("yes"));
    assertFalse(StringUtil.isBool("10"));
  }
}
