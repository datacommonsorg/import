package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;

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
    assertTrue(
        StringUtil.SplitStructuredLineWithEscapes(
            "o ne,two,three", delimiter, symbolPairs, result));
    assertThat(result).containsExactly("o ne", "two", "three");

    // when delimiter within symbol pair, do not split on the delimiter
    result.clear();
    assertTrue(
        StringUtil.SplitStructuredLineWithEscapes(
            "'one, two', three, \"four, five\"", delimiter, symbolPairs, result));
    assertThat(result).containsExactly("'one", " two'", " three", " \"four, five\"");

    // escaped values will be returned with escape character
    result.clear();
    assertTrue(
        StringUtil.SplitStructuredLineWithEscapes(
            "\"{ \\\"type\\\": \\\"feature\\\" }\"", delimiter, symbolPairs, result));
    assertThat(result).containsExactly("\"{ \\\"type\\\": \\\"feature\\\" }\"");

    // symbol pairs must be closed
    result.clear();
    assertFalse(
        StringUtil.SplitStructuredLineWithEscapes(
            "\"{ \\\"type\\\": \\\"feature\\\" }", delimiter, symbolPairs, result));

    // multiple symbol pairs
    result.clear();
    symbolPairs = Map.of('{', '}', '[', ']');
    assertTrue(
        StringUtil.SplitStructuredLineWithEscapes(
            "\\{item1\\},it\\\\em2,{\\{subitem1\\},sub\\\\item2},item4\\,item5,[5,{6,7}]",
            delimiter,
            symbolPairs,
            result));
    assertThat(result)
        .containsExactly(
            "\\{item1\\}",
            "it\\\\em2",
            "{\\{subitem1\\},sub\\\\item2}",
            "item4\\,item5",
            "[5,{6,7}]");
  }
}
