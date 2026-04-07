package org.datacommons.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.jsonld.JsonLdParser;
import org.junit.Test;

public class JsonLdParserTest {

  @Test
  public void testExpandedJsonLd_ProducesCanonicalMcfGraph() throws Exception {
    String jsonLd =
        "[\n"
            + "  {\n"
            + "    \"@id\": \"http://example.com/subject1\",\n"
            + "    \"http://schema.org/name\": [\n"
            + "      { \"@value\": \"Canonical Entity Name\" }\n"
            + "    ]\n"
            + "  }\n"
            + "]";

    InputStream in = new ByteArrayInputStream(jsonLd.getBytes(StandardCharsets.UTF_8));
    McfGraph graph = JsonLdParser.parse(in);

    assertNotNull(graph);
    assertTrue(graph.getNodesMap().containsKey("http://example.com/subject1"));
  }

  @Test
  public void testCompactedContextMapping_ExpandsSuccessfully() throws Exception {
    String jsonLd =
        "{\n"
            + "  \"@context\": {\n"
            + "    \"name\": \"http://schema.org/name\"\n"
            + "  },\n"
            + "  \"@id\": \"http://example.com/subject2\",\n"
            + "  \"name\": \"Compacted Alias Name\"\n"
            + "}";

    InputStream in = new ByteArrayInputStream(jsonLd.getBytes(StandardCharsets.UTF_8));
    McfGraph graph = JsonLdParser.parse(in);

    assertNotNull(graph);
    assertTrue(graph.getNodesMap().containsKey("http://example.com/subject2"));
  }
}
