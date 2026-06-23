package org.datacommons.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.parser.jsonld.JsonLdParser;
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

  @Test
  public void testPrefixStripping_RemovesDcidAndSchemaPrefixes() throws Exception {
    String jsonLd =
        "{\n"
            + "  \"@context\": {\n"
            + "    \"schema\": \"https://schema.org/\",\n"
            + "    \"dcid\": \"https://datacommons.org/browser/\"\n"
            + "  },\n"
            + "  \"@graph\": [\n"
            + "    {\n"
            + "      \"@id\": \"dcid:JaneDoe_Sample\",\n"
            + "      \"@type\": \"schema:Person\",\n"
            + "      \"schema:name\": \"Jane Doe\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    InputStream in = new ByteArrayInputStream(jsonLd.getBytes(StandardCharsets.UTF_8));
    McfGraph graph = JsonLdParser.parse(in);

    assertNotNull(graph);
    // The ID should be stripped of the dcid prefix
    assertTrue(graph.getNodesMap().containsKey("JaneDoe_Sample"));

    McfGraph.PropertyValues node = graph.getNodesMap().get("JaneDoe_Sample");
    assertNotNull(node);

    // The type should be stripped of the schema prefix
    boolean hasCleanType = false;
    for (org.datacommons.proto.Mcf.McfGraph.TypedValue tv :
        node.getPvsMap().get("typeOf").getTypedValuesList()) {
      if ("Person".equals(tv.getValue())) {
        hasCleanType = true;
        break;
      }
    }
    assertTrue("Stored type should be 'Person' without schema prefix", hasCleanType);
  }

  @Test
  public void testNestedObservationProperties() throws Exception {
    String jsonLd =
        "{\n"
            + "  \"@context\": {\n"
            + "    \"custom\": \"https://customsource.org/schema/\",\n"
            + "    \"dcid\": \"https://datacommons.org/browser/\"\n"
            + "  },\n"
            + "  \"@graph\": [\n"
            + "    {\n"
            + "      \"@id\": \"dcid:TestObs\",\n"
            + "      \"@type\": \"dcid:StatVarObservation\",\n"
            + "      \"dcid:variableMeasured\": {\n"
            + "        \"@id\": \"custom:FinancialTrade\",\n"
            + "        \"dcid:observationProperties\": [\n"
            + "          { \"@id\": \"custom:destinationCountry\" },\n"
            + "          { \"@id\": \"custom:sourceCountry\" }\n"
            + "        ]\n"
            + "      },\n"
            + "      \"dcid:observationDate\": \"2024\",\n"
            + "      \"dcid:value\": 100,\n"
            + "      \"custom:sourceCountry\": { \"@id\": \"dcid:country/FRA\" },\n"
            + "      \"custom:destinationCountry\": { \"@id\": \"dcid:country/USA\" }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    InputStream in = new java.io.ByteArrayInputStream(jsonLd.getBytes(StandardCharsets.UTF_8));
    McfGraph graph = JsonLdParser.parse(in);

    assertNotNull(graph);
    assertTrue(graph.getNodesMap().containsKey("TestObs"));
    McfGraph.PropertyValues node = graph.getNodesMap().get("TestObs");

    assertTrue(node.containsPvs("variableMeasured"));
    boolean hasVariableMeasured = false;
    for (McfGraph.TypedValue tv : node.getPvsMap().get("variableMeasured").getTypedValuesList()) {
      if ("https://customsource.org/schema/FinancialTrade".equals(tv.getValue())) {
        hasVariableMeasured = true;
        break;
      }
    }
    assertTrue(hasVariableMeasured);

    assertTrue(node.containsPvs("observationProperties"));
    java.util.List<McfGraph.TypedValue> values =
        node.getPvsMap().get("observationProperties").getTypedValuesList();
    assertTrue(values.size() == 2);

    boolean foundDest = false;
    boolean foundSource = false;
    for (McfGraph.TypedValue tv : values) {
      if ("https://customsource.org/schema/destinationCountry".equals(tv.getValue())) {
        foundDest = true;
      }
      if ("https://customsource.org/schema/sourceCountry".equals(tv.getValue())) {
        foundSource = true;
      }
    }
    assertTrue(foundDest);
    assertTrue(foundSource);
  }

  @Test
  public void testNestedObservationProperties_StringList() throws Exception {
    String jsonLd =
        "{\n"
            + "  \"@context\": {\n"
            + "    \"custom\": \"https://customsource.org/schema/\",\n"
            + "    \"dcid\": \"https://datacommons.org/browser/\"\n"
            + "  },\n"
            + "  \"@graph\": [\n"
            + "    {\n"
            + "      \"@id\": \"dcid:TestObs\",\n"
            + "      \"@type\": \"dcid:StatVarObservation\",\n"
            + "      \"dcid:variableMeasured\": {\n"
            + "        \"@id\": \"custom:FinancialTrade\",\n"
            + "        \"dcid:observationProperties\": [\n"
            + "          \"custom:destinationCountry\",\n"
            + "          \"custom:sourceCountry\"\n"
            + "        ]\n"
            + "      },\n"
            + "      \"dcid:observationDate\": \"2024\",\n"
            + "      \"dcid:value\": 100,\n"
            + "      \"custom:sourceCountry\": { \"@id\": \"dcid:country/FRA\" },\n"
            + "      \"custom:destinationCountry\": { \"@id\": \"dcid:country/USA\" }\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    InputStream in = new java.io.ByteArrayInputStream(jsonLd.getBytes(StandardCharsets.UTF_8));
    McfGraph graph = JsonLdParser.parse(in);

    assertNotNull(graph);
    assertTrue(graph.getNodesMap().containsKey("TestObs"));
    McfGraph.PropertyValues node = graph.getNodesMap().get("TestObs");

    assertTrue(node.containsPvs("variableMeasured"));
    assertTrue(node.containsPvs("observationProperties"));
    java.util.List<McfGraph.TypedValue> values =
        node.getPvsMap().get("observationProperties").getTypedValuesList();
    assertTrue(values.size() == 2);

    boolean foundDest = false;
    boolean foundSource = false;
    for (McfGraph.TypedValue tv : values) {
      if ("custom:destinationCountry".equals(tv.getValue())
          || "https://customsource.org/schema/destinationCountry".equals(tv.getValue())) {
        foundDest = true;
      }
      if ("custom:sourceCountry".equals(tv.getValue())
          || "https://customsource.org/schema/sourceCountry".equals(tv.getValue())) {
        foundSource = true;
      }
    }
    assertTrue(foundDest);
    assertTrue(foundSource);
  }
}
