package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.datacommons.util.McfUtil.newValues;
import static org.datacommons.util.TestUtil.newLogCtx;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;

public class EntityResolverTest {
  // This includes 6 nodes with 7 external IDs.

  // India using isoCode
  PropertyValues in = buildNode("Place", Map.of("isoCode", "IN"));
  String inDcid = "country/IND";
  // CA, but the type is not a valid place type.
  PropertyValues ca = buildNode("USState", Map.of("geoId", "06"));
  String caDcid = "geoId/06";
  // SF using wikidataId
  PropertyValues sf = buildNode("City", Map.of("wikidataId", "Q62"));
  String sfDcid = "geoId/0667000";
  // Venezia using nuts
  PropertyValues vz = buildNode("Place", Map.of("nutsCode", "ITH35"));
  String vzDcid = "nuts/ITH35";
  // Unknown country
  PropertyValues unk = buildNode("Country", Map.of("isoCode", "ZZZ"));
  // Tamil Nadu / Karnataka using diverging IDs
  PropertyValues tn = buildNode("Place", Map.of("isoCode", "IN-KA", "wikidataId", "Q1445"));

  List<PropertyValues> testPlaceNodes = List.of(in, ca, sf, vz, unk, tn);

  @Test
  public void endToEndWithApiCalls() throws Exception {
    LogWrapper lw = newLogCtx();

    EntityResolver resolver =
        new EntityResolver(new ReconClient(HttpClient.newHttpClient(), lw), lw);
    for (PropertyValues node : testPlaceNodes) {
      resolver.submitNode(node);
    }

    // Submit 20 more SF nodes. This should result in NO more external resolutions since the
    // entities are maintained in a Set.
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }

    resolver.resolveNodes().get();

    testAssertionSuiteOnResolverInstance(resolver, lw);
  }

  // Runs assertions on the place constants as defined in the class constants.
  // These assertions are factored out of individual tests to allow testing different
  // input methods (API, addLocalGraph) have the same local behavior with the same input
  // Does NOT test I/O related assertions, which are left to the individual test functions.
  private void testAssertionSuiteOnResolverInstance(EntityResolver resolver, LogWrapper lw)
      throws IOException, InterruptedException {
    assertThat(resolver.getResolvedNode(in)).hasValue(inDcid);

    // CA type is not valid. At this level, the resolver does not care about the type, so it will
    // resolve.
    // However, when resolving from the resolver controller (i.e. ExternalIdController), it will not
    // resolve.
    assertThat(resolver.getResolvedNode(ca)).hasValue(caDcid);
    assertThat(lw.getLog().getEntriesList()).isEmpty();

    // SF and Venezia get mapped.
    assertThat(resolver.getResolvedNode(sf)).hasValue(sfDcid);
    assertThat(resolver.getResolvedNode(vz)).hasValue(vzDcid);

    assertThat(resolver.getResolvedNode(unk)).isEmpty();

    // We provided external IDs that map to diverging DCIDs.
    assertThat(resolver.getResolvedNode(tn)).isEmpty();
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(), "Resolution_DivergingDcidsForExternalIds", "Divergence found."));
  }

  PropertyValues buildNode(String typeOf, Map<String, String> extIds) {
    PropertyValues.Builder node = PropertyValues.newBuilder();
    node.putPvs(Vocabulary.TYPE_OF, newValues(ValueType.RESOLVED_REF, typeOf));
    for (Map.Entry<String, String> pv : extIds.entrySet()) {
      node.putPvs(pv.getKey(), newValues(ValueType.TEXT, pv.getValue()));
    }
    return node.build();
  }
}
