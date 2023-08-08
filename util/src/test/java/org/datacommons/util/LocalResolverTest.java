package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.datacommons.util.McfUtil.newValues;
import static org.datacommons.util.TestUtil.newLogCtx;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;

public class LocalResolverTest {
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
  public void withAllPhases() throws Exception {
    LogWrapper lw = newLogCtx();
    LocalResolver resolver = new LocalResolver(lw);

    List<PropertyValues> nodesWithoutDcid = List.of(in, sf, vz);

    for (PropertyValues node : nodesWithoutDcid) {
      // assert that test nodes without DCID are initially NOT resolvable by this resolver.
      assertThat(resolver.submitNode(node)).isFalse();
    }

    // Construct input side MCF where we also provide the DCIDs of the nodes
    var inWithDcid = addDcidToNode(in, inDcid);
    var sfWithDcid = addDcidToNode(sf, sfDcid);
    var vzWithDcid = addDcidToNode(vz, vzDcid);

    // Used for test where resolving an input node with diverging "external"
    // (loaded from local graph) throws an error
    var tamilNaduWithDcid =
        addDcidToNode(buildNode("Place", Map.of("isoCode", "IN-KA")), "wikidataId/Q1445");
    var karnatakaWithDcid =
        addDcidToNode(buildNode("Place", Map.of("wikidataId", "Q1445")), "wikidataId/Q1185");

    resolver.submitNode(inWithDcid);
    resolver.submitNode(sfWithDcid);
    resolver.submitNode(vzWithDcid);
    resolver.submitNode(tamilNaduWithDcid);
    resolver.submitNode(karnatakaWithDcid);

    for (PropertyValues node : nodesWithoutDcid) {
      // assert that the same nodes without DCID are now resolvable by this resolver.
      assertThat(resolver.submitNode(node)).isTrue();
    }

    resolver.resolveNodes().get();

    // India, SF and Venezia are mapped.
    assertThat(resolver.getResolvedNode(in)).hasValue(inDcid);

    // SF and Venezia get mapped.
    assertThat(resolver.getResolvedNode(sf)).hasValue(sfDcid);
    assertThat(resolver.getResolvedNode(vz)).hasValue(vzDcid);

    // CA and unknown are not mapped.
    assertThat(resolver.getResolvedNode(ca)).isEmpty();
    assertThat(resolver.getResolvedNode(unk)).isEmpty();
    assertThat(lw.getLog().getEntriesList()).isEmpty();

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

  // Given a node, returns a copy of the node with the given dcid added as a PV
  private static PropertyValues addDcidToNode(PropertyValues node, String dcid) {
    return PropertyValues.newBuilder(node)
        .putPvs("dcid", newValues(Mcf.ValueType.TEXT, dcid))
        .build();
  }
}
