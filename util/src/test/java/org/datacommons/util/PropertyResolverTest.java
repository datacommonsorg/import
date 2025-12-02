package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static java.net.http.HttpClient.newHttpClient;
import static org.datacommons.util.TestUtil.newLogCtx;

import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;

public class PropertyResolverTest {

  private static final String SF_GEOID_DCID = "geoId/0667000";
  private static final String VZ_NUTS_DCID = "nuts/ITH35";
  private static final String IN_DCID = "country/IND";

  // San Francisco using wikidataId
  private static final PropertyValues SF = newNode("City", Map.of("wikidataId", "Q62"));
  // Venezia using nutsCode
  private static final PropertyValues VZ = newNode("Place", Map.of("nutsCode", "ITH35"));
  // India using isoCode
  private static final PropertyValues IN = newNode("Place", Map.of("isoCode", "IN"));
  // Unknown country using isoCode
  private static final PropertyValues UNK = newNode("Country", Map.of("isoCode", "ZZZ"));
  // Tamil Nadu / Karnataka using diverging IDs (isoCode and wikidataId)
  private static final PropertyValues DIVERGING_NODE =
      newNode("Place", Map.of("isoCode", "IN-KA", "wikidataId", "Q1445"));
  // Node with no resolvable properties
  private static final PropertyValues NON_RESOLVABLE_NODE =
      newNode("Place", Map.of("unsupportedProp", "someValue"));

  private static final List<PropertyValues> TEST_NODES =
      List.of(SF, VZ, IN, UNK, DIVERGING_NODE, NON_RESOLVABLE_NODE);

  @Test
  public void endToEnd() {
    PropertyResolver resolver =
        new PropertyResolver(new ReconClient(newHttpClient(), newLogCtx()), newLogCtx());

    // Submit nodes
    for (PropertyValues node : TEST_NODES) {
      resolver.submit(node);
    }

    resolver.drain();

    assertThat(resolver.resolve("sf", SF).get()).isEqualTo(SF_GEOID_DCID);
    assertThat(resolver.resolve("vz", VZ).get()).isEqualTo(VZ_NUTS_DCID);
    assertThat(resolver.resolve("in", IN).get()).isEqualTo(IN_DCID);
    assertThat(resolver.resolve("unk", UNK).isPresent()).isFalse();
    assertThat(resolver.resolve("diverging", DIVERGING_NODE).isPresent()).isFalse();
    assertThat(resolver.resolve("non-resolvable", NON_RESOLVABLE_NODE).isPresent()).isFalse();
  }

  @Test
  public void submitNode() {
    PropertyResolver resolver =
        new PropertyResolver(new ReconClient(newHttpClient(), newLogCtx()), newLogCtx());

    assertThat(resolver.submit(SF)).isTrue();
    assertThat(resolver.submit(VZ)).isTrue();
    assertThat(resolver.submit(IN)).isTrue();
    assertThat(resolver.submit(UNK)).isTrue();
    assertThat(resolver.submit(DIVERGING_NODE)).isTrue();
    assertThat(resolver.submit(NON_RESOLVABLE_NODE)).isFalse();
  }

  private static PropertyValues newNode(String typeOf, Map<String, String> props) {
    PropertyValues.Builder node = PropertyValues.newBuilder();
    node.putPvs(Vocabulary.TYPE_OF, McfUtil.newValues(ValueType.RESOLVED_REF, typeOf));
    for (Map.Entry<String, String> entry : props.entrySet()) {
      node.putPvs(entry.getKey(), McfUtil.newValues(ValueType.TEXT, entry.getValue()));
    }
    return node.build();
  }
}
