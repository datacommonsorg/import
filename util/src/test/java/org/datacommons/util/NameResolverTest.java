package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static java.net.http.HttpClient.newHttpClient;
import static org.datacommons.util.TestUtil.newLogCtx;
import static org.datacommons.util.Vocabulary.*;

import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;

public class NameResolverTest {
  private static final PropertyValues SF = newNode("City", Map.of(NAME, "San Francisco"));
  private static final String SF_GEOID_DCID = "geoId/0667000";

  private static final PropertyValues MTV = newNode("Place", Map.of(NAME, "Mountain View"));
  private static final String MTV_GEOID_DCID = "geoId/0649670";

  private static final PropertyValues NON_NAME_NODE = newNode("Place", Map.of("isoCode", "IN"));

  private static final List<PropertyValues> TEST_NODES = List.of(SF, MTV, NON_NAME_NODE);

  private static final PropertyValues UNSUBMITTED_NODE = newNode("City", Map.of(NAME, "Palo Alto"));

  @Test
  public void endToEnd() {
    NameResolver resolver = new NameResolver(new ReconClient(newHttpClient(), newLogCtx()));

    for (PropertyValues node : TEST_NODES) {
      resolver.submit(node);
    }

    resolver.drain();

    assertThat(resolver.resolve(SF)).hasValue(SF_GEOID_DCID);
    assertThat(resolver.resolve(MTV)).hasValue(MTV_GEOID_DCID);
    assertThat(resolver.resolve(UNSUBMITTED_NODE)).isEmpty();
  }

  @Test
  public void submitNode() {
    NameResolver resolver = new NameResolver(new ReconClient(newHttpClient(), newLogCtx()));

    assertThat(resolver.submit(SF)).isTrue();
    assertThat(resolver.submit(MTV)).isTrue();
    assertThat(resolver.submit(NON_NAME_NODE)).isFalse();
  }

  private static PropertyValues newNode(String typeOf, Map<String, String> props) {
    PropertyValues.Builder node = PropertyValues.newBuilder();
    node.putPvs(Vocabulary.TYPE_OF, McfUtil.newValues(ValueType.RESOLVED_REF, typeOf));
    for (var pv : props.entrySet()) {
      node.putPvs(pv.getKey(), McfUtil.newValues(ValueType.TEXT, pv.getValue()));
    }
    return node.build();
  }
}
