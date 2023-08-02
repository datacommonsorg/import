package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static java.net.http.HttpClient.newHttpClient;
import static org.datacommons.util.Vocabulary.LATITUDE;
import static org.datacommons.util.Vocabulary.LONGITUDE;

import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;

public class CoordinatesResolverTest {
  private static final PropertyValues SF =
      newNode("City", Map.of(LATITUDE, "37.77493", LONGITUDE, "-122.41942"));
  private static final String SF_ZIP_DCID = "zip/94103";

  private static final PropertyValues BIG_BEN =
      newNode("Place", Map.of(LATITUDE, "51.510357", LONGITUDE, "-0.116773"));
  private static final String BIG_BEN_NUTS_DCID = "nuts/UKI32";

  private static final PropertyValues UNSUBMITTED_NODE =
      newNode("City", Map.of(LATITUDE, "12.34", LONGITUDE, "56.78"));

  private static final List<PropertyValues> TEST_NODES = List.of(SF, BIG_BEN);

  @Test
  public void endToEnd() throws Exception {
    CoordinatesResolver resolver = new CoordinatesResolver(new ReconClient(newHttpClient()));

    assertThat(resolver.isResolved()).isFalse();

    for (PropertyValues node : TEST_NODES) {
      resolver.submitNode(node);
    }

    assertThat(resolver.isResolved()).isFalse();

    resolver.resolve().get();

    assertThat(resolver.isResolved()).isTrue();

    assertThat(resolver.getResolvedNode(SF)).isEqualTo(SF_ZIP_DCID);
    assertThat(resolver.getResolvedNode(BIG_BEN)).isEqualTo(BIG_BEN_NUTS_DCID);
    assertThat(resolver.getResolvedNode(UNSUBMITTED_NODE)).isEmpty();
  }

  @Test
  public void endToEnd_chunked() throws Exception {
    CoordinatesResolver resolver = new CoordinatesResolver(new ReconClient(newHttpClient()), 1);

    assertThat(resolver.isResolved()).isFalse();

    for (PropertyValues node : TEST_NODES) {
      resolver.submitNode(node);
    }

    assertThat(resolver.isResolved()).isFalse();

    resolver.resolve().get();

    assertThat(resolver.isResolved()).isTrue();

    assertThat(resolver.getResolvedNode(SF)).isEqualTo(SF_ZIP_DCID);
    assertThat(resolver.getResolvedNode(BIG_BEN)).isEqualTo(BIG_BEN_NUTS_DCID);
    assertThat(resolver.getResolvedNode(UNSUBMITTED_NODE)).isEmpty();
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
