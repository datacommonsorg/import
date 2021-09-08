package org.datacommons.util;

import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.junit.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExternalIdResolverTest {
  @Test
  public void endToEnd() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    // This includes 7 external IDs.
    // India using isoCode
    var in = buildNode("Place", Map.of("isoCode", "IN"));
    // CA, but the type is not a valid place type.
    var ca = buildNode("USState", Map.of("geoId", "06"));
    // SF using wikidataId
    var sf = buildNode("City", Map.of("wikidataId", "Q62"));
    // Venezia using nuts
    var vz = buildNode("Place", Map.of("nutsCode", "ITH35"));
    // Unknown country
    var unk = buildNode("Country", Map.of("isoCode", "ZZZ"));
    // Tamil Nadu / Karnataka using diverging IDs
    var tn = buildNode("Place", Map.of("isoCode", "IN-KA", "wikidataId", "Q1445"));

    var resolver = new ExternalIdResolver(HttpClient.newHttpClient(), true, lw);
    for (var node : List.of(in, ca, sf, vz, unk, tn)) {
      resolver.submitNode(node);
    }
    // Issue 20 more SF calls, which should all be batched.
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();
    assertEquals("country/IND", resolver.resolveNode("in", in));

    // CA type is not valid. So its not an error, but we won't resolve.
    assertEquals("", resolver.resolveNode("ca", ca));
    assertTrue(lb.build().getEntriesList().isEmpty());

    // SF gets mapped.
    assertEquals("geoId/0667000", resolver.resolveNode("sf", sf));
    assertEquals("nuts/ITH35", resolver.resolveNode("vz", vz));

    // This cannot be resolved.
    assertEquals("", resolver.resolveNode("unk", unk));
    assertTrue(
        TestUtil.checkLog(
            lb.build(),
            "Resolution_UnresolvedExternalId_isoCode",
            "Unresolved external ID :: id: 'ZZZ'"));

    // We provided external IDs that map to diverging DCIDs.
    assertEquals("", resolver.resolveNode("tn", tn));
    assertTrue(
        TestUtil.checkLog(
            lb.build(),
            "Resolution_DivergingDcidsForExternalIds_isoCode_wikidataId",
            "Found diverging DCIDs for external IDs"));

    // There are 7 IDs, and batch-size if 4, so we must have done 2 calls.
    assertTrue(TestUtil.checkCounter(lb.build(), "Resolution_NumDcCalls", 2));
  }

  Mcf.McfGraph.PropertyValues buildNode(String typeOf, Map<String, String> extIds) {
    Mcf.McfGraph.PropertyValues.Builder node = Mcf.McfGraph.PropertyValues.newBuilder();
    node.putPvs(Vocabulary.TYPE_OF, McfUtil.newValues(Mcf.ValueType.RESOLVED_REF, typeOf));
    for (var pv : extIds.entrySet()) {
      node.putPvs(pv.getKey(), McfUtil.newValues(Mcf.ValueType.TEXT, pv.getValue()));
    }
    return node.build();
  }
}
