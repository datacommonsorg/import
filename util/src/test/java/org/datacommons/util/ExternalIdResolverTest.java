package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;
import org.junit.Test;

public class ExternalIdResolverTest {

  // This includes 7 external IDs.
  // India using isoCode
  McfGraph.PropertyValues in = buildNode("Place", Map.of("isoCode", "IN"));
  String inDcid = "country/IND";
  // CA, but the type is not a valid place type.
  McfGraph.PropertyValues ca = buildNode("USState", Map.of("geoId", "06"));
  // SF using wikidataId
  McfGraph.PropertyValues sf = buildNode("City", Map.of("wikidataId", "Q62"));
  String sfDcid = "geoId/0667000";
  // Venezia using nuts
  McfGraph.PropertyValues vz = buildNode("Place", Map.of("nutsCode", "ITH35"));
  String vzDcid = "nuts/ITH35";
  // Unknown country
  McfGraph.PropertyValues unk = buildNode("Country", Map.of("isoCode", "ZZZ"));
  // Tamil Nadu / Karnataka using diverging IDs
  McfGraph.PropertyValues tn =
      buildNode("Place", Map.of("isoCode", "IN-KA", "wikidataId", "Q1445"));

  List<McfGraph.PropertyValues> testPlaceNodes = List.of(in, ca, sf, vz, unk, tn);

  @Test
  public void endToEnd() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    var resolver = new ExternalIdResolver(HttpClient.newHttpClient(), true, lw);
    for (var node : testPlaceNodes) {
      resolver.submitNode(node);
    }
    // Issue 20 more SF calls, which should all be batched.
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();
    assertEquals(inDcid, resolver.resolveNode("in", in));

    // CA type is not valid. So its not an error, but we won't resolve.
    assertEquals("", resolver.resolveNode("ca", ca));
    assertTrue(lw.getLog().getEntriesList().isEmpty());

    // SF and Venezia get mapped.
    assertEquals(sfDcid, resolver.resolveNode("sf", sf));
    assertEquals(vzDcid, resolver.resolveNode("vz", vz));

    // This cannot be resolved.
    assertEquals("", resolver.resolveNode("unk", unk));
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(),
            "Resolution_UnresolvedExternalId_isoCode",
            "Unresolved external ID :: id: 'ZZZ'"));

    // We provided external IDs that map to diverging DCIDs.
    assertEquals("", resolver.resolveNode("tn", tn));
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(),
            "Resolution_DivergingDcidsForExternalIds_isoCode_wikidataId",
            "Found diverging DCIDs for external IDs"));

    // There are 7 IDs, and batch-size if 4, so we must have done 2 calls.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumDcCalls", 2));
  }

  @Test
  public void testLocalGraph() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));

    var resolver = new ExternalIdResolver(null, true, lw);

    // construct input side MCF where we also provide the DCIDs of the nodes
    var inWithDcid = addDcidToNode(in, inDcid);
    var sfWithDcid = addDcidToNode(sf, sfDcid);
    var vzWithDcid = addDcidToNode(vz, vzDcid);

    resolver.addLocalGraph(inWithDcid);
    resolver.addLocalGraph(sfWithDcid);
    resolver.addLocalGraph(vzWithDcid);

    resolver.drainRemoteCalls();

    assertEquals(inDcid, resolver.resolveNode("in", in));
    assertEquals(sfDcid, resolver.resolveNode("sf", sf));
    assertEquals(vzDcid, resolver.resolveNode("vz", vz));

    /* TODO(snny): look at the rest of this and see which tests should be
    uncommented for local tests.


    // CA type is not valid. So its not an error, but we won't resolve.
    assertEquals("", resolver.resolveNode("ca", ca));
    assertTrue(lw.getLog().getEntriesList().isEmpty());

    // SF gets mapped.
    assertEquals("geoId/0667000", resolver.resolveNode("sf", sf));
    assertEquals("nuts/ITH35", resolver.resolveNode("vz", vz));

    // This cannot be resolved.
    assertEquals("", resolver.resolveNode("unk", unk));
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(),
            "Resolution_UnresolvedExternalId_isoCode",
            "Unresolved external ID :: id: 'ZZZ'"));

    // We provided external IDs that map to diverging DCIDs.
    assertEquals("", resolver.resolveNode("tn", tn));
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(),
            "Resolution_DivergingDcidsForExternalIds_isoCode_wikidataId",
            "Found diverging DCIDs for external IDs"));

    // There are 7 IDs, and batch-size if 4, so we must have done 2 calls.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumDcCalls", 2));
    */
  }

  Mcf.McfGraph.PropertyValues buildNode(String typeOf, Map<String, String> extIds) {
    Mcf.McfGraph.PropertyValues.Builder node = Mcf.McfGraph.PropertyValues.newBuilder();
    node.putPvs(Vocabulary.TYPE_OF, McfUtil.newValues(Mcf.ValueType.RESOLVED_REF, typeOf));
    for (var pv : extIds.entrySet()) {
      node.putPvs(pv.getKey(), McfUtil.newValues(Mcf.ValueType.TEXT, pv.getValue()));
    }
    return node.build();
  }

  // Given a node, returns a copy of the node with the given dcid added as a PV
  Mcf.McfGraph.PropertyValues addDcidToNode(Mcf.McfGraph.PropertyValues node, String dcid) {
    Mcf.McfGraph.PropertyValues.Builder nodeWithDcidBuilder =
        Mcf.McfGraph.PropertyValues.newBuilder(node);
    nodeWithDcidBuilder.putPvs("dcid", McfUtil.newValues(Mcf.ValueType.TEXT, dcid));
    Mcf.McfGraph.PropertyValues nodeWithDcid = nodeWithDcidBuilder.build();

    return nodeWithDcid;
  }
}
