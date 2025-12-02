package org.datacommons.util;

import static com.google.common.truth.Truth.assertThat;
import static org.datacommons.util.Vocabulary.LATITUDE;
import static org.datacommons.util.Vocabulary.LONGITUDE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.ExternalIdResolver.ApiVersion;
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

  McfGraph.PropertyValues bigBenWithLatLng =
      buildNode("Place", Map.of(LATITUDE, "51.510357", LONGITUDE, "-0.116773"));
  String bigBenDcid = "nuts/UKI32";

  List<McfGraph.PropertyValues> testPlaceNodesPlusLatLngNodes =
      ImmutableList.<McfGraph.PropertyValues>builder()
          .addAll(testPlaceNodes)
          .add(bigBenWithLatLng)
          .build();

  @Test
  public void endToEndWithApiCalls() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), false, true, lw, ApiVersion.V1);
    for (var node : testPlaceNodes) {
      resolver.submitNode(node);
    }
    // Issue 20 more SF calls, which should all be batched.
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    // There are 7 IDs, and batch-size if 4, so we must have done 2 calls.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumDcCalls", 2));
  }

  @Test
  public void endToEndWithLocalSideMcf() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS =
        1; // This allows us to count the number of DC calls exactly
    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), false, true, lw, ApiVersion.V1);

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

    resolver.addLocalGraph(inWithDcid);
    resolver.addLocalGraph(sfWithDcid);
    resolver.addLocalGraph(vzWithDcid);
    resolver.addLocalGraph(tamilNaduWithDcid);
    resolver.addLocalGraph(karnatakaWithDcid);

    for (var node : testPlaceNodes) {
      resolver.submitNode(node);
    }
    // Issue 20 more SF calls, which should all be caught in local memory.
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }

    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    // There should have been exactly one API call for the node "unk" for which no
    // local data was available
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumDcCalls", 1));
  }

  @Test
  public void endToEndWithApiCalls_withLatLngNodes_withCoordinatesResolutionDisabled()
      throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), false, true, lw, ApiVersion.V1);
    for (var node : testPlaceNodesPlusLatLngNodes) {
      resolver.submitNode(node);
    }
    // Issue 20 more SF calls, which should all be batched.
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    // There are 7 IDs, and batch-size if 4, so we must have done 2 calls.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumDcCalls", 2));

    // Coordinates assertions.

    // There is 1 lat-lng node but coordinates resolution is disabled so there should be no counters
    // (represented as -1).
    assertThat(TestUtil.getCounter(lw.getLog(), ReconClient.NUM_API_CALLS_COUNTER)).isEqualTo(-1);
    // Since coordinates resolution is disabled, this won't resolve.
    assertEquals("", resolver.resolveNode("bigben", bigBenWithLatLng));
  }

  @Test
  public void endToEndWithApiCalls_withLatLngNodes_withCoordinatesResolutionEnabled()
      throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), true, true, lw, ApiVersion.V1);
    for (var node : testPlaceNodesPlusLatLngNodes) {
      resolver.submitNode(node);
    }
    // Issue 20 more SF calls, which should all be batched.
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    // There are 7 IDs, and batch-size if 4, so we must have done 2 calls.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumDcCalls", 2));

    // Coordinates assertions.

    // There is 1 lat-lng node so there should be 1 DC call.
    assertThat(TestUtil.getCounter(lw.getLog(), ReconClient.NUM_API_CALLS_COUNTER)).isEqualTo(1);
    // big ben with lat lng should be resolved.
    assertEquals(bigBenDcid, resolver.resolveNode("bigben", bigBenWithLatLng));
  }

  @Test
  public void endToEndWithApiCalls_v2() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), false, true, lw, ApiVersion.V2);
    for (var node : testPlaceNodes) {
      resolver.submitNode(node);
    }
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumResolveV2Calls", 5));
    assertThat(TestUtil.getCounter(lw.getLog(), "Resolution_NumDcCalls")).isEqualTo(-1);
  }

  @Test
  public void endToEndWithLocalSideMcf_v2() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 1;
    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), false, true, lw, ApiVersion.V2);

    var inWithDcid = addDcidToNode(in, inDcid);
    var sfWithDcid = addDcidToNode(sf, sfDcid);
    var vzWithDcid = addDcidToNode(vz, vzDcid);

    var tamilNaduWithDcid =
        addDcidToNode(buildNode("Place", Map.of("isoCode", "IN-KA")), "wikidataId/Q1445");
    var karnatakaWithDcid =
        addDcidToNode(buildNode("Place", Map.of("wikidataId", "Q1445")), "wikidataId/Q1185");

    resolver.addLocalGraph(inWithDcid);
    resolver.addLocalGraph(sfWithDcid);
    resolver.addLocalGraph(vzWithDcid);
    resolver.addLocalGraph(tamilNaduWithDcid);
    resolver.addLocalGraph(karnatakaWithDcid);

    for (var node : testPlaceNodes) {
      resolver.submitNode(node);
    }
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }

    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumResolveV2Calls", 1));
    assertThat(TestUtil.getCounter(lw.getLog(), "Resolution_NumDcCalls")).isEqualTo(-1);
  }

  @Test
  public void endToEndWithApiCalls_withLatLngNodes_withCoordinatesResolutionDisabled_v2()
      throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), false, true, lw, ApiVersion.V2);
    for (var node : testPlaceNodesPlusLatLngNodes) {
      resolver.submitNode(node);
    }
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumResolveV2Calls", 5));
    assertThat(TestUtil.getCounter(lw.getLog(), "Resolution_NumDcCalls")).isEqualTo(-1);

    // In v2, ReconClient counter includes all external-ID resolve calls; with coords disabled we
    // expect 5 external-ID calls and 0 coordinate calls.
    assertThat(TestUtil.getCounter(lw.getLog(), ReconClient.NUM_API_CALLS_COUNTER)).isEqualTo(5);
    assertEquals("", resolver.resolveNode("bigben", bigBenWithLatLng));
  }

  @Test
  public void endToEndWithApiCalls_withLatLngNodes_withCoordinatesResolutionEnabled_v2()
      throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExternalIdResolver.MAX_RESOLUTION_BATCH_IDS = 4;

    var resolver =
        new ExternalIdResolver(HttpClient.newHttpClient(), true, true, lw, ApiVersion.V2);
    for (var node : testPlaceNodesPlusLatLngNodes) {
      resolver.submitNode(node);
    }
    for (int i = 0; i < 20; i++) {
      resolver.submitNode(sf);
    }
    resolver.drainRemoteCalls();

    testAssertionSuiteOnResolverInstance(resolver, lw);

    assertTrue(TestUtil.checkCounter(lw.getLog(), "Resolution_NumResolveV2Calls", 5));
    assertThat(TestUtil.getCounter(lw.getLog(), "Resolution_NumDcCalls")).isEqualTo(-1);

    // In v2, ReconClient counter includes both external-ID and coordinate resolve calls; here we
    // expect 5 external-ID calls plus 1 coordinate call.
    assertThat(TestUtil.getCounter(lw.getLog(), ReconClient.NUM_API_CALLS_COUNTER)).isEqualTo(6);
    assertEquals(bigBenDcid, resolver.resolveNode("bigben", bigBenWithLatLng));
  }

  // Runs assertions on the place constants as defined in the class constants.
  // These assertions are factored out of individual tests to allow testing different
  // input methods (API, addLocalGraph) have the same local behavior with the same input
  // Does NOT test I/O related assertions, which are left to the individual test functions.
  private void testAssertionSuiteOnResolverInstance(ExternalIdResolver resolver, LogWrapper lw)
      throws IOException, InterruptedException {
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
