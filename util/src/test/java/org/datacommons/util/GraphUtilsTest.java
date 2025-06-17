package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
import org.datacommons.proto.Mcf.McfType;
import org.datacommons.proto.Mcf.OptimizedMcfGraph;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;

public class GraphUtilsTest {

  private McfGraph.PropertyValues createSVObsNode(
      String dcid,
      String obsAbout,
      String varMeasured,
      String obsDate,
      String value,
      ValueType valueType,
      String obsPeriod,
      String measurementMethod,
      String unit,
      String scalingFactor,
      Map<String, String> otherPvs) {

    McfGraph.PropertyValues.Builder nodeBuilder = McfGraph.PropertyValues.newBuilder();
    nodeBuilder.putPvs(
        Vocabulary.TYPE_OF,
        McfUtil.newValues(ValueType.RESOLVED_REF, Vocabulary.STAT_VAR_OBSERVATION_TYPE));
    nodeBuilder.putPvs(Vocabulary.DCID, McfUtil.newValues(ValueType.TEXT, dcid));
    nodeBuilder.putPvs(
        Vocabulary.OBSERVATION_ABOUT, McfUtil.newValues(ValueType.RESOLVED_REF, obsAbout));
    nodeBuilder.putPvs(
        Vocabulary.VARIABLE_MEASURED, McfUtil.newValues(ValueType.RESOLVED_REF, varMeasured));
    nodeBuilder.putPvs(Vocabulary.OBSERVATION_DATE, McfUtil.newValues(ValueType.TEXT, obsDate));
    nodeBuilder.putPvs(Vocabulary.VALUE, McfUtil.newValues(valueType, value));

    if (obsPeriod != null) {
      nodeBuilder.putPvs(
          Vocabulary.OBSERVATION_PERIOD, McfUtil.newValues(ValueType.TEXT, obsPeriod));
    }
    if (measurementMethod != null) {
      nodeBuilder.putPvs(
          Vocabulary.MEASUREMENT_METHOD,
          McfUtil.newValues(ValueType.RESOLVED_REF, measurementMethod));
    }
    if (unit != null) {
      nodeBuilder.putPvs(Vocabulary.UNIT, McfUtil.newValues(ValueType.RESOLVED_REF, unit));
    }
    if (scalingFactor != null) {
      nodeBuilder.putPvs(
          Vocabulary.SCALING_FACTOR, McfUtil.newValues(ValueType.TEXT, scalingFactor));
    }

    if (otherPvs != null) {
      for (Map.Entry<String, String> entry : otherPvs.entrySet()) {
        nodeBuilder.putPvs(entry.getKey(), McfUtil.newValues(ValueType.TEXT, entry.getValue()));
      }
    }
    return nodeBuilder.build();
  }

  @Test
  public void testBuildOptimizedMcfGraph_singleObservation() {
    McfGraph.PropertyValues svoNode =
        createSVObsNode(
            "obs1",
            "dcid:placeA",
            "dcid:svX",
            "2023-01-15",
            "100.5",
            ValueType.NUMBER,
            "P1M",
            "dcid:methodA",
            "dcid:unitX",
            "1",
            Map.of("description", "A test observation"));

    McfGraph.Builder graphBuilder = McfGraph.newBuilder().setType(McfType.INSTANCE_MCF);
    graphBuilder.putNodes("l:node1", svoNode);
    McfGraph mcfGraph = graphBuilder.build();

    OptimizedMcfGraph result = GraphUtils.buildOptimizedMcfGraph(List.of(mcfGraph));

    assertNotNull(result);
    assertEquals(1, result.getGraphCount());

    McfOptimizedGraph optimizedEntry = result.getGraph(0);
    McfStatVarObsSeries series = optimizedEntry.getSvObsSeries();

    McfStatVarObsSeries.Key expectedKey =
        McfStatVarObsSeries.Key.newBuilder()
            .setObservationAbout("placeA")
            .setVariableMeasured("svX")
            .setMeasurementMethod("methodA")
            .setObservationPeriod("P1M")
            .setUnit("unitX")
            .setScalingFactor("1")
            .build();
    assertEquals(expectedKey, series.getKey());

    assertEquals(1, series.getSvObsListCount());
    McfStatVarObsSeries.StatVarObs obs = series.getSvObsList(0);
    assertEquals("2023-01-15", obs.getDate());
    assertEquals("obs1", obs.getDcid());
    assertEquals(100.5, obs.getNumber(), 0.001);
    assertEquals("A test observation", GraphUtils.getPropVal(obs.getPvs(), "description"));
  }

  @Test
  public void testBuildOptimizedMcfGraph_multipleObservations_sameKey() {
    McfGraph.PropertyValues svoNode1 =
        createSVObsNode(
            "obs1",
            "dcid:placeA",
            "dcid:svX",
            "2023-01-15",
            "100.5",
            ValueType.NUMBER,
            "P1M",
            "dcid:methodA",
            "dcid:unitX",
            "1",
            Map.of("description", "Observation 1"));
    McfGraph.PropertyValues svoNode2 =
        createSVObsNode(
            "obs2",
            "dcid:placeA",
            "dcid:svX",
            "2023-02-15",
            "102.0",
            ValueType.NUMBER,
            "P1M",
            "dcid:methodA",
            "dcid:unitX",
            "1",
            Map.of("description", "Observation 2"));

    McfGraph.Builder graphBuilder = McfGraph.newBuilder().setType(McfType.INSTANCE_MCF);
    graphBuilder.putNodes("l:node1", svoNode1);
    graphBuilder.putNodes("l:node2", svoNode2);
    McfGraph mcfGraph = graphBuilder.build();

    OptimizedMcfGraph result = GraphUtils.buildOptimizedMcfGraph(List.of(mcfGraph));

    assertNotNull(result);
    assertEquals(1, result.getGraphCount());

    McfOptimizedGraph optimizedEntry = result.getGraph(0);
    McfStatVarObsSeries series = optimizedEntry.getSvObsSeries();

    McfStatVarObsSeries.Key expectedKey =
        McfStatVarObsSeries.Key.newBuilder()
            .setObservationAbout("placeA")
            .setVariableMeasured("svX")
            .setMeasurementMethod("methodA")
            .setObservationPeriod("P1M")
            .setUnit("unitX")
            .setScalingFactor("1")
            .build();
    assertEquals(expectedKey, series.getKey());

    assertEquals(2, series.getSvObsListCount());

    McfStatVarObsSeries.StatVarObs obs1 = series.getSvObsList(0);
    assertEquals("2023-01-15", obs1.getDate());
    assertEquals("obs1", obs1.getDcid());
    assertEquals(100.5, obs1.getNumber(), 0.001);

    McfStatVarObsSeries.StatVarObs obs2 = series.getSvObsList(1);
    assertEquals("2023-02-15", obs2.getDate());
    assertEquals("obs2", obs2.getDcid());
    assertEquals(102.0, obs2.getNumber(), 0.001);
  }

  @Test
  public void testBuildOptimizedMcfGraph_multipleObservations_differentKeys() {
    McfGraph.PropertyValues svoNode1 =
        createSVObsNode(
            "obs1",
            "dcid:placeA",
            "dcid:svX",
            "2023-01-15",
            "100.5",
            ValueType.NUMBER,
            "P1M",
            "dcid:methodA",
            "dcid:unitX",
            "1",
            null);
    McfGraph.PropertyValues svoNode2 =
        createSVObsNode( // Different variableMeasured
            "obs2",
            "dcid:placeA",
            "dcid:svY",
            "2023-01-15",
            "200.0",
            ValueType.NUMBER,
            "P1M",
            "dcid:methodA",
            "dcid:unitX",
            "1",
            null);

    McfGraph.Builder graphBuilder = McfGraph.newBuilder().setType(McfType.INSTANCE_MCF);
    graphBuilder.putNodes("l:node1", svoNode1);
    graphBuilder.putNodes("l:node2", svoNode2);
    McfGraph mcfGraph = graphBuilder.build();

    OptimizedMcfGraph result = GraphUtils.buildOptimizedMcfGraph(List.of(mcfGraph));

    assertNotNull(result);
    assertEquals(2, result.getGraphCount());
  }
}
