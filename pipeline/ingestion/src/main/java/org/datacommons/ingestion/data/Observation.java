package org.datacommons.ingestion.data;

import com.google.common.base.Joiner;
import java.util.List;
import java.util.Objects;
import org.datacommons.pipeline.util.PipelineUtils;
import org.datacommons.proto.Storage.Observations;

/**
 * Models a statvar observation time series.
 *
 * <p>This class is used to store the result of parsing a time series row in memory. It is not
 * inserted into the pipeline.
 */
public class Observation {
  private static final String OBS_SERIES_DCID_PREFIX = "dc/os/";
  private static final String OBS_SERIES_TYPE = "StatVarObsSeries";
  private static final String VARIABLE_MEASURED_PREDICATE = "variableMeasured";
  private static final String OBSERVATION_ABOUT_PREDICATE = "observationAbout";
  private static final String NAME_PREDICATE = "name";
  private static final String TYPE_OF_PREDICATE = "typeOf";
  private static final String PROVENANCE_DCID_PREFIX = "dc/base/";

  private String variableMeasured;
  private String observationAbout;
  private Observations observations;
  private String observationPeriod;
  private String measurementMethod;
  private String unit;
  private String scalingFactor;
  private String importName;
  private String provenanceUrl;
  private String facetId;
  private boolean isDcAggregate;
  private NodesEdges obsGraph;

  private Observation(Builder builder) {
    this.variableMeasured = builder.variableMeasured;
    this.observationAbout = builder.observationAbout;
    this.observations = builder.observations.build();
    this.observationPeriod = builder.observationPeriod;
    this.measurementMethod = builder.measurementMethod;
    this.unit = builder.unit;
    this.scalingFactor = builder.scalingFactor;
    this.importName = builder.importName;
    this.provenanceUrl = builder.provenanceUrl;
    this.facetId = builder.facetId;
    this.isDcAggregate = builder.isDcAggregate;
    this.obsGraph = toObsGraph();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getVariableMeasured() {
    return variableMeasured;
  }

  public String getObservationAbout() {
    return observationAbout;
  }

  public Observations getObservations() {
    return observations;
  }

  public String getObservationPeriod() {
    return observationPeriod;
  }

  public String getMeasurementMethod() {
    return measurementMethod;
  }

  public String getUnit() {
    return unit;
  }

  public String getScalingFactor() {
    return scalingFactor;
  }

  public String getImportName() {
    return importName;
  }

  public String getProvenanceUrl() {
    return provenanceUrl;
  }

  public String getFacetId() {
    return facetId;
  }

  public boolean getIsDcAggregate() {
    return isDcAggregate;
  }

  public NodesEdges getObsGraph() {
    return obsGraph;
  }

  private NodesEdges toObsGraph() {
    var graph = new NodesEdges();
    var seriesDcid =
        OBS_SERIES_DCID_PREFIX
            + Joiner.on("_")
                .join(
                    replaceSlashesWithUnderscores(variableMeasured),
                    replaceSlashesWithUnderscores(observationAbout),
                    facetId);
    var seriesName = Joiner.on(" | ").join(variableMeasured, observationAbout, facetId);
    var provenanceDcid = PROVENANCE_DCID_PREFIX + importName;

    // Add series node
    graph.addNode(
        Node.builder()
            .subjectId(seriesDcid)
            .value(seriesDcid)
            .reference(true)
            .name(seriesName)
            .types(List.of(OBS_SERIES_TYPE))
            .build());

    // Add leaf node
    graph.addNode(
        Node.builder()
            .subjectId(PipelineUtils.generateSha256(seriesName))
            .value(seriesName)
            .build());

    // Add variableMeasured edge
    graph.addEdge(
        Edge.builder()
            .subjectId(seriesDcid)
            .predicate(VARIABLE_MEASURED_PREDICATE)
            .objectId(variableMeasured)
            .provenance(provenanceDcid)
            .build());
    // Add observationAbout edge
    graph.addEdge(
        Edge.builder()
            .subjectId(seriesDcid)
            .predicate(OBSERVATION_ABOUT_PREDICATE)
            .objectId(observationAbout)
            .provenance(provenanceDcid)
            .build());
    // Add name edge
    graph.addEdge(
        Edge.builder()
            .subjectId(seriesDcid)
            .predicate(NAME_PREDICATE)
            .objectId(PipelineUtils.generateSha256(seriesName))
            .provenance(provenanceDcid)
            .build());
    // Add typeOf edge
    graph.addEdge(
        Edge.builder()
            .subjectId(seriesDcid)
            .predicate(TYPE_OF_PREDICATE)
            .objectId(OBS_SERIES_TYPE)
            .provenance(provenanceDcid)
            .build());

    return graph;
  }

  private static String replaceSlashesWithUnderscores(String s) {
    return s == null ? null : s.replace('/', '_');
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Observation that = (Observation) o;
    return Objects.equals(variableMeasured, that.variableMeasured)
        && Objects.equals(observationAbout, that.observationAbout)
        && Objects.equals(observations, that.observations)
        && Objects.equals(observationPeriod, that.observationPeriod)
        && Objects.equals(measurementMethod, that.measurementMethod)
        && Objects.equals(unit, that.unit)
        && Objects.equals(scalingFactor, that.scalingFactor)
        && Objects.equals(importName, that.importName)
        && Objects.equals(provenanceUrl, that.provenanceUrl)
        && Objects.equals(facetId, that.facetId)
        && Objects.equals(isDcAggregate, that.isDcAggregate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        variableMeasured,
        observationAbout,
        observations,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        importName,
        provenanceUrl,
        facetId,
        isDcAggregate);
  }

  // Builder for Observation
  public static class Builder {
    private String variableMeasured = "";
    private String observationAbout = "";
    private Observations.Builder observations = Observations.newBuilder();
    private String observationPeriod = "";
    private String measurementMethod = "";
    private String unit = "";
    private String scalingFactor = "";
    private String importName = "";
    private String provenanceUrl = "";
    private String facetId = "";
    private boolean isDcAggregate = false;

    public Builder variableMeasured(String variableMeasured) {
      this.variableMeasured = variableMeasured;
      return this;
    }

    public Builder observationAbout(String observationAbout) {
      this.observationAbout = observationAbout;
      return this;
    }

    public Builder observation(String date, String value) {
      this.observations.putValues(date, value);
      return this;
    }

    public Builder observations(Observations observations) {
      this.observations = observations.toBuilder();
      return this;
    }

    public Builder observationPeriod(String observationPeriod) {
      this.observationPeriod = observationPeriod;
      return this;
    }

    public Builder measurementMethod(String measurementMethod) {
      this.measurementMethod = measurementMethod;
      return this;
    }

    public Builder unit(String unit) {
      this.unit = unit;
      return this;
    }

    public Builder isDcAggregate(boolean isDcAggregate) {
      this.isDcAggregate = isDcAggregate;
      return this;
    }

    public Builder scalingFactor(String scalingFactor) {
      this.scalingFactor = scalingFactor;
      return this;
    }

    public Builder importName(String importName) {
      this.importName = importName;
      return this;
    }

    public Builder provenanceUrl(String provenanceUrl) {
      this.provenanceUrl = provenanceUrl;
      return this;
    }

    public Observation build() {
      int intHash =
          Objects.hash(
              importName, measurementMethod, observationPeriod, scalingFactor, unit, isDcAggregate);
      // Convert to positive long and then to string
      this.facetId = String.valueOf((long) intHash & 0x7fffffffL);
      return new Observation(this);
    }
  }
}
