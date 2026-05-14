package org.datacommons.ingestion.data;

import com.google.common.base.Joiner;
import com.google.common.hash.Hashing;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Models a statvar observation time series.
 *
 * <p>This class is used to store the result of parsing a time series row in memory.
 */
public class TimeSeries implements Serializable {
  private static final String OBS_SERIES_DCID_PREFIX = "dc/os/";
  private static final String OBS_SERIES_TYPE = "StatVarObsSeries";
  private static final String OBSERVATION_ABOUT_PREDICATE = "observationAbout";
  private static final String VARIABLE_MEASURED_PREDICATE = "variableMeasured";
  private static final String NAME_PREDICATE = "name";
  private static final String TYPE_OF_PREDICATE = "typeOf";

  private String observationAbout;
  private String variableMeasured;
  private Map<String, String> observations;
  private String observationPeriod;
  private String measurementMethod;
  private String unit;
  private String scalingFactor;
  private String importName;
  private String provenanceUrl;
  private String facetId;
  private boolean isDcAggregate;
  private boolean isBaseDc;
  private NodesEdges obsGraph;

  private TimeSeries(Builder builder) {
    this.observationAbout = builder.observationAbout;
    this.variableMeasured = builder.variableMeasured;
    this.observations = builder.observations;
    this.observationPeriod = builder.observationPeriod;
    this.measurementMethod = builder.measurementMethod;
    this.unit = builder.unit;
    this.scalingFactor = builder.scalingFactor;
    this.importName = builder.importName;
    this.provenanceUrl = builder.provenanceUrl;
    this.facetId = builder.facetId;
    this.isDcAggregate = builder.isDcAggregate;
    this.isBaseDc = builder.isBaseDc;
    this.obsGraph = toObsGraph();
  }

  public static Builder builder() {
    return new Builder();
  }

  public TimeSeriesKey getKey() {
    return new TimeSeriesKey(
        variableMeasured,
        observationAbout,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        facetId);
  }

  public String getObservationAbout() {
    return observationAbout;
  }

  public String getVariableMeasured() {
    return variableMeasured;
  }

  public Map<String, String> getObservations() {
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

  public boolean getIsBaseDc() {
    return isBaseDc;
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
    var provenanceDcid = ProvenanceUtils.getProvenanceDcid(importName, this.isBaseDc);

    // Add series node
    graph.addNode(
        Node.builder()
            .subjectId(seriesDcid)
            .value(seriesDcid)
            .name(seriesName)
            .types(List.of(OBS_SERIES_TYPE))
            .build());

    // Add leaf node
    graph.addNode(
        Node.builder().subjectId(Encode.generateSha256(seriesName)).value(seriesName).build());

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
            .objectId(Encode.generateSha256(seriesName))
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
    TimeSeries that = (TimeSeries) o;

    return Objects.equals(observationAbout, that.observationAbout)
        && Objects.equals(variableMeasured, that.variableMeasured)
        && Objects.equals(observations, that.observations)
        && Objects.equals(observationPeriod, that.observationPeriod)
        && Objects.equals(measurementMethod, that.measurementMethod)
        && Objects.equals(unit, that.unit)
        && Objects.equals(scalingFactor, that.scalingFactor)
        && Objects.equals(importName, that.importName)
        && Objects.equals(provenanceUrl, that.provenanceUrl)
        && Objects.equals(facetId, that.facetId)
        && Objects.equals(isDcAggregate, that.isDcAggregate)
        && Objects.equals(isBaseDc, that.isBaseDc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        observationAbout,
        variableMeasured,
        observations,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        importName,
        provenanceUrl,
        facetId,
        isDcAggregate,
        isBaseDc);
  }

  public static String calculateFacetId(
      String importName,
      String measurementMethod,
      String observationPeriod,
      String scalingFactor,
      String unit,
      boolean isDcAggregate) {
    String concatString =
        Joiner.on("^")
            .useForNull("")
            .join(
                importName,
                measurementMethod,
                observationPeriod,
                scalingFactor,
                unit,
                String.valueOf(isDcAggregate));
    long fingerprint =
        Hashing.farmHashFingerprint64().hashString(concatString, StandardCharsets.UTF_8).asLong();
    return String.valueOf(fingerprint);
  }

  // Builder for TimeSeries
  public static class Builder {
    private String observationAbout = "";
    private String variableMeasured = "";
    private Map<String, String> observations = new HashMap<>();
    private String observationPeriod = "";
    private String measurementMethod = "";
    private String unit = "";
    private String scalingFactor = "";
    private String importName = "";
    private String provenanceUrl = "";
    private String facetId = "";
    private boolean isDcAggregate = false;
    private boolean isBaseDc = true;

    public Builder observationAbout(String observationAbout) {
      this.observationAbout = observationAbout;
      return this;
    }

    public Builder variableMeasured(String variableMeasured) {
      this.variableMeasured = variableMeasured;
      return this;
    }

    public Builder observation(String date, String value) {
      this.observations.put(date, value);
      return this;
    }

    public Builder observations(Map<String, String> observations) {
      this.observations = observations == null ? new HashMap<>() : new HashMap<>(observations);
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

    public Builder isBaseDc(boolean isBaseDc) {
      this.isBaseDc = isBaseDc;
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

    public TimeSeries build() {
      this.facetId =
          calculateFacetId(
              importName, measurementMethod, observationPeriod, scalingFactor, unit, isDcAggregate);
      return new TimeSeries(this);
    }
  }
}
