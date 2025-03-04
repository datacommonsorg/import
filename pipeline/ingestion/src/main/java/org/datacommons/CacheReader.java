package org.datacommons;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.proto.CacheData.EntityInfo;
import org.datacommons.proto.CacheData.PagedEntities;
import org.datacommons.proto.ChartStoreOuterClass.ChartStore;
import org.datacommons.proto.ChartStoreOuterClass.ObsTimeSeries.SourceSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheReader.class);

  public static class DecodeEntitiesFn extends SimpleFunction<String, PagedEntities> {
    @Override
    public PagedEntities apply(String input) {
      try {
        String value = input.split(",")[1];
        PagedEntities entity =
            PagedEntities.parseFrom(
                new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(value))));
        return entity;
      } catch (IOException ex) {
        return null;
      }
    }
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
    private final String prefix;

    public LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      LOG.info(prefix + c.element());
      c.output(c.element());
    }
  }

  //
  // d/m/Count_MedicalConditionIncident_ConditionStreptococcusPneumonia_ProbableCase_AmericanIndianAndAlaskaNativeAlone^race^RaceCodeEnum^0,H4sIAAAAAAAAAONK51JyzE0tykxOzPPMS8lMzHPMS3HMSSzOTvRLLMksS3XMyc9LlSJCjZJsSrJ+UmJxqr5HaW5iXlBqYkpiUk5qcEliSVhiUbEgAxh8sAcArZlN2nIAAAA=
  //   <dcid^predicate^type^page>, page starts from 0.
  //     PagedEntities, out arcs, sorted, excluding StatVarObservation.

  public static PCollection<Entity> GetEntities(String file, String prefix, Pipeline p) {
    return p.apply(prefix, TextIO.read().from(file))
        .apply(
            Filter.by(
                new SerializableFunction<String, Boolean>() {
                  @Override
                  public Boolean apply(String input) {
                    // Read from the L or M caches.
                    return input.startsWith("d/m/") || input.startsWith("d/l/");
                  }
                }))
        // .apply(MapElements.via(new DecodeEntitiesFn()))
        .apply(
            "GetEntities",
            ParDo.of(
                new DoFn<String, Entity>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    String row = c.element();
                    List<Entity> eList = CacheReader.GetEntitiesfromCache(row);
                    for (Entity e : eList) {
                      c.output(e);
                    }
                  }
                }));
  }

  public static List<Entity> GetEntitiesfromCache(String row) {
    String key = row.split(",")[0];
    String value = row.split(",")[1];
    String prefix = key.substring(4);
    String[] keys = prefix.split("\\^");
    List<Entity> entities = new ArrayList<>();
    if (!(value.isEmpty()) && keys.length >= 2) {
      try {
        String dcid = keys[0];
        String predicate = keys[1];
        PagedEntities elist =
            PagedEntities.parseFrom(
                new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(value))));
        for (EntityInfo entity : elist.getEntitiesList()) {
          // TODO: add a self edge if value is populated
          Entity e =
              new Entity(
                  dcid,
                  entity.getDcid(),
                  predicate,
                  entity.getDcid(),
                  entity.getValue(),
                  entity.getProvenanceId(),
                  entity.getName(),
                  entity.getTypesList());
          entities.add(e);
        }
      } catch (IOException e) {
        LOGGER.error(e.getMessage());
      }
    }
    return entities;
  }

  public static List<Observation> GetObsfromCache(String row) {
    String key = row.split(",")[0];
    String value = row.split(",")[1];
    String prefix = key.substring(4);
    String[] keys = prefix.split("\\^");
    List<Observation> obs = new ArrayList<>();
    if (!(value.isEmpty()) && keys.length >= 2) {
      try {
        ChartStore chart =
            ChartStore.parseFrom(
                new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(value))));
        for (SourceSeries ss : chart.getObsTimeSeries().getSourceSeriesList()) {
          List<String> observations = new ArrayList<>();
          for (Map.Entry<String, Double> e : ss.getValMap().entrySet()) {
            observations.add(
                String.format("{\"%s\" : \"%s\"}", e.getKey(), e.getValue().toString()));
          }
          for (Map.Entry<String, String> e : ss.getStrValMap().entrySet()) {
            observations.add(String.format("{\"%s\" : \"%s\"}", e.getKey(), e.getValue()));
          }
          Observation ob =
              new Observation(
                  keys[1],
                  keys[0],
                  observations,
                  ss.getObservationPeriod(),
                  ss.getMeasurementMethod(),
                  ss.getScalingFactor(),
                  ss.getUnit(),
                  ss.getProvenanceDomain(),
                  ss.getProvenanceUrl(),
                  ss.getImportName());
          obs.add(ob);
        }

      } catch (IOException e) {
        LOGGER.error(e.getMessage());
      }
    }
    return obs;
  }

  //   <placeId^statVarId>, ChartStore containing ObsTimeSeries.
  public static PCollection<Observation> GetObs(String file, String prefix, Pipeline p) {
    PCollection<String> entries =
        p.apply(prefix, TextIO.read().from(file))
            .apply(
                Filter.by(
                    new SerializableFunction<String, Boolean>() {
                      @Override
                      public Boolean apply(String input) {
                        return input.startsWith("d/3/");
                      }
                    }));

    // .apply(MapElements.via(new DecodeObsFn()))
    PCollection<Observation> obs =
        entries.apply(
            "GetObs",
            ParDo.of(
                new DoFn<String, Observation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    String row = c.element();
                    List<Observation> obList = CacheReader.GetObsfromCache(row);
                    for (Observation ob : obList) {
                      c.output(ob);
                    }
                  }
                }));
    return obs;
  }
}
