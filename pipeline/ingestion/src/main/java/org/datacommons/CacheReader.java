package org.datacommons;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.proto.CacheData.EntityInfo;
import org.datacommons.proto.CacheData.PagedEntities;
import org.datacommons.proto.ChartStoreOuterClass.ChartStore;
import org.datacommons.proto.ChartStoreOuterClass.ObsTimeSeries.SourceSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Utility functions to read caches from GCS.
public class CacheReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheReader.class);

  // Returns GCS path for the import group. 
  private static String GetPrefix(String projectId, String bucketId, String group) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    Page<Blob> blobs =
        storage.list(
            bucketId,
            Storage.BlobListOption.prefix(group),
            Storage.BlobListOption.currentDirectory());

    String prefix = "";
    for (Blob blob : blobs.iterateAll()) {
      prefix = blob.getName();
    }
    return String.format("gs://%s/%scache.csv*", bucketId, prefix);
  }

  private static List<Entity> GetEntitiesfromCache(String row) {
    // Cache format:  <dcid^predicate^type^page>, PagedEntities
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

  private static List<Observation> GetObservationsfromCache(String row) {
    // Cache format: <placeId^statVarId>, ChartStore containing ObsTimeSeries.
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

  public static PCollection<Entity> GetEntities(String group, Pipeline p, IngestionOptions config) {
    String prefix = GetPrefix(config.getProjectId(), config.getStorageBucketId(), group);
    return p.apply(group, TextIO.read().from(prefix))
        .apply(
            Filter.by(
                new SerializableFunction<String, Boolean>() {
                  @Override
                  public Boolean apply(String input) {
                    // Read from the L or M caches.
                    return input.startsWith("d/m/") || input.startsWith("d/l/");
                  }
                }))
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


  public static PCollection<Observation> GetObservations(
      String group, Pipeline p, IngestionOptions config) {
    String prefix = GetPrefix(config.getProjectId(), config.getStorageBucketId(), group);
    PCollection<String> entries =
        p.apply(group, TextIO.read().from(prefix))
            .apply(
                Filter.by(
                    new SerializableFunction<String, Boolean>() {
                      @Override
                      public Boolean apply(String input) {
                        return input.startsWith("d/3/");
                      }
                    }));

    PCollection<Observation> obs =
        entries.apply(
            "GetObs",
            ParDo.of(
                new DoFn<String, Observation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    String row = c.element();
                    List<Observation> obList = CacheReader.GetObservationsfromCache(row);
                    for (Observation ob : obList) {
                      c.output(ob);
                    }
                  }
                }));
    return obs;
  }
}
