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

/** Utility functions to read caches from GCS. */
public class CacheReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheReader.class);

  /**
   * GCS cache path for the import group.
   *
   * @param projectId GCP project id
   * @param bucketId GCS bucket Id
   * @param group Import group
   * @return
   */
  public static String getCachePath(String projectId, String bucketId, String group) {
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

  /**
   * Get entities parsing a cache row.
   *
   * @param row Input cache row to be parsed
   * @return List of entities present in the row
   */
  private static List<Entity> parseEntityCache(String row) {
    // Cache format:  <dcid^predicate^type^page>, PagedEntities
    List<Entity> entities = new ArrayList<>();
    if (row != null && !row.isEmpty()) {
      String[] items = row.split(",");
      if (items.length == 2) {
        String key = items[0];
        String value = items[1];
        String prefix = key.substring(4);
        String[] keys = prefix.split("\\^");
        if (!(value.isEmpty()) && keys.length >= 2) {
          try {
            String dcid = keys[0];
            String predicate = keys[1];
            PagedEntities elist =
                PagedEntities.parseFrom(
                    new GZIPInputStream(
                        new ByteArrayInputStream(Base64.getDecoder().decode(value))));
            for (EntityInfo entity : elist.getEntitiesList()) {
              String subjectId, objectId, nodeId = "";
              // Add a self edge if value is populated.
              if (!entity.getValue().isEmpty()) {
                subjectId = dcid;
                objectId = dcid;
                // Terminal edges won't produce any object nodes.
              } else {
                if (row.startsWith("d/m/")) {
                  subjectId = dcid;
                  objectId = entity.getDcid();
                  nodeId = entity.getDcid();
                } else { // "d/l/"
                  subjectId = entity.getDcid();
                  objectId = dcid;
                  nodeId = entity.getDcid();
                }
              }
              // TODO: fix id column.
              Entity e =
                  new Entity(
                      subjectId,
                      subjectId,
                      predicate,
                      objectId,
                      entity.getValue(),
                      entity.getProvenanceId(),
                      nodeId,
                      entity.getName(),
                      entity.getTypesList());
              entities.add(e);
            }
          } catch (IOException e) {
            LOGGER.error("Error parsing entity cache", e);
          }
        }
      }
    }
    return entities;
  }

  /**
   * Get observations parsing a cache row.
   *
   * @param row Input cache row to be parsed
   * @return List of observations present in the row
   */
  private static List<Observation> parseObservationCache(String row) {
    // Cache format: <placeId^statVarId>, ChartStore containing ObsTimeSeries.
    List<Observation> obs = new ArrayList<>();
    if (row != null && !row.isEmpty()) {
      String[] items = row.split(",");
      if (items.length == 2) {
        String key = items[0];
        String value = items[1];
        String prefix = key.substring(4);
        String[] keys = prefix.split("\\^");
        if (!(value.isEmpty()) && keys.length >= 2) {
          try {
            ChartStore chart =
                ChartStore.parseFrom(
                    new GZIPInputStream(
                        new ByteArrayInputStream(Base64.getDecoder().decode(value))));
            for (SourceSeries source : chart.getObsTimeSeries().getSourceSeriesList()) {
              List<String> observations = new ArrayList<>();
              for (Map.Entry<String, Double> e : source.getValMap().entrySet()) {
                observations.add(
                    String.format("{\"%s\" : \"%s\"}", e.getKey(), e.getValue().toString()));
              }
              for (Map.Entry<String, String> e : source.getStrValMap().entrySet()) {
                observations.add(String.format("{\"%s\" : \"%s\"}", e.getKey(), e.getValue()));
              }
              Observation ob =
                  new Observation(
                      keys[1],
                      keys[0],
                      observations,
                      source.getObservationPeriod(),
                      source.getMeasurementMethod(),
                      source.getScalingFactor(),
                      source.getUnit(),
                      source.getProvenanceDomain(),
                      source.getProvenanceUrl(),
                      source.getImportName());
              obs.add(ob);
            }

          } catch (IOException e) {
            // TODO: add counters to capture errors.
            LOGGER.error("Error parsing observation cache", e);
          }
        }
      }
    }
    return obs;
  }

  /**
   * A dataflow pipeline stage to filter and transform a PCollection of cache entries to a
   * PCollection of entities.
   *
   * @param entries PCollection of input cache entries
   * @return PCollection of entities
   */
  public static PCollection<Entity> getEntities(PCollection<String> entries) {
    return entries
        .apply(
            "FilterEntities",
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
                    List<Entity> entityList = CacheReader.parseEntityCache(row);
                    for (Entity entity : entityList) {
                      c.output(entity);
                    }
                  }
                }));
  }

  /**
   * A dataflow pipeline stage to filter and transform a PCollection of cache entries to a
   * PCollection of observations.
   *
   * @param entries PCollection of input cache entries
   * @return PCollection of observations
   */
  public static PCollection<Observation> getObservations(PCollection<String> entries) {
    return entries
        .apply(
            "FilterObs",
            Filter.by(
                new SerializableFunction<String, Boolean>() {
                  @Override
                  public Boolean apply(String input) {
                    return input.startsWith("d/3/");
                  }
                }))
        .apply(
            "GetObs",
            ParDo.of(
                new DoFn<String, Observation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    String row = c.element();
                    List<Observation> obsList = CacheReader.parseObservationCache(row);
                    for (Observation ob : obsList) {
                      c.output(ob);
                    }
                  }
                }));
  }
}
