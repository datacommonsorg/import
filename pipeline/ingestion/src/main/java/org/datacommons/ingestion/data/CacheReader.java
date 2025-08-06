package org.datacommons.ingestion.data;

import com.google.api.gax.paging.Page;
import com.google.cloud.ByteArray;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.datacommons.pipeline.util.GraphUtils;
import org.datacommons.proto.CacheData.EntityInfo;
import org.datacommons.proto.CacheData.PagedEntities;
import org.datacommons.proto.ChartStoreOuterClass.ChartStore;
import org.datacommons.proto.ChartStoreOuterClass.ObsTimeSeries.SourceSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CacheReader is a class that has utility methods to read data from BT cache. */
public class CacheReader implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheReader.class);
  private static final String OUT_ARC_CACHE_PREFIX = "d/m/";
  private static final String IN_ARC_CACHE_PREFIX = "d/l/";
  private static final String OBS_TIME_SERIES_CACHE_PREFIX = "d/3/";
  private static final int CACHE_KEY_PREFIX_SIZE = 4;
  private static final String CACHE_KEY_VALUE_SEPARATOR_REGEX = ",";
  private static final String CACHE_KEY_SEPARATOR_REGEX = "\\^";

  private final String gcsBucketId;

  public CacheReader(String gcsBucketId) {
    this.gcsBucketId = gcsBucketId;
  }

  /** Returns the GCS cache path for the import group. */
  public static String getCachePath(String projectId, String bucketId, String importGroup) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    Page<Blob> blobs =
        storage.list(
            bucketId,
            Storage.BlobListOption.prefix(importGroup),
            Storage.BlobListOption.currentDirectory());

    // Currently this fetches the "last" blob in the directory which typically tends
    // to be the latest.
    // TODO: Update this code to fetch a specific blob and update the logic to
    // definitively fetch the latest if none is provided.
    String prefix = "";
    for (Blob blob : blobs.iterateAll()) {
      prefix = blob.getName();
    }
    if (prefix.isEmpty()) {
      throw new RuntimeException(String.format("No blobs found for import group: %s", importGroup));
    }
    return String.format("gs://%s/%scache.csv*", bucketId, prefix);
  }

  /**
   * Returns the GCS path for the import group cache in the specified bucket with the specified
   * import group version.
   *
   * <p>The returned path is of the form: gs://gcsBucketId/importGroupVersion/cache.csv*
   *
   * <p>e.g. gs://datcom-store/auto1d_2025_03_26_02_16_23/cache.csv*
   */
  public String getImportGroupCachePath(String importGroupVersion) {
    return String.format("gs://%s/%s/cache.csv*", gcsBucketId, importGroupVersion);
  }

  /** Parses an arc cache row to extract nodes and edges. */
  public NodesEdges parseArcRow(String row) {
    NodesEdges result = new NodesEdges();

    // Cache format: <dcid^predicate^type^page>, PagedEntities
    if (row != null && !row.isEmpty()) {
      String[] items = row.split(CACHE_KEY_VALUE_SEPARATOR_REGEX);
      if (items.length == 2) {
        String key = items[0];
        String value = items[1];
        String suffix = key.substring(CACHE_KEY_PREFIX_SIZE);
        String[] keys = suffix.split(CACHE_KEY_SEPARATOR_REGEX);
        if (!(value.isEmpty()) && keys.length >= 2) {
          String dcid = keys[0];
          String predicate = keys[1];
          String typeOf = keys[2];

          PagedEntities elist = ProtoUtil.parseCacheProto(value, PagedEntities.parser());
          for (EntityInfo entity : elist.getEntitiesList()) {
            String nodeId = "";
            String nodeValue = "";
            String subjectId = "";
            String objectId = "";
            boolean reference = false;
            ByteArray bytes = null;
            if (isOutArcCacheRow(row)) { // Out arc row
              subjectId = dcid;
              if (!entity.getDcid().isEmpty()) { // Reference
                nodeId = entity.getDcid();
                nodeValue = entity.getDcid();
                objectId = entity.getDcid();
                reference = true;
              } else { // Value
                String hash = GraphUtils.generateSha256(entity.getValue());
                nodeId = hash;
                if (GraphUtils.storeValueAsBytes(predicate)) {
                  bytes = ByteArray.copyFrom(GraphUtils.compressString(entity.getValue()));
                } else {
                  nodeValue = entity.getValue();
                }
                objectId = hash;
              }
            } else { // In arc row
              nodeId = entity.getDcid();
              nodeValue = entity.getDcid();
              subjectId = entity.getDcid();
              objectId = dcid;
              reference = true;
            }

            List<String> types = entity.getTypesList();
            if (types.isEmpty() && !typeOf.isEmpty()) {
              types = Arrays.asList(typeOf);
            }

            // Add node.
            if (!nodeId.isEmpty()) {
              result.addNode(
                  Node.builder()
                      .subjectId(nodeId)
                      .value(nodeValue)
                      .reference(reference)
                      .name(entity.getName())
                      .types(types)
                      .build());
            }

            // Add edge.
            if (!subjectId.isEmpty() && !objectId.isEmpty()) {
              result.addEdge(
                  Edge.builder()
                      .subjectId(subjectId)
                      .predicate(predicate)
                      .objectId(objectId)
                      .provenance(entity.getProvenanceId())
                      .build());
            }
          }
        }
      }
    }

    return result;
  }

  /** Parses a time series cache row to extract observations. */
  public List<Observation> parseTimeSeriesRow(String row) {
    List<Observation> result = new ArrayList<>();

    // Cache format: <placeId^statVarId>, ChartStore containing ObsTimeSeries.
    if (row != null && !row.isEmpty()) {
      String[] items = row.split(CACHE_KEY_VALUE_SEPARATOR_REGEX);
      if (items.length == 2) {
        String key = items[0];
        String value = items[1];
        String suffix = key.substring(CACHE_KEY_PREFIX_SIZE);
        String[] keys = suffix.split(CACHE_KEY_SEPARATOR_REGEX);
        if (!(value.isEmpty()) && keys.length >= 2) {
          String variableMeasured = keys[1];
          String observationAbout = keys[0];
          ChartStore chart = ProtoUtil.parseCacheProto(value, ChartStore.parser());
          for (SourceSeries source : chart.getObsTimeSeries().getSourceSeriesList()) {
            Observation.Builder builder =
                Observation.builder()
                    .variableMeasured(variableMeasured)
                    .observationAbout(observationAbout)
                    .observationPeriod(source.getObservationPeriod())
                    .measurementMethod(source.getMeasurementMethod())
                    .scalingFactor(source.getScalingFactor())
                    .unit(source.getUnit())
                    .isDcAggregate(source.getIsDcAggregate())
                    .provenanceUrl(source.getProvenanceUrl())
                    .importName(source.getImportName());
            for (Map.Entry<String, Double> e : source.getValMap().entrySet()) {
              builder.observation(e.getKey(), e.getValue().toString());
            }
            for (Map.Entry<String, String> e : source.getStrValMap().entrySet()) {
              builder.observation(e.getKey(), e.getValue());
            }
            result.add(builder.build());
          }
        }
      }
    }

    return result;
  }

  private static final boolean isInArcCacheRow(String row) {
    return row.startsWith(IN_ARC_CACHE_PREFIX);
  }

  private static final boolean isOutArcCacheRow(String row) {
    return row.startsWith(OUT_ARC_CACHE_PREFIX);
  }

  public static final boolean isArcCacheRow(String row) {
    return isInArcCacheRow(row) || isOutArcCacheRow(row);
  }

  public static final boolean isObsTimeSeriesCacheRow(String row) {
    return row.startsWith(OBS_TIME_SERIES_CACHE_PREFIX);
  }
}
