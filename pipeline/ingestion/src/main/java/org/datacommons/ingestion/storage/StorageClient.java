package org.datacommons.ingestion.storage;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.datacommons.pipeline.util.PipelineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageClient.class);
  private final Storage storage;
  private final String bucketName;
  private final String version;

  public StorageClient(String bucketName, String version) {
    this.storage = StorageOptions.getDefaultInstance().getService();
    this.bucketName = bucketName;
    this.version = version;
  }

  public String writeBlob(String subject_id, String predicate, String content) throws IOException {
    String objectName = getObjectName(subject_id, predicate);
    BlobInfo blobInfo =
        BlobInfo.newBuilder(this.bucketName, objectName)
            .setContentType("text/plain")
            .setContentEncoding("gzip")
            .build();

    try (WriteChannel writer = storage.writer(blobInfo)) {
      writer.write(ByteBuffer.wrap(PipelineUtils.compressString(content)));
      LOGGER.info("Uploaded " + objectName + " to GCS bucket " + this.bucketName);
    } catch (IOException e) {
      LOGGER.error(
          "Error during upload of "
              + objectName
              + " to GCS bucket "
              + this.bucketName
              + ": "
              + e.getMessage());
      throw e;
    }
    return String.format("https://storage.googleapis.com/%s/%s", this.bucketName, objectName);
  }

  private String getObjectName(String subject_id, String predicate) {
    return String.format("%s/%s/%s.txt.gz", this.version, predicate, subject_id);
  }
}
