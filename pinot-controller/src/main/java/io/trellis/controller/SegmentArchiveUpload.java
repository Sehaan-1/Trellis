/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.trellis.controller;

import io.trellis.common.ZooKeeperMetadataStore;
import io.trellis.segment.local.SegmentArchives;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.apache.zookeeper.KeeperException;

/** Streams a ZIP into bounded trusted staging, then registers it against the table schema. */
public final class SegmentArchiveUpload {
  /** Indicates the compressed request exceeds the configured limit. */
  public static final class TooLargeException extends IOException {
    public TooLargeException() {
      super("archive exceeds maxArchiveBytes");
    }
  }

  /** Indicates this upload service has reached its active-operation limit. */
  public static final class TooManyUploadsException extends IOException {
    public TooManyUploadsException() {
      super("too many concurrent uploads");
    }
  }

  private final ZooKeeperMetadataStore metadata;
  private final SegmentArchiveRegistrar registrar;
  private final Path staging;
  private final SegmentArchives.Limits limits;
  private final int maxDocuments;
  private final Semaphore permits;

  /** Uses a limit of one active upload; dependencies remain caller-owned and staging trusted. */
  public SegmentArchiveUpload(
      ZooKeeperMetadataStore metadata,
      BlobStore blobs,
      Path stagingDirectory,
      SegmentArchives.Limits limits,
      int maxDocuments)
      throws IOException {
    this(metadata, blobs, stagingDirectory, limits, maxDocuments, 1);
  }

  /** Dependencies are caller-owned; maxConcurrentUploads must be positive. */
  public SegmentArchiveUpload(
      ZooKeeperMetadataStore metadata,
      BlobStore blobs,
      Path stagingDirectory,
      SegmentArchives.Limits limits,
      int maxDocuments,
      int maxConcurrentUploads)
      throws IOException {
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.registrar = new SegmentArchiveRegistrar(Objects.requireNonNull(blobs, "blobs"), metadata);
    this.staging = Objects.requireNonNull(stagingDirectory, "stagingDirectory");
    this.limits = Objects.requireNonNull(limits, "limits");
    if (maxDocuments < 0) throw new IllegalArgumentException("maxDocuments must be nonnegative");
    this.maxDocuments = maxDocuments;
    if (maxConcurrentUploads <= 0)
      throw new IllegalArgumentException("maxConcurrentUploads must be positive");
    this.permits = new Semaphore(maxConcurrentUploads);
    if (!Files.isDirectory(staging, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("stagingDirectory must be an existing non-symlink directory");
  }

  boolean usesMetadata(ZooKeeperMetadataStore store) {
    return metadata == store;
  }

  /**
   * Reads caller-owned input once without closing it. Table and schema metadata must remain stable
   * throughout the call; reads are not transactional or leader-fenced. A failure may leave an
   * orphaned blob or uncertain metadata outcome. Temporary-file cleanup may fail, and a crash may
   * leave files. The per-instance limit rejects excess operations without queueing. It does not
   * bound concurrent requests before this method, other service instances, elapsed time, validation
   * CPU or memory. Call only across a trusted boundary with external authentication and controls.
   */
  public SegmentRegistrar.Registration upload(
      String tableName, TableConfig.TableType type, String segmentName, InputStream input)
      throws IOException, KeeperException, InterruptedException {
    SegmentRegistrar.requireName(tableName);
    Objects.requireNonNull(type, "type");
    SegmentRegistrar.requireName(segmentName);
    Objects.requireNonNull(input, "input");
    if (!permits.tryAcquire()) throw new TooManyUploadsException();
    try {
      return uploadAcquired(tableName, type, segmentName, input);
    } finally {
      permits.release();
    }
  }

  private SegmentRegistrar.Registration uploadAcquired(
      String tableName, TableConfig.TableType type, String segmentName, InputStream input)
      throws IOException, KeeperException, InterruptedException {
    var table = metadata.getTable(tableName, type).value();
    var schema = metadata.getSchema(table.schemaName()).value();
    try (TempFile temp = new TempFile(Files.createTempFile(staging, ".segment-upload-", ".zip"))) {
      try (OutputStream output = Files.newOutputStream(temp.path)) {
        long total = 0;
        byte[] buffer = new byte[65536];
        int count;
        while ((count = input.read(buffer)) != -1) {
          if (count == 0) continue;
          if (count > limits.maxArchiveBytes() - total) throw new TooLargeException();
          output.write(buffer, 0, count);
          total += count;
        }
      }
      return registrar.register(
          tableName, type, segmentName, schema, temp.path, staging, limits, maxDocuments);
    }
  }

  private static final class TempFile implements AutoCloseable {
    private final Path path;

    TempFile(Path path) {
      this.path = path;
    }

    @Override
    public void close() throws IOException {
      Files.delete(path);
    }
  }
}
