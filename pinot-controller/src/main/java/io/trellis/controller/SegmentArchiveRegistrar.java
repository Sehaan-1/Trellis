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
import io.trellis.segment.local.SegmentPreparation;
import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import org.apache.zookeeper.KeeperException;

/** Registers a validated local segment archive without exposing an upload endpoint. */
public final class SegmentArchiveRegistrar {
  private final BlobStore blobs;
  private final ZooKeeperMetadataStore metadata;

  /** Store and metadata client remain caller-owned and must be secured by the caller. */
  public SegmentArchiveRegistrar(BlobStore blobs, ZooKeeperMetadataStore metadata) {
    this.blobs = Objects.requireNonNull(blobs, "blobs");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
  }

  /**
   * Validates a flat ZIP by extracting to a new staging directory, then stores the original ZIP and
   * creates a descriptor. The archive and staging filesystem must be trusted and stable for the
   * entire call, without out-of-band mutation. All limits apply, but validation CPU and memory are
   * not bounded by them. Temporary cleanup is attempted and cleanup failures are suppressed on the
   * original exception. Crashes may leave staging files. Blob storage and metadata are not a
   * transaction: failures may orphan a blob or leave an uncertain metadata write. Reconcile before
   * retrying. This does not check table/schema existence, authenticate, assign or fence leadership.
   */
  public SegmentRegistrar.Registration register(
      String tableName,
      TableConfig.TableType type,
      String segmentName,
      Schema expected,
      Path archive,
      Path stagingDirectory,
      SegmentArchives.Limits limits,
      int maxDocuments)
      throws IOException, KeeperException, InterruptedException {
    SegmentRegistrar.requireName(tableName);
    Objects.requireNonNull(type, "type");
    SegmentRegistrar.requireName(segmentName);
    Objects.requireNonNull(expected, "expected");
    SegmentRegistrar.requireName(expected.name());
    for (Schema.Field field : expected.fields()) SegmentRegistrar.requireName(field.name());
    Objects.requireNonNull(archive, "archive");
    Objects.requireNonNull(stagingDirectory, "stagingDirectory");
    Objects.requireNonNull(limits, "limits");
    if (maxDocuments < 0) throw new IllegalArgumentException("maxDocuments must be nonnegative");
    if (!Files.isDirectory(stagingDirectory, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("stagingDirectory must be an existing non-symlink directory");
    BasicFileAttributes attrs =
        Files.readAttributes(archive, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!attrs.isRegularFile()) throw new IOException("archive must be a regular file");
    if (attrs.size() > limits.maxArchiveBytes())
      throw new IOException("archive exceeds maxArchiveBytes");
    SegmentPreparation.Report report;
    try (Staging staging =
        new Staging(Files.createTempDirectory(stagingDirectory, ".segment-validation-"))) {
      report =
          SegmentPreparation.prepare(
              archive, staging.path.resolve("segment"), limits, expected, maxDocuments);
    }
    BlobStore.StoredBlob blob;
    try (InputStream source = Files.newInputStream(archive, LinkOption.NOFOLLOW_LINKS)) {
      blob = blobs.put(source, limits.maxArchiveBytes());
    }
    if (blob.size() != attrs.size())
      throw new IOException("archive size changed while registering");
    var descriptor =
        new SegmentDescriptor(
            tableName,
            type,
            segmentName,
            expected.name(),
            expected.fields(),
            blob,
            report.segment().documents());
    int version = metadata.createSegment(descriptor);
    return new SegmentRegistrar.Registration(descriptor, version);
  }

  private static final class Staging implements AutoCloseable {
    private final Path path;

    Staging(Path path) {
      this.path = path;
    }

    @Override
    public void close() throws IOException {
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<Path>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(
                Path file, BasicFileAttributes attributes) throws IOException {
              Files.delete(file);
              return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult postVisitDirectory(
                Path directory, IOException failure) throws IOException {
              if (failure != null) throw failure;
              Files.delete(directory);
              return java.nio.file.FileVisitResult.CONTINUE;
            }
          });
    }
  }
}
