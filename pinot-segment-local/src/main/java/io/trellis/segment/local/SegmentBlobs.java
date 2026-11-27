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
package io.trellis.segment.local;

import io.trellis.spi.Schema;
import io.trellis.spi.storage.BlobStore;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;

/** Transfer verified local segment archives through an opaque blob store. */
public final class SegmentBlobs {
  private SegmentBlobs() {}

  /**
   * Verifies and stages an archive before storing it. The segment and existing staging directory
   * must be trusted and stable. The store and streams remain caller-owned. A store failure or a
   * staging cleanup failure can leave an already-published blob; retries may create another key.
   */
  public static BlobStore.StoredBlob put(
      BlobStore store,
      Path segment,
      Path stagingDirectory,
      SegmentArchives.Limits limits,
      Schema schema,
      int maxDocuments)
      throws IOException {
    Objects.requireNonNull(store, "store");
    Objects.requireNonNull(segment, "segment");
    Objects.requireNonNull(limits, "limits");
    Objects.requireNonNull(schema, "schema");
    try (StagedFile stage = createStage(stagingDirectory)) {
      try (OutputStream output = Files.newOutputStream(stage.path)) {
        SegmentArchiveWriter.write(segment, output, limits, schema, maxDocuments);
      }
      try (InputStream input = Files.newInputStream(stage.path)) {
        return store.put(input, limits.maxArchiveBytes());
      }
    }
  }

  /**
   * Copies a blob into bounded staging, then extracts and verifies it. Target must not exist. The
   * blob store must honor its SHA-256 verification contract. Caller must discard target if this
   * method fails: staging cleanup can fail after preparation has completed. The target is visible
   * during preparation and must not be consumed until success. No atomic publication is implied.
   */
  public static SegmentPreparation.Report prepare(
      BlobStore store,
      BlobStore.Key key,
      Path stagingDirectory,
      Path target,
      SegmentArchives.Limits limits,
      Schema schema,
      int maxDocuments)
      throws IOException {
    Objects.requireNonNull(store, "store");
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(limits, "limits");
    Objects.requireNonNull(schema, "schema");
    if (maxDocuments < 0) throw new IllegalArgumentException("maxDocuments must be nonnegative");
    LocalSegments.validateSchema(schema);
    try (StagedFile stage = createStage(stagingDirectory)) {
      try (OutputStream file = Files.newOutputStream(stage.path)) {
        LimitedOutput output = new LimitedOutput(file, limits.maxArchiveBytes());
        long reported = store.copyTo(key, output);
        if (reported != output.count) throw new IOException("blob size mismatch");
      }
      return SegmentPreparation.prepare(stage.path, target, limits, schema, maxDocuments);
    }
  }

  private static StagedFile createStage(Path directory) throws IOException {
    Objects.requireNonNull(directory, "stagingDirectory");
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("stagingDirectory must be an existing non-symlink directory");
    return new StagedFile(Files.createTempFile(directory, ".segment-", ".zip"));
  }

  private static final class StagedFile implements AutoCloseable {
    final Path path;

    StagedFile(Path path) {
      this.path = path;
    }

    @Override
    public void close() throws IOException {
      Files.delete(path);
    }
  }

  private static final class LimitedOutput extends FilterOutputStream {
    private final long limit;
    private long count;

    LimitedOutput(OutputStream out, long limit) {
      super(out);
      this.limit = limit;
    }

    @Override
    public void write(int value) throws IOException {
      if (count == limit) throw new IOException("blob exceeds maxArchiveBytes");
      out.write(value);
      count++;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      if (length > limit - count) throw new IOException("blob exceeds maxArchiveBytes");
      out.write(bytes, offset, length);
      count += length;
    }
  }
}
