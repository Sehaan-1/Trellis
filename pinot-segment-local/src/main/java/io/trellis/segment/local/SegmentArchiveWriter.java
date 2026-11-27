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
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Writes a bounded ZIP of a verified local segment. */
public final class SegmentArchiveWriter {
  private SegmentArchiveWriter() {}

  /** Uncompressed statistics and verified document count. */
  public record Report(int files, long expandedBytes, int documents) {}

  /**
   * Verifies and archives a trusted, stable directory. The caller owns destination. On any failure
   * destination may contain partial bytes and must be discarded. Verification and ZIP compression
   * are not bounded in CPU or memory by these limits. No authentication or publication is provided.
   * Excludes Lucene's transient write.lock; all other entries must be flat regular files with names
   * accepted by SegmentArchives. Out-of-band directory mutation during this call is prohibited.
   */
  public static Report write(
      Path segment,
      OutputStream destination,
      SegmentArchives.Limits limits,
      Schema expected,
      int maxDocuments)
      throws IOException {
    Objects.requireNonNull(segment, "segment");
    Objects.requireNonNull(destination, "destination");
    Objects.requireNonNull(limits, "limits");
    Objects.requireNonNull(expected, "expected");
    SegmentVerifier.Report verified = SegmentVerifier.verify(segment, expected, maxDocuments);
    int count = 0;
    long total = 0;
    Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    try (var files = Files.list(segment);
        var zip = new ZipOutputStream(new LimitedOutput(destination, limits.maxArchiveBytes()))) {
      var iterator = files.iterator();
      while (iterator.hasNext()) {
        Path file = iterator.next();
        String name = file.getFileName().toString();
        if (name.equals("write.lock")) continue;
        if (name.length() > 255
            || !name.matches("[A-Za-z0-9_][A-Za-z0-9_.-]*")
            || name.endsWith(".")
            || !names.add(name)) throw new IOException("invalid or duplicate archive entry name");
        BasicFileAttributes attrs =
            Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attrs.isRegularFile()) throw new IOException("segment entry must be a regular file");
        if (++count > limits.maxFiles()) throw new IOException("archive exceeds maxFiles");
        if (attrs.size() > limits.maxFileBytes() || attrs.size() > limits.maxTotalBytes() - total)
          throw new IOException("segment files exceed size limits");
        zip.putNextEntry(new ZipEntry(name));
        long size = 0;
        try (InputStream input = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
          byte[] buffer = new byte[65536];
          int n;
          while ((n = input.read(buffer)) != -1) {
            if (n == 0) continue;
            if (n > limits.maxFileBytes() - size || n > limits.maxTotalBytes() - total)
              throw new IOException("segment files exceed size limits");
            zip.write(buffer, 0, n);
            size += n;
            total += n;
          }
        }
        if (size != attrs.size()) throw new IOException("segment file changed while reading");
        zip.closeEntry();
      }
      if (count == 0) throw new IOException("empty segment archive");
      zip.finish();
    }
    return new Report(count, total, verified.documents());
  }

  /** Counts the entire ZIP, including headers and footer; never closes the caller's stream. */
  private static final class LimitedOutput extends FilterOutputStream {
    private final long limit;
    private long count;

    LimitedOutput(OutputStream output, long limit) {
      super(output);
      this.limit = limit;
    }

    @Override
    public void write(int value) throws IOException {
      if (count == limit) throw new IOException("archive exceeds maxArchiveBytes");
      out.write(value);
      count++;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      if (length > limit - count) throw new IOException("archive exceeds maxArchiveBytes");
      out.write(bytes, offset, length);
      count += length;
    }

    @Override
    public void close() throws IOException {
      flush();
    }
  }
}
