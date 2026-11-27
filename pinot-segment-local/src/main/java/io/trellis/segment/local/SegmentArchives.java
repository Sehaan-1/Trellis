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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/** Bounded extraction of flat ZIP archives. This does not establish segment validity. */
public final class SegmentArchives {
  private SegmentArchives() {}

  /** Explicit caller-selected resource limits. Byte limits may be zero. */
  public record Limits(long maxArchiveBytes, int maxFiles, long maxFileBytes, long maxTotalBytes) {
    public Limits {
      if (maxArchiveBytes < 0 || maxFiles <= 0 || maxFileBytes < 0 || maxTotalBytes < 0)
        throw new IllegalArgumentException("invalid archive limits");
    }
  }

  /** Extraction statistics, not a segment validation result. */
  public record Report(int files, long expandedBytes) {}

  /**
   * Extracts regular files into a new directory using ZIP central-directory entries. The archive
   * and target parent must be stable and trusted while this runs. Only flat ASCII filenames are
   * accepted, with case-insensitive duplicate rejection. File count, compressed archive size,
   * per-file and total expanded bytes are bounded. CRC-32 and declared size are checked for each
   * file. No archive metadata, executable bits, directories, or links are restored.
   *
   * <p>The target is visible while extraction is in progress; callers must not publish or consume
   * it until this method succeeds. On failure we attempt to remove the target and its extracted
   * files; cleanup failure is suppressed on the original error. This method does not validate
   * Lucene checksums or segment metadata and does not guarantee power-loss durability.
   */
  public static Report extract(Path archive, Path target, Limits limits) throws IOException {
    Objects.requireNonNull(archive, "archive");
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(limits, "limits");
    BasicFileAttributes attrs =
        Files.readAttributes(archive, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!attrs.isRegularFile()) throw new IOException("archive must be a regular file");
    if (attrs.size() > limits.maxArchiveBytes())
      throw new IOException("archive exceeds maxArchiveBytes");
    boolean created = false;
    try (ZipFile zip = new ZipFile(archive.toFile(), StandardCharsets.UTF_8)) {
      if (zip.size() == 0 || zip.size() > limits.maxFiles())
        throw new IOException("invalid archive file count");
      Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
      long declaredTotal = 0;
      Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        String name = entry.getName();
        if (entry.isDirectory()
            || name.length() > 255
            || !name.matches("[A-Za-z0-9_][A-Za-z0-9_.-]*")
            || name.endsWith(".")) throw new IOException("invalid archive entry name");
        if (!names.add(name)) throw new IOException("duplicate archive entry");
        long size = entry.getSize();
        if (size < 0
            || entry.getCrc() < 0
            || size > limits.maxFileBytes()
            || size > limits.maxTotalBytes() - declaredTotal)
          throw new IOException("invalid or excessive declared entry size");
        declaredTotal += size;
      }
      Files.createDirectory(target);
      created = true;
      long total = 0;
      entries = zip.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        long count = 0;
        CRC32 crc = new CRC32();
        try (InputStream input = zip.getInputStream(entry);
            OutputStream output =
                Files.newOutputStream(
                    target.resolve(entry.getName()),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
          byte[] buffer = new byte[65536];
          int n;
          while ((n = input.read(buffer)) != -1) {
            if (n == 0) continue;
            if (n > limits.maxFileBytes() - count || n > limits.maxTotalBytes() - total)
              throw new IOException("expanded archive exceeds limits");
            crc.update(buffer, 0, n);
            output.write(buffer, 0, n);
            count += n;
            total += n;
          }
        }
        if (count != entry.getSize() || crc.getValue() != entry.getCrc())
          throw new IOException("archive entry size or CRC mismatch");
      }
      return new Report(names.size(), total);
    } catch (IOException | RuntimeException | Error e) {
      if (created) {
        try {
          removeTarget(target);
        } catch (IOException cleanup) {
          e.addSuppressed(cleanup);
        }
      }
      throw e;
    }
  }

  private static void removeTarget(Path target) throws IOException {
    try (var files = Files.list(target)) {
      for (Path file : files.toList()) Files.delete(file);
    }
    Files.delete(target);
  }
}
