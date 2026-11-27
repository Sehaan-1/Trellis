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

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentArchivesTest {
  @TempDir Path root;
  private static final SegmentArchives.Limits LIMITS = new SegmentArchives.Limits(4096, 5, 64, 128);

  private Path zip(String name, String[] names, String[] values) throws IOException {
    Path path = root.resolve(name);
    try (var out = new ZipOutputStream(Files.newOutputStream(path))) {
      for (int i = 0; i < names.length; i++) {
        out.putNextEntry(new ZipEntry(names[i]));
        out.write(values[i].getBytes(StandardCharsets.UTF_8));
        out.closeEntry();
      }
    }
    return path;
  }

  @Test
  void extractsFlatFilesAndReportsSizes() throws Exception {
    Path archive =
        zip("in.zip", new String[] {"segments_1", "_0.cfs"}, new String[] {"abc", "xyzzy"});
    var report = SegmentArchives.extract(archive, root.resolve("out"), LIMITS);
    assertThat(report.files()).isEqualTo(2);
    assertThat(report.expandedBytes()).isEqualTo(8);
    assertThat(Files.readString(root.resolve("out/segments_1"))).isEqualTo("abc");
    assertThat(Files.readString(root.resolve("out/_0.cfs"))).isEqualTo("xyzzy");
    assertThatThrownBy(() -> SegmentArchives.extract(archive, root.resolve("out"), LIMITS))
        .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
  }

  @Test
  void rejectsTraversalDirectoriesCaseCollisionsAndEmptyArchives() throws Exception {
    Path target = root.resolve("out");
    for (String invalid :
        new String[] {"../escape", "/absolute", "a/b", "dir/", ".hidden", "name."}) {
      Path archive = zip("invalid.zip", new String[] {invalid}, new String[] {"x"});
      assertThatThrownBy(() -> SegmentArchives.extract(archive, target, LIMITS))
          .isInstanceOf(IOException.class);
      assertThat(Files.exists(target)).isFalse();
      Files.delete(archive);
    }
    Path collision =
        zip("collision.zip", new String[] {"_0.cfs", "_0.CFS"}, new String[] {"a", "b"});
    assertThatThrownBy(() -> SegmentArchives.extract(collision, target, LIMITS))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("duplicate");
    Path empty = zip("empty.zip", new String[0], new String[0]);
    assertThatThrownBy(() -> SegmentArchives.extract(empty, target, LIMITS))
        .isInstanceOf(IOException.class);
    assertThat(Files.exists(root.resolve("escape"))).isFalse();
  }

  @Test
  void limitsAndInvalidInputsAreRejected() throws Exception {
    Path archive = zip("in.zip", new String[] {"a", "b"}, new String[] {"ab", "cde"});
    Path out = root.resolve("out");
    assertThatThrownBy(
            () -> SegmentArchives.extract(archive, out, new SegmentArchives.Limits(1, 5, 64, 128)))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(
            () ->
                SegmentArchives.extract(archive, out, new SegmentArchives.Limits(4096, 1, 64, 128)))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(
            () ->
                SegmentArchives.extract(archive, out, new SegmentArchives.Limits(4096, 5, 2, 128)))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(
            () -> SegmentArchives.extract(archive, out, new SegmentArchives.Limits(4096, 5, 64, 4)))
        .isInstanceOf(IOException.class);
    assertThat(Files.exists(out)).isFalse();
    assertThatThrownBy(() -> new SegmentArchives.Limits(1, 0, 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SegmentArchives.Limits(-1, 1, 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> SegmentArchives.extract(null, out, LIMITS));
    Path symlink = root.resolve("link.zip");
    Files.createSymbolicLink(symlink, archive);
    assertThatThrownBy(() -> SegmentArchives.extract(symlink, out, LIMITS))
        .isInstanceOf(IOException.class);
  }

  @Test
  void understatedSizeCannotBypassExpandedLimit() throws Exception {
    Path archive =
        zip(
            "understated.zip",
            new String[] {"_0.cfs"},
            new String[] {"longer-than-the-declared-size"});
    byte[] raw = Files.readAllBytes(archive);
    int central = -1;
    for (int i = 0; i < raw.length - 28; i++) {
      if (raw[i] == 0x50 && raw[i + 1] == 0x4b && raw[i + 2] == 0x01 && raw[i + 3] == 0x02) {
        central = i;
        break;
      }
    }
    assertThat(central).isNotNegative();
    raw[central + 24] = 1;
    raw[central + 25] = 0;
    raw[central + 26] = 0;
    raw[central + 27] = 0;
    Files.write(archive, raw);
    Path out = root.resolve("out");
    var limits = new SegmentArchives.Limits(4096, 3, 2, 2);
    assertThatThrownBy(() -> SegmentArchives.extract(archive, out, limits))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("expanded archive exceeds limits");
    assertThat(Files.exists(out)).isFalse();
  }

  @Test
  void checksumMismatchCleansTarget() throws Exception {
    Path archive = root.resolve("broken.zip");
    byte[] bytes = "plain-text-corruption-marker".getBytes(StandardCharsets.UTF_8);
    CRC32 crc = new CRC32();
    crc.update(bytes);
    try (var out = new ZipOutputStream(Files.newOutputStream(archive))) {
      var entry = new ZipEntry("_0.cfs");
      entry.setMethod(ZipEntry.STORED);
      entry.setSize(bytes.length);
      entry.setCompressedSize(bytes.length);
      entry.setCrc(crc.getValue());
      out.putNextEntry(entry);
      out.write(bytes);
      out.closeEntry();
    }
    byte[] raw = Files.readAllBytes(archive);
    byte[] marker = bytes;
    int start = -1;
    outer:
    for (int i = 0; i <= raw.length - marker.length; i++) {
      for (int j = 0; j < marker.length; j++) if (raw[i + j] != marker[j]) continue outer;
      start = i;
      break;
    }
    assertThat(start).isNotNegative();
    raw[start] ^= 1;
    Files.write(archive, raw);
    Path out = root.resolve("out");
    assertThatThrownBy(() -> SegmentArchives.extract(archive, out, LIMITS))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("CRC");
    assertThat(Files.exists(out)).isFalse();
  }
}
