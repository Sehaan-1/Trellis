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

import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentPreparationTest {
  @TempDir Path root;
  private static final Schema SCHEMA =
      new Schema("games", List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
  private static final SegmentArchives.Limits LIMITS =
      new SegmentArchives.Limits(1000000, 100, 1000000, 1000000);

  private Path archive() throws IOException {
    Path segment = root.resolve("source");
    LocalSegments.write(segment, SCHEMA, List.of(new GenericRow().put("team", "red")));
    Path archive = root.resolve("segment.zip");
    try (var output = new ZipOutputStream(Files.newOutputStream(archive));
        var files = Files.list(segment)) {
      for (Path file : files.toList()) {
        if (file.getFileName().toString().equals("write.lock")) continue;
        output.putNextEntry(new ZipEntry(file.getFileName().toString()));
        Files.copy(file, output);
        output.closeEntry();
      }
    }
    return archive;
  }

  @Test
  void extractsAndValidates() throws Exception {
    Path target = root.resolve("target");
    var report = SegmentPreparation.prepare(archive(), target, LIMITS, SCHEMA, 1);
    assertThat(report.segment()).isEqualTo(new SegmentVerifier.Report(1, 1));
    assertThat(report.archive().files()).isPositive();
    try (var segment = LocalSegments.open(target)) {
      assertThat(segment.rowCount()).isEqualTo(1);
    }
  }

  @Test
  void cleansUpAfterValidationFailures() throws Exception {
    Path archive = archive();
    Path target = root.resolve("target");
    assertThatThrownBy(() -> SegmentPreparation.prepare(archive, target, LIMITS, SCHEMA, 0))
        .isInstanceOf(IOException.class);
    assertThat(target).doesNotExist();
    Schema mismatch = new Schema("other", SCHEMA.fields());
    assertThatThrownBy(() -> SegmentPreparation.prepare(archive, target, LIMITS, mismatch, 1))
        .isInstanceOf(IOException.class);
    assertThat(target).doesNotExist();
  }

  @Test
  void preservesExistingTargetsAndRejectsBadArguments() throws Exception {
    Path archive = archive();
    Path target = Files.createDirectory(root.resolve("target"));
    Path marker = Files.writeString(target.resolve("marker"), "safe");
    assertThatThrownBy(() -> SegmentPreparation.prepare(archive, target, LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    assertThat(Files.readString(marker)).isEqualTo("safe");
    assertThatThrownBy(
            () -> SegmentPreparation.prepare(archive, root.resolve("new"), LIMITS, SCHEMA, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentPreparation.prepare(archive, target, LIMITS, null, 1));
    assertThat(root.resolve("new")).doesNotExist();
  }

  @Test
  void cleansUpNonSegmentArchive() throws Exception {
    Path archive = root.resolve("invalid.zip");
    try (var zip = new ZipOutputStream(Files.newOutputStream(archive))) {
      zip.putNextEntry(new ZipEntry("not-a-segment"));
      zip.write(1);
      zip.closeEntry();
    }
    Path target = root.resolve("target");
    assertThatThrownBy(() -> SegmentPreparation.prepare(archive, target, LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    assertThat(target).doesNotExist();
  }

  @Test
  void failedExtractionDoesNotLeaveTarget() throws Exception {
    Path archive = archive();
    Path target = root.resolve("target");
    var tiny = new SegmentArchives.Limits(1, 100, 1000000, 1000000);
    assertThatThrownBy(() -> SegmentPreparation.prepare(archive, target, tiny, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    assertThat(target).doesNotExist();
  }
}
