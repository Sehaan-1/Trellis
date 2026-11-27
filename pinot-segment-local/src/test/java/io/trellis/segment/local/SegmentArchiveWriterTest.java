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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentArchiveWriterTest {
  @TempDir Path root;
  private static final Schema SCHEMA =
      new Schema("games", List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
  private static final SegmentArchives.Limits LIMITS =
      new SegmentArchives.Limits(1000000, 100, 1000000, 1000000);

  private Path segment() throws IOException {
    Path path = root.resolve("source");
    LocalSegments.write(path, SCHEMA, List.of(new GenericRow().put("team", "red")));
    return path;
  }

  private static final class Output extends ByteArrayOutputStream {
    boolean closed;

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  @Test
  void roundTripsWithoutClosingCallerStream() throws Exception {
    Path segment = segment();
    Output output = new Output();
    var report = SegmentArchiveWriter.write(segment, output, LIMITS, SCHEMA, 1);
    assertThat(output.closed).isFalse();
    assertThat(report.documents()).isEqualTo(1);
    assertThat(report.files()).isPositive();
    Path archive = Files.write(root.resolve("a.zip"), output.toByteArray());
    var result = SegmentPreparation.prepare(archive, root.resolve("target"), LIMITS, SCHEMA, 1);
    assertThat(result.archive().files()).isEqualTo(report.files());
    assertThat(result.archive().expandedBytes()).isEqualTo(report.expandedBytes());
    try (var opened = LocalSegments.open(root.resolve("target"))) {
      assertThat(opened.rowCount()).isEqualTo(1);
    }
  }

  @Test
  void enforcesEveryLimitAndPreservesStreamOwnershipOnFailure() throws Exception {
    Path path = segment();
    for (var limits :
        List.of(
            new SegmentArchives.Limits(1, 100, 1000000, 1000000),
            new SegmentArchives.Limits(1000000, 1, 1000000, 1000000),
            new SegmentArchives.Limits(1000000, 100, 0, 1000000),
            new SegmentArchives.Limits(1000000, 100, 1000000, 0))) {
      Output output = new Output();
      assertThatThrownBy(() -> SegmentArchiveWriter.write(path, output, limits, SCHEMA, 1))
          .isInstanceOf(IOException.class);
      assertThat(output.closed).isFalse();
    }
    Output output = new Output();
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, output, LIMITS, SCHEMA, 0))
        .isInstanceOf(IOException.class);
    assertThat(output.size()).isZero();
    var mismatch = new Schema("other", SCHEMA.fields());
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, output, LIMITS, mismatch, 1))
        .isInstanceOf(IOException.class);
    assertThat(output.size()).isZero();
  }

  @Test
  void rejectsUnsafeNamesDirectoriesAndSymlinks() throws Exception {
    Path path = segment();
    Path unsafe = Files.createFile(path.resolve("bad name"));
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, new Output(), LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    Files.delete(unsafe);
    Path dir = Files.createDirectory(path.resolve("folder"));
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, new Output(), LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    Files.delete(dir);
    Files.createSymbolicLink(path.resolve("link"), root);
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, new Output(), LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
  }

  @Test
  void acceptsExactLimitsAndRejectsCaseCollisions() throws Exception {
    Path path = segment();
    Output first = new Output();
    var report = SegmentArchiveWriter.write(path, first, LIMITS, SCHEMA, 1);
    var exact =
        new SegmentArchives.Limits(
            first.size(), report.files(), LIMITS.maxFileBytes(), report.expandedBytes());
    Output second = new Output();
    assertThat(SegmentArchiveWriter.write(path, second, exact, SCHEMA, 1)).isEqualTo(report);
    assertThat(second.size()).isEqualTo(first.size());
    Files.createFile(path.resolve("extra"));
    Files.createFile(path.resolve("EXTRA"));
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, new Output(), LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("duplicate");
  }

  @Test
  void validatesArgumentsAndPropagatesOutputFailure() throws Exception {
    Path path = segment();
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentArchiveWriter.write(null, new Output(), LIMITS, SCHEMA, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentArchiveWriter.write(path, null, LIMITS, SCHEMA, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentArchiveWriter.write(path, new Output(), null, SCHEMA, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentArchiveWriter.write(path, new Output(), LIMITS, null, 1));
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, new Output(), LIMITS, SCHEMA, -1))
        .isInstanceOf(IllegalArgumentException.class);
    var failure =
        new java.io.OutputStream() {
          public void write(int value) throws IOException {
            throw new IOException("output failed");
          }
        };
    assertThatThrownBy(() -> SegmentArchiveWriter.write(path, failure, LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("output failed");
  }
}
