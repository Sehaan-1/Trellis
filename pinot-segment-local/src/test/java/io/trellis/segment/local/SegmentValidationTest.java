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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentValidationTest {
  @TempDir Path temp;
  private static final Schema SCHEMA =
      new Schema("s", List.of(new Schema.Field("x", Schema.DataType.STRING, false)));

  @Test
  void rejectsUnsupportedAndInvalidInputsWithoutCommit() throws Exception {
    var unsupported = new Schema("s", List.of(new Schema.Field("x", Schema.DataType.INT, false)));
    assertThatThrownBy(
            () -> LocalSegments.write(temp.resolve("unsupported"), unsupported, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(Files.exists(temp.resolve("unsupported"))).isFalse();
    var multi = new Schema("s", List.of(new Schema.Field("x", Schema.DataType.STRING, true)));
    assertThatThrownBy(() -> LocalSegments.write(temp.resolve("multi"), multi, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> LocalSegments.write(temp.resolve("missing"), SCHEMA, List.of(new GenericRow())))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LocalSegments.open(temp.resolve("missing")))
        .isInstanceOf(java.io.IOException.class);
    assertThatThrownBy(
            () ->
                LocalSegments.write(
                    temp.resolve("null"), SCHEMA, List.of(new GenericRow().put("x", null))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                LocalSegments.write(
                    temp.resolve("wrong"), SCHEMA, List.of(new GenericRow().put("x", 1L))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                LocalSegments.write(
                    temp.resolve("unicode"), SCHEMA, List.of(new GenericRow().put("x", "\uD800"))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LocalSegments.write(temp, SCHEMA, List.of()))
        .isInstanceOf(java.nio.file.FileAlreadyExistsException.class);
  }

  @Test
  void openingMissingPathDoesNotCreateIt() {
    Path path = temp.resolve("absent");
    assertThatThrownBy(() -> LocalSegments.open(path)).isInstanceOf(java.nio.file.NoSuchFileException.class);
    assertThat(Files.exists(path)).isFalse();
  }

  @Test
  void rejectsForeignIndexMetadata() throws Exception {
    Path path = temp.resolve("foreign");
    Files.createDirectory(path);
    try (var dir = new MMapDirectory(path);
        var writer = new IndexWriter(dir, new IndexWriterConfig())) {
      writer.commit();
    }
    assertThatThrownBy(() -> LocalSegments.open(path))
        .isInstanceOf(java.io.IOException.class)
        .hasMessageContaining("metadata");
  }
}
