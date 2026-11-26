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
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentRoundTripTest {
  @TempDir Path temp;
  private static final Schema SCHEMA = new Schema("games", List.of(
      new Schema.Field("team", Schema.DataType.STRING, false),
      new Schema.Field("score", Schema.DataType.LONG, false)));
  private static GenericRow row(String team, long score) {
    return new GenericRow().put("team", team).put("score", score);
  }

  @Test void roundTripDictionaryAndRandomReads() throws Exception {
    Path path = temp.resolve("segment");
    LocalSegments.write(path, SCHEMA, List.of(row("z", -3), row("a", Long.MAX_VALUE), row("z", Long.MIN_VALUE), row("", 0)));
    try (var segment = LocalSegments.open(path)) {
      assertThat(segment.schema().name()).isEqualTo("games");
      assertThat(segment.schema().fields()).isEqualTo(SCHEMA.fields());
      assertThat(segment.rowCount()).isEqualTo(4);
      assertThat(segment.getLong(2, "score")).isEqualTo(Long.MIN_VALUE);
      assertThat(segment.getLong(1, "score")).isEqualTo(Long.MAX_VALUE);
      assertThat(segment.getString(0, "team")).isEqualTo("z");
      assertThat(segment.getString(3, "team")).isEmpty();
      assertThat(segment.dictionarySize("team")).isEqualTo(3);
      assertThat(segment.dictionaryValue("team", 0)).isEmpty();
      assertThat(segment.dictionaryValue("team", 1)).isEqualTo("a");
      assertThat(segment.dictionaryValue("team", 2)).isEqualTo("z");
      assertThat(segment.dictionaryOrdinal(0, "team")).isEqualTo(2);
      assertThat(segment.dictionaryOrdinal(1, "team")).isEqualTo(1);
      assertThat(segment.dictionaryOrdinal(2, "team")).isEqualTo(2);
      assertThatThrownBy(() -> segment.getLong(-1, "score")).isInstanceOf(IndexOutOfBoundsException.class);
      assertThatThrownBy(() -> segment.getString(4, "team")).isInstanceOf(IndexOutOfBoundsException.class);
      assertThatThrownBy(() -> segment.getLong(0, "team")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> segment.dictionarySize("missing")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> segment.dictionaryValue("team", 3)).isInstanceOf(IndexOutOfBoundsException.class);
      assertThatThrownBy(() -> segment.dictionaryValue("team", -1)).isInstanceOf(IndexOutOfBoundsException.class);
      var executor = Executors.newFixedThreadPool(4);
      try {
        var tasks = java.util.stream.IntStream.range(0, 64).<java.util.concurrent.Callable<String>>mapToObj(i -> () -> segment.getString(i % 4, "team")).toList();
        var results = executor.invokeAll(tasks);
        for (int i = 0; i < results.size(); i++) assertThat(results.get(i).get()).isEqualTo(List.of("z", "a", "z", "").get(i % 4));
      } finally { executor.shutdownNow(); }
    }
    try (var reopened = LocalSegments.open(path)) { assertThat(reopened.getString(1, "team")).isEqualTo("a"); }
  }

  @Test void emptySegmentHasEmptyDictionary() throws Exception {
    Path path = temp.resolve("empty");
    LocalSegments.write(path, SCHEMA, List.of());
    try (var segment = LocalSegments.open(path)) {
      assertThat(segment.rowCount()).isZero();
      assertThat(segment.dictionarySize("team")).isZero();
      assertThatThrownBy(() -> segment.dictionaryValue("team", 0)).isInstanceOf(IndexOutOfBoundsException.class);
      assertThatThrownBy(() -> segment.getString(0, "team")).isInstanceOf(IndexOutOfBoundsException.class);
    }
  }
}
