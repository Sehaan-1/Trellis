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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BitmapIndexesTest {
  @TempDir Path temp;
  private static final Schema SCHEMA =
      new Schema(
          "s",
          List.of(
              new Schema.Field("name", Schema.DataType.STRING, false),
              new Schema.Field("n", Schema.DataType.LONG, false)));

  private static GenericRow row(String value, long n) {
    return new GenericRow().put("name", value).put("n", n);
  }

  @Test
  void bitmapEqualityRangeAndSnapshot() throws Exception {
    Path path = temp.resolve("s");
    LocalSegments.write(
        path,
        SCHEMA,
        List.of(row("a", Long.MIN_VALUE), row("b", 2), row("a", 5), row("c", Long.MAX_VALUE)));
    io.trellis.segment.spi.BitmapIndexes.StringIndex strings;
    io.trellis.segment.spi.BitmapIndexes.LongIndex longs;
    try (var segment = LocalSegments.open(path)) {
      strings = RoaringIndexes.strings(segment, "name");
      longs = RoaringIndexes.longs(segment, "n");
      assertThatThrownBy(() -> RoaringIndexes.strings(segment, "n"))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> RoaringIndexes.longs(segment, "nope"))
          .isInstanceOf(IllegalArgumentException.class);
    }
    int[] snapshot = strings.equalTo("a");
    assertThat(snapshot).containsExactly(0, 2);
    snapshot[0] = 42;
    assertThat(strings.equalTo("a")).containsExactly(0, 2);
    assertThat(strings.equalTo("A")).isEmpty();
    assertThatNullPointerException().isThrownBy(() -> strings.equalTo(null));
    assertThat(longs.equalTo(5)).containsExactly(2);
    assertThat(longs.equalTo(4)).isEmpty();
    assertThat(longs.between(2, 5)).containsExactly(1, 2);
    assertThat(longs.between(Long.MIN_VALUE, Long.MAX_VALUE)).containsExactly(0, 1, 2, 3);
    assertThat(longs.between(5, 2)).isEmpty();
    assertThat(longs.between(Long.MAX_VALUE, Long.MAX_VALUE)).containsExactly(3);
  }

  @Test
  void emptyIndexesValidateColumns() throws Exception {
    Path path = temp.resolve("empty");
    LocalSegments.write(path, SCHEMA, List.of());
    try (var segment = LocalSegments.open(path)) {
      assertThat(RoaringIndexes.longs(segment, "n").between(0, 1)).isEmpty();
      assertThat(RoaringIndexes.strings(segment, "name").equalTo("a")).isEmpty();
      assertThatThrownBy(() -> RoaringIndexes.longs(segment, "name"))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
