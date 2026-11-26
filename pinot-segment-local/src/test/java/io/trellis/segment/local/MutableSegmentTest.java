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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.lucene.store.AlreadyClosedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MutableSegmentTest {
  @TempDir Path temp;
  private static final Schema SCHEMA =
      new Schema(
          "events",
          List.of(
              new Schema.Field("name", Schema.DataType.STRING, false),
              new Schema.Field("n", Schema.DataType.LONG, false)));

  private static GenericRow row(String name, long n) {
    return new GenericRow().put("name", name).put("n", n);
  }

  @Test
  void snapshotsArePointInTimeAndOutliveWriter() throws Exception {
    var mutable = InMemorySegments.create(SCHEMA);
    assertThat(mutable.schema().fields()).isEqualTo(SCHEMA.fields());
    var empty = mutable.snapshot();
    assertThat(empty.rowCount()).isZero();
    assertThat(empty.dictionarySize("name")).isZero();
    assertThat(mutable.append(row("z", 4))).isZero();
    var first = mutable.snapshot();
    assertThat(mutable.append(row("a", -8))).isEqualTo(1);
    var second = mutable.snapshot();
    assertThat(mutable.rowCount()).isEqualTo(2);
    mutable.close();
    mutable.close();
    assertThatThrownBy(() -> mutable.append(row("x", 1)))
        .isInstanceOf(AlreadyClosedException.class);
    assertThatThrownBy(mutable::snapshot).isInstanceOf(AlreadyClosedException.class);
    assertThatThrownBy(mutable::rowCount).isInstanceOf(AlreadyClosedException.class);
    assertThat(empty.rowCount()).isZero();
    assertThat(first.rowCount()).isEqualTo(1);
    assertThat(first.getString(0, "name")).isEqualTo("z");
    assertThat(first.dictionaryOrdinal(0, "name")).isZero();
    assertThat(second.rowCount()).isEqualTo(2);
    assertThat(second.getLong(1, "n")).isEqualTo(-8);
    assertThat(second.dictionaryValue("name", 0)).isEqualTo("a");
    assertThat(second.dictionaryOrdinal(0, "name")).isEqualTo(1);
    assertThat(RoaringIndexes.strings(second, "name").equalTo("z")).containsExactly(0);
    empty.close();
    empty.close();
    first.close();
    first.close();
    assertThat(second.getString(1, "name")).isEqualTo("a");
    var rows = new ArrayList<GenericRow>();
    for (int i = 0; i < second.rowCount(); i++)
      rows.add(row(second.getString(i, "name"), second.getLong(i, "n")));
    LocalSegments.write(temp.resolve("persisted"), second.schema(), rows);
    second.close();
    second.close();
    try (var reopened = LocalSegments.open(temp.resolve("persisted"))) {
      assertThat(reopened.rowCount()).isEqualTo(2);
      assertThat(reopened.getString(1, "name")).isEqualTo("a");
    }
  }

  @Test
  void validationFailuresDoNotConsumeIdsOrPoisonSegment() throws Exception {
    try (var mutable = InMemorySegments.create(SCHEMA)) {
      assertThatThrownBy(() -> mutable.append(new GenericRow().put("name", "x")))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> mutable.append(row("\uD800", 1)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> mutable.append(row("x".repeat(40000), 1)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> mutable.append(new GenericRow().put("name", "a").put("n", 1)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(mutable.rowCount()).isZero();
      assertThat(mutable.append(row("ok", 3))).isZero();
      try (var snap = mutable.snapshot()) {
        assertThat(snap.getString(0, "name")).isEqualTo("ok");
      }
    }
    var unsupported = new Schema("s", List.of(new Schema.Field("x", Schema.DataType.INT, false)));
    assertThatThrownBy(() -> InMemorySegments.create(unsupported))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void concurrentAppendsReceiveUniqueIdsAndPreserveRows() throws Exception {
    try (var mutable = InMemorySegments.create(SCHEMA)) {
      var executor = Executors.newFixedThreadPool(8);
      try {
        var tasks =
            IntStream.range(0, 400)
                .<java.util.concurrent.Callable<Integer>>mapToObj(
                    i -> () -> mutable.append(row("r" + i, i)))
                .toList();
        var futures = executor.invokeAll(tasks);
        assertThat(mutable.rowCount()).isEqualTo(400);
        boolean[] seen = new boolean[400];
        try (var snapshot = mutable.snapshot()) {
          for (int i = 0; i < futures.size(); i++) {
            int doc = futures.get(i).get();
            assertThat(seen[doc]).isFalse();
            seen[doc] = true;
            assertThat(snapshot.getLong(doc, "n")).isEqualTo(i);
            assertThat(snapshot.getString(doc, "name")).isEqualTo("r" + i);
          }
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }
  @Test
  void snapshotsRaceWithAppendsWithoutPartialRows() throws Exception {
    try (var mutable = InMemorySegments.create(SCHEMA)) {
      var executor = Executors.newSingleThreadExecutor();
      try {
        var appends = executor.submit(() -> {
          for (int i = 0; i < 100; i++) mutable.append(row("r" + i, i));
          return null;
        });
        for (int i = 0; i < 40; i++) {
          try (var snapshot = mutable.snapshot()) {
            int count = snapshot.rowCount();
            for (int doc = 0; doc < count; doc++) {
              assertThat(snapshot.getLong(doc, "n")).isEqualTo(doc);
              assertThat(snapshot.getString(doc, "name")).isEqualTo("r" + doc);
            }
          }
        }
        appends.get();
        assertThat(mutable.rowCount()).isEqualTo(100);
      } finally {
        executor.shutdownNow();
      }
    }
  }

}
