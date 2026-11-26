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
package io.trellis.core;

import static org.assertj.core.api.Assertions.*;

import io.trellis.segment.local.InMemorySegments;
import io.trellis.segment.local.LocalSegments;
import io.trellis.segment.spi.ImmutableSegment;
import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import java.nio.file.Path;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalQueryExecutorTest {
  @TempDir Path temp;
  private static final Schema SCHEMA =
      new Schema(
          "baseball",
          List.of(
              new Schema.Field("team", Schema.DataType.STRING, false),
              new Schema.Field("score", Schema.DataType.LONG, false)));

  private static GenericRow row(String team, long score) {
    return new GenericRow().put("team", team).put("score", score);
  }

  private ImmutableSegment segment(String name, List<GenericRow> rows) throws Exception {
    Path path = temp.resolve(name);
    LocalSegments.write(path, SCHEMA, rows);
    return LocalSegments.open(path);
  }

  @Test
  void sqlScanFilterProjectionGroupOrderAndLimitAcrossSegments() throws Exception {
    try (var first = segment("one", List.of(row("a", 3), row("b", 7)));
        var second = segment("two", List.of(row("a", 8), row("b", 4), row("c", 1)))) {
      var results =
          LocalQueryExecutor.execute(
              "games",
              List.of(first, second),
              "SELECT team, SUM(score) AS total, COUNT(*) AS n FROM games WHERE score >= 4 GROUP BY team ORDER BY total DESC LIMIT 1",
              10);
      assertThat(results.columns()).containsExactly("team", "total", "n");
      assertThat(results.rows()).hasSize(1);
      assertThat(results.rows().get(0).get(0)).isEqualTo("b");
      assertThat(((Number) results.rows().get(0).get(1)).longValue()).isEqualTo(11);
      assertThat(((Number) results.rows().get(0).get(2)).longValue()).isEqualTo(2);
      var projection =
          LocalQueryExecutor.execute(
              "games",
              List.of(first, second),
              "SELECT team, score + 1 AS adjusted FROM games WHERE team = 'a' ORDER BY score DESC",
              10);
      assertThat(projection.rows()).hasSize(2);
      assertThat(projection.rows().get(0).get(0)).isEqualTo("a");
      assertThat(((Number) projection.rows().get(0).get(1)).longValue()).isEqualTo(9);
      assertThat(((Number) projection.rows().get(1).get(1)).longValue()).isEqualTo(4);
      assertThat(first.getString(0, "team")).isEqualTo("a");
      assertThatThrownBy(() -> results.rows().clear())
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> results.rows().get(0).clear())
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> results.columns().clear())
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Test
  void emptyAggregatesAndSnapshotIsolation() throws Exception {
    try (var empty = segment("empty", List.of());
        var mutable = InMemorySegments.create(SCHEMA)) {
      var aggregate =
          LocalQueryExecutor.execute(
              "games", List.of(empty), "SELECT COUNT(*) AS n, SUM(score) AS total FROM games", 1);
      assertThat(((Number) aggregate.rows().get(0).get(0)).longValue()).isZero();
      assertThat(aggregate.rows().get(0).get(1)).isNull();
      mutable.append(row("a", 1));
      try (var snapshot = mutable.snapshot()) {
        mutable.append(row("b", 2));
        var count =
            LocalQueryExecutor.execute("games", List.of(snapshot), "SELECT COUNT(*) FROM games", 1);
        assertThat(((Number) count.rows().get(0).get(0)).longValue()).isEqualTo(1);
      }
    }
  }

  @Test
  void rejectsInvalidQueriesAndExcessOutput() throws Exception {
    try (var first = segment("s", List.of(row("a", 1), row("b", 2)))) {
      assertThatThrownBy(
              () ->
                  LocalQueryExecutor.execute("games", List.of(first), "SELECT team FROM games", 1))
          .isInstanceOf(SQLDataException.class)
          .hasMessageContaining("maxRows");
      assertThatThrownBy(
              () ->
                  LocalQueryExecutor.execute(
                      "games", List.of(first), "SELECT missing FROM games", 10))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute("games", List.of(first), "SELECT FROM", 10))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () ->
                  LocalQueryExecutor.execute(
                      "games", List.of(first), "SELECT * FROM games; SELECT * FROM games", 10))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute("games", List.of(first), "DELETE FROM games", 10))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute("games", List.of(first), "SELECT * FROM games", 0))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute(" ", List.of(first), "SELECT * FROM games", 1))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute("games", List.of(), "SELECT * FROM games", 1))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute("games", List.of(first), "SELECT * FROM GAMES", 1))
          .isInstanceOf(SQLException.class);
    }
  }

  @Test
  void rejectsMismatchedSchemas() throws Exception {
    var different =
        new Schema("other", List.of(new Schema.Field("other", Schema.DataType.STRING, false)));
    Path other = temp.resolve("other");
    LocalSegments.write(other, different, List.of());
    try (var first = segment("s", List.of());
        var second = LocalSegments.open(other)) {
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute("t", List.of(first, second), "SELECT * FROM t", 1))
          .isInstanceOf(IllegalArgumentException.class);
    }
    var otherName = new Schema("another", SCHEMA.fields());
    Path renamed = temp.resolve("renamed");
    LocalSegments.write(renamed, otherName, List.of());
    try (var first = LocalSegments.open(temp.resolve("s"));
        var second = LocalSegments.open(renamed)) {
      assertThatThrownBy(
              () -> LocalQueryExecutor.execute("t", List.of(first, second), "SELECT * FROM t", 1))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void resultCopiesInputsAndSupportsNulls() {
    var columns = new ArrayList<>(List.of("a"));
    var inner = new ArrayList<Object>(Arrays.asList((Object) null));
    var source = new ArrayList<List<Object>>();
    source.add(inner);
    var result = new QueryResult(columns, source);
    inner.set(0, 1);
    columns.clear();
    source.clear();
    assertThat(result.columns()).containsExactly("a");
    assertThat(result.rows().get(0)).containsExactly((Object) null);
    assertThatThrownBy(() -> new QueryResult(List.of("x"), List.of(List.of())))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
