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

import io.trellis.segment.spi.SearchIndexes;
import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.lucene.store.AlreadyClosedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SearchIndexesTest {
  @TempDir Path temp;
  private static final Schema SCHEMA = new Schema("s", List.of(new Schema.Field("text", Schema.DataType.STRING, false)));
  @Test void analyzedTextUsesAndSemanticsAndSurvivesSourceClose() throws Exception {
    Path path = temp.resolve("s");
    LocalSegments.write(path, SCHEMA, List.of(new GenericRow().put("text", "Red fox jumps"), new GenericRow().put("text", "red dog"), new GenericRow().put("text", "FOX red")));
    SearchIndexes.TextIndex index;
    try (var segment = LocalSegments.open(path)) {
      assertThatThrownBy(() -> LuceneIndexes.text(segment, "missing")).isInstanceOf(IllegalArgumentException.class);
      index = LuceneIndexes.text(segment, "text");
    }
    try (index) {
      assertThat(index.search("RED fox", 10)).extracting(SearchIndexes.Hit::documentId).containsExactlyInAnyOrder(0, 2);
      assertThat(index.search("fox dog", 10)).isEmpty();
      assertThat(index.search("!!!", 10)).isEmpty();
      assertThat(index.search("red", 1)).hasSize(1);
      assertThatThrownBy(() -> index.search("red", 0)).isInstanceOf(IllegalArgumentException.class);
      assertThatNullPointerException().isThrownBy(() -> index.search(null, 1));
    }
    assertThatThrownBy(() -> index.search("", 1)).isInstanceOf(AlreadyClosedException.class);
  }

  @Test void vectorsUseEuclideanNearestAndValidateInput() throws Exception {
    var input = IntStream.range(0, 150).mapToObj(i -> new float[] {i, 0}).toList();
    try (var index = LuceneIndexes.vectors(2, input)) {
      input.get(55)[0] = 9999;
      var hits = index.nearest(new float[]{55, 0}, 3);
      assertThat(hits).hasSize(3);
      assertThat(hits.get(0).documentId()).isEqualTo(55);
      assertThat(hits.get(0).score()).isGreaterThanOrEqualTo(hits.get(1).score());
      assertThatThrownBy(() -> index.nearest(new float[]{1}, 1)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> index.nearest(new float[]{Float.NaN, 1}, 1)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> index.nearest(new float[]{0, 1}, -1)).isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(() -> LuceneIndexes.vectors(0, List.of())).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LuceneIndexes.vectors(100000, List.of())).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LuceneIndexes.vectors(2, List.of(new float[]{0}))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LuceneIndexes.vectors(2, List.of(new float[]{0, Float.POSITIVE_INFINITY}))).isInstanceOf(IllegalArgumentException.class);
    try (var empty = LuceneIndexes.vectors(2, List.of())) { assertThat(empty.nearest(new float[]{0, 0}, 2)).isEmpty(); }
  }
}
