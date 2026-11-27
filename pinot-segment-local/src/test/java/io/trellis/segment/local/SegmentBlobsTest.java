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

import io.trellis.deepstore.local.FileBlobStore;
import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentBlobsTest {
  @TempDir Path root;
  private static final Schema SCHEMA =
      new Schema("games", List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
  private static final SegmentArchives.Limits LIMITS =
      new SegmentArchives.Limits(1000000, 100, 1000000, 1000000);

  private FileBlobStore store() throws IOException {
    return new FileBlobStore(Files.createDirectory(root.resolve("blobs")));
  }

  private Path stage() throws IOException {
    return Files.createDirectory(root.resolve("stage"));
  }

  private Path segment() throws IOException {
    Path path = root.resolve("source");
    LocalSegments.write(path, SCHEMA, List.of(new GenericRow().put("team", "red")));
    return path;
  }

  private void empty(Path path) throws IOException {
    try (var files = Files.list(path)) {
      assertThat(files.toList()).isEmpty();
    }
  }

  @Test
  void roundTripsThroughRealBlobStore() throws Exception {
    var store = store();
    Path stage = stage();
    var blob = SegmentBlobs.put(store, segment(), stage, LIMITS, SCHEMA, 1);
    assertThat(blob.size()).isPositive();
    empty(stage);
    var report =
        SegmentBlobs.prepare(store, blob.key(), stage, root.resolve("target"), LIMITS, SCHEMA, 1);
    assertThat(report.segment().documents()).isEqualTo(1);
    empty(stage);
    try (var result = LocalSegments.open(root.resolve("target"))) {
      assertThat(result.rowCount()).isEqualTo(1);
    }
  }

  @Test
  void rejectsOversizedAndCorruptedBlobsAndCleansStaging() throws Exception {
    var store = store();
    Path stage = stage();
    var blob = SegmentBlobs.put(store, segment(), stage, LIMITS, SCHEMA, 1);
    var tiny = new SegmentArchives.Limits(blob.size() - 1, 100, 1000000, 1000000);
    Path target = root.resolve("target");
    assertThatThrownBy(
            () -> SegmentBlobs.prepare(store, blob.key(), stage, target, tiny, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    empty(stage);
    assertThat(target).doesNotExist();
    Files.write(root.resolve("blobs").resolve(blob.key().sha256() + ".blob"), new byte[] {1, 2, 3});
    assertThatThrownBy(
            () -> SegmentBlobs.prepare(store, blob.key(), stage, target, LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    empty(stage);
    assertThat(target).doesNotExist();
  }

  @Test
  void invalidArchiveSchemaAndLimitsDoNotLeaveStagingOrTarget() throws Exception {
    var store = store();
    Path stage = stage();
    Path target = root.resolve("target");
    var bad = store.put(new ByteArrayInputStream(new byte[] {1, 2, 3}), 3);
    assertThatThrownBy(
            () -> SegmentBlobs.prepare(store, bad.key(), stage, target, LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    empty(stage);
    assertThat(target).doesNotExist();
    var good = SegmentBlobs.put(store, segment(), stage, LIMITS, SCHEMA, 1);
    var mismatch = new Schema("wrong", SCHEMA.fields());
    assertThatThrownBy(
            () -> SegmentBlobs.prepare(store, good.key(), stage, target, LIMITS, mismatch, 1))
        .isInstanceOf(IOException.class);
    empty(stage);
    assertThat(target).doesNotExist();
    assertThatThrownBy(
            () -> SegmentBlobs.prepare(store, good.key(), stage, target, LIMITS, SCHEMA, 0))
        .isInstanceOf(IOException.class);
    empty(stage);
    assertThat(target).doesNotExist();
    assertThatThrownBy(
            () ->
                SegmentBlobs.put(
                    store,
                    root.resolve("source"),
                    stage,
                    new SegmentArchives.Limits(1, 100, 1000000, 1000000),
                    SCHEMA,
                    1))
        .isInstanceOf(IOException.class);
    empty(stage);
  }

  @Test
  void preservesExistingTargetAndValidatesArguments() throws Exception {
    var store = store();
    Path stage = stage();
    var blob = SegmentBlobs.put(store, segment(), stage, LIMITS, SCHEMA, 1);
    Path target = Files.createDirectory(root.resolve("target"));
    Path marker = Files.writeString(target.resolve("marker"), "safe");
    assertThatThrownBy(
            () -> SegmentBlobs.prepare(store, blob.key(), stage, target, LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    assertThat(Files.readString(marker)).isEqualTo("safe");
    empty(stage);
    assertThatThrownBy(
            () -> SegmentBlobs.prepare(store, blob.key(), stage, target, LIMITS, SCHEMA, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentBlobs.put(null, root, stage, LIMITS, SCHEMA, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentBlobs.prepare(store, null, stage, target, LIMITS, SCHEMA, 1));
    Path symlink = root.resolve("link");
    Files.createSymbolicLink(symlink, stage);
    assertThatThrownBy(
            () ->
                SegmentBlobs.prepare(
                    store, blob.key(), symlink, root.resolve("new"), LIMITS, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    empty(stage);
  }
}
