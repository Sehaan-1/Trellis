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
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentDescriptorLoadingTest {
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

  private SegmentDescriptor descriptor(FileBlobStore store, Path stage) throws IOException {
    Path source = root.resolve("source");
    LocalSegments.write(source, SCHEMA, List.of(new GenericRow().put("team", "red")));
    var blob = SegmentBlobs.put(store, source, stage, LIMITS, SCHEMA, 1);
    return new SegmentDescriptor(
        "events",
        TableConfig.TableType.OFFLINE,
        "segment",
        SCHEMA.name(),
        SCHEMA.fields(),
        blob,
        1);
  }

  private SegmentDescriptor with(
      SegmentDescriptor d, Schema s, BlobStore.StoredBlob blob, int count) {
    return new SegmentDescriptor(
        d.tableName(), d.tableType(), d.segmentName(), s.name(), s.fields(), blob, count);
  }

  private void empty(Path path) throws IOException {
    try (var files = Files.list(path)) {
      assertThat(files.toList()).isEmpty();
    }
  }

  @Test
  void preparesDescriptorThroughRealBlobStore() throws Exception {
    var store = store();
    var stage = stage();
    var descriptor = descriptor(store, stage);
    Path target = root.resolve("target");
    var report = SegmentBlobs.prepare(store, descriptor, stage, target, LIMITS, 1);
    assertThat(report.segment().documents()).isEqualTo(1);
    try (var segment = LocalSegments.open(target)) {
      assertThat(segment.rowCount()).isEqualTo(1);
    }
    empty(stage);
  }

  @Test
  void rejectsWrongLengthAndDocumentCountsWithoutLeavingTarget() throws Exception {
    var store = store();
    var stage = stage();
    var d = descriptor(store, stage);
    Path target = root.resolve("target");
    var shortBlob = new BlobStore.StoredBlob(d.blob().key(), d.blob().size() - 1);
    var longBlob = new BlobStore.StoredBlob(d.blob().key(), d.blob().size() + 1);
    for (var wrong : List.of(shortBlob, longBlob)) {
      assertThatThrownBy(
              () ->
                  SegmentBlobs.prepare(store, with(d, SCHEMA, wrong, 1), stage, target, LIMITS, 1))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("size mismatch");
      assertThat(target).doesNotExist();
      empty(stage);
    }
    assertThatThrownBy(
            () ->
                SegmentBlobs.prepare(store, with(d, SCHEMA, d.blob(), 0), stage, target, LIMITS, 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("document count mismatch");
    assertThat(target).doesNotExist();
    empty(stage);
    assertThatThrownBy(() -> SegmentBlobs.prepare(store, d, stage, target, LIMITS, 0))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("maxDocuments");
    assertThat(target).doesNotExist();
    empty(stage);
  }

  @Test
  void rejectsSchemaHashLimitsAndPreservesExistingTarget() throws Exception {
    var store = store();
    var stage = stage();
    var d = descriptor(store, stage);
    Path target = root.resolve("target");
    var mismatch = new Schema("other", SCHEMA.fields());
    assertThatThrownBy(
            () ->
                SegmentBlobs.prepare(
                    store, with(d, mismatch, d.blob(), 1), stage, target, LIMITS, 1))
        .isInstanceOf(IOException.class);
    assertThat(target).doesNotExist();
    empty(stage);
    var missing = new BlobStore.StoredBlob(new BlobStore.Key("0".repeat(64)), d.blob().size());
    assertThatThrownBy(
            () ->
                SegmentBlobs.prepare(store, with(d, SCHEMA, missing, 1), stage, target, LIMITS, 1))
        .isInstanceOf(IOException.class);
    var small = new SegmentArchives.Limits(d.blob().size() - 1, 100, 1000000, 1000000);
    assertThatThrownBy(() -> SegmentBlobs.prepare(store, d, stage, target, small, 1))
        .isInstanceOf(IOException.class);
    Files.createDirectory(target);
    Path marker = Files.writeString(target.resolve("marker"), "safe");
    assertThatThrownBy(() -> SegmentBlobs.prepare(store, d, stage, target, LIMITS, 1))
        .isInstanceOf(IOException.class);
    assertThat(Files.readString(marker)).isEqualTo("safe");
    empty(stage);
  }

  @Test
  void rejectsInvalidArguments() throws Exception {
    var store = store();
    var stage = stage();
    var d = descriptor(store, stage);
    Path target = root.resolve("target");
    assertThatNullPointerException()
        .isThrownBy(
            () -> SegmentBlobs.prepare(store, (SegmentDescriptor) null, stage, target, LIMITS, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentBlobs.prepare(null, d, stage, target, LIMITS, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentBlobs.prepare(store, d, null, target, LIMITS, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentBlobs.prepare(store, d, stage, null, LIMITS, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentBlobs.prepare(store, d, stage, target, null, 1));
    assertThatThrownBy(() -> SegmentBlobs.prepare(store, d, stage, target, LIMITS, -1))
        .isInstanceOf(IllegalArgumentException.class);
    empty(stage);
    assertThat(target).doesNotExist();
  }
}
