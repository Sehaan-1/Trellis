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
package io.trellis.spi;

import static org.assertj.core.api.Assertions.*;

import io.trellis.spi.storage.BlobStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SegmentDescriptorTest {
  private static final BlobStore.StoredBlob BLOB =
      new BlobStore.StoredBlob(new BlobStore.Key("a".repeat(64)), 100);

  @Test
  void retainsImmutableSchemaSnapshotAndIdentity() {
    var fields = new ArrayList<>(List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
    var descriptor =
        new SegmentDescriptor(
            "events", TableConfig.TableType.OFFLINE, "seg", "schema", fields, BLOB, 7);
    fields.clear();
    assertThat(descriptor.schemaFields()).hasSize(1);
    assertThat(descriptor.schema().fields()).isEqualTo(descriptor.schemaFields());
    assertThat(descriptor.schema().name()).isEqualTo("schema");
    assertThatThrownBy(() -> descriptor.schemaFields().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(descriptor)
        .isEqualTo(
            new SegmentDescriptor(
                "events",
                TableConfig.TableType.OFFLINE,
                "seg",
                "schema",
                descriptor.schemaFields(),
                BLOB,
                7));
  }

  @Test
  void rejectsInvalidArguments() {
    var fields = List.of(new Schema.Field("x", Schema.DataType.LONG, false));
    assertThatThrownBy(
            () ->
                new SegmentDescriptor(
                    " ", TableConfig.TableType.OFFLINE, "s", "n", fields, BLOB, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new SegmentDescriptor("t", TableConfig.TableType.OFFLINE, "", "n", fields, BLOB, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new SegmentDescriptor("t", TableConfig.TableType.OFFLINE, "s", "", fields, BLOB, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new SegmentDescriptor(
                    "t", TableConfig.TableType.OFFLINE, "s", "n", List.of(), BLOB, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new SegmentDescriptor(
                    "t",
                    TableConfig.TableType.OFFLINE,
                    "s",
                    "n",
                    List.of(fields.get(0), fields.get(0)),
                    BLOB,
                    0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new SegmentDescriptor(
                    "t", TableConfig.TableType.OFFLINE, "s", "n", fields, BLOB, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new SegmentDescriptor(
                    null, TableConfig.TableType.OFFLINE, "s", "n", fields, BLOB, 0));
    assertThatNullPointerException()
        .isThrownBy(() -> new SegmentDescriptor("t", null, "s", "n", fields, BLOB, 0));
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new SegmentDescriptor("t", TableConfig.TableType.OFFLINE, "s", "n", null, BLOB, 0));
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new SegmentDescriptor(
                    "t", TableConfig.TableType.OFFLINE, "s", "n", fields, null, 0));
  }
}
