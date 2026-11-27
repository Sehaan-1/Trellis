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
package io.trellis.common;

import static org.assertj.core.api.Assertions.*;

import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.util.List;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.junit.jupiter.api.Test;

class SegmentDescriptorRecordTest {
  private SegmentDescriptor descriptor() {
    return new SegmentDescriptor(
        "table / å",
        TableConfig.TableType.REALTIME,
        "segment",
        "schema",
        List.of(
            new Schema.Field("x", Schema.DataType.LONG, false),
            new Schema.Field("y", Schema.DataType.STRING, true)),
        new BlobStore.StoredBlob(new BlobStore.Key("a".repeat(64)), Long.MAX_VALUE),
        Integer.MAX_VALUE);
  }

  @Test
  void roundTripPreservesAllFieldsAndSchemaOrder() {
    var original = descriptor();
    ZNRecord encoded = SegmentDescriptorRecord.encode(original);
    assertThat(encoded.getId()).isEqualTo(original.segmentName());
    assertThat(SegmentDescriptorRecord.decode(encoded)).isEqualTo(original);
    encoded.setSimpleField("trellis.segment.table", "changed");
    assertThat(original.tableName()).isEqualTo("table / å");
    assertThatNullPointerException().isThrownBy(() -> SegmentDescriptorRecord.encode(null));
    assertThatNullPointerException().isThrownBy(() -> SegmentDescriptorRecord.decode(null));
  }

  @Test
  void rejectsMissingOrInvalidScalarFields() {
    for (String field :
        List.of(
            "trellis.segment.format",
            "trellis.segment.table",
            "trellis.segment.tableType",
            "trellis.segment.schema",
            "trellis.segment.sha256",
            "trellis.segment.bytes",
            "trellis.segment.documents")) {
      ZNRecord record = SegmentDescriptorRecord.encode(descriptor());
      record.getSimpleFields().remove(field);
      assertThatThrownBy(() -> SegmentDescriptorRecord.decode(record))
          .as(field)
          .isInstanceOf(IllegalArgumentException.class);
    }
    for (var entry :
        Map.of(
                "trellis.segment.format",
                "2",
                "trellis.segment.tableType",
                "BAD",
                "trellis.segment.sha256",
                "ABC",
                "trellis.segment.bytes",
                "-1",
                "trellis.segment.documents",
                "2147483648")
            .entrySet()) {
      ZNRecord record = SegmentDescriptorRecord.encode(descriptor());
      record.setSimpleField(entry.getKey(), entry.getValue());
      assertThatThrownBy(() -> SegmentDescriptorRecord.decode(record))
          .as(entry.getKey())
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void rejectsMissingAndMalformedSchemaMetadata() {
    ZNRecord missing = SegmentDescriptorRecord.encode(descriptor());
    missing.getListFields().remove("trellis.fields");
    assertThatThrownBy(() -> SegmentDescriptorRecord.decode(missing))
        .isInstanceOf(IllegalArgumentException.class);
    ZNRecord map = SegmentDescriptorRecord.encode(descriptor());
    map.getMapFields().remove("trellis.field.0");
    assertThatThrownBy(() -> SegmentDescriptorRecord.decode(map))
        .isInstanceOf(IllegalArgumentException.class);
    ZNRecord type = SegmentDescriptorRecord.encode(descriptor());
    type.setMapField("trellis.field.0", Map.of("type", "BAD", "multiValue", "false"));
    assertThatThrownBy(() -> SegmentDescriptorRecord.decode(type))
        .isInstanceOf(IllegalArgumentException.class);
    ZNRecord duplicate = SegmentDescriptorRecord.encode(descriptor());
    duplicate.setListField("trellis.fields", List.of("x", "x"));
    assertThatThrownBy(() -> SegmentDescriptorRecord.decode(duplicate))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
