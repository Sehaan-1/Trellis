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

import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.util.List;
import java.util.Objects;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Converts segment descriptors to Helix records without persisting them. */
public final class SegmentDescriptorRecord {
  private SegmentDescriptorRecord() {}

  public static ZNRecord encode(SegmentDescriptor descriptor) {
    Objects.requireNonNull(descriptor, "descriptor");
    ZNRecord record = SchemaRecord.encode(descriptor.schema());
    ZNRecord result = new ZNRecord(descriptor.segmentName());
    result.setSimpleField("trellis.segment.format", "1");
    result.setSimpleField("trellis.segment.table", descriptor.tableName());
    result.setSimpleField("trellis.segment.tableType", descriptor.tableType().name());
    result.setSimpleField("trellis.segment.schema", descriptor.schemaName());
    result.setSimpleField("trellis.segment.sha256", descriptor.blob().key().sha256());
    result.setSimpleField("trellis.segment.bytes", Long.toString(descriptor.blob().size()));
    result.setSimpleField("trellis.segment.documents", Integer.toString(descriptor.documents()));
    result.setListField("trellis.fields", record.getListField("trellis.fields"));
    for (int i = 0; i < descriptor.schemaFields().size(); i++) {
      String key = "trellis.field." + i;
      result.setMapField(key, record.getMapField(key));
    }
    return result;
  }

  /** Rejects incomplete, malformed or unsupported records. */
  public static SegmentDescriptor decode(ZNRecord record) {
    Objects.requireNonNull(record, "record");
    if (!"1".equals(record.getSimpleField("trellis.segment.format")))
      throw new IllegalArgumentException("invalid segment metadata format");
    try {
      String schemaName = required(record, "trellis.segment.schema");
      ZNRecord schemaRecord = new ZNRecord(schemaName);
      List<String> fields = record.getListField("trellis.fields");
      if (fields == null || fields.isEmpty())
        throw new IllegalArgumentException("missing schema fields");
      schemaRecord.setListField("trellis.fields", fields);
      for (int i = 0; i < fields.size(); i++) {
        String key = "trellis.field." + i;
        if (record.getMapField(key) == null)
          throw new IllegalArgumentException("missing field metadata");
        schemaRecord.setMapField(key, record.getMapField(key));
      }
      Schema schema = SchemaRecord.decode(schemaRecord);
      return new SegmentDescriptor(
          required(record, "trellis.segment.table"),
          TableConfig.TableType.valueOf(required(record, "trellis.segment.tableType")),
          record.getId(),
          schema.name(),
          schema.fields(),
          new BlobStore.StoredBlob(
              new BlobStore.Key(required(record, "trellis.segment.sha256")),
              Long.parseLong(required(record, "trellis.segment.bytes"))),
          Integer.parseInt(required(record, "trellis.segment.documents")));
    } catch (NullPointerException e) {
      throw new IllegalArgumentException("missing segment metadata", e);
    }
  }

  private static String required(ZNRecord record, String name) {
    String value = record.getSimpleField(name);
    if (value == null) throw new IllegalArgumentException("missing segment metadata: " + name);
    return value;
  }
}
