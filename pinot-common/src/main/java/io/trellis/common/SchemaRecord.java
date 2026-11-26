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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Conversion between ordered SPI schemas and Helix records, independent of persistence. */
public final class SchemaRecord {
  private static final String FIELDS = "trellis.fields";
  private SchemaRecord() {}

  public static ZNRecord encode(Schema schema) {
    Objects.requireNonNull(schema, "schema");
    ZNRecord record = new ZNRecord(schema.name());
    List<String> names = new ArrayList<>();
    for (int i = 0; i < schema.fields().size(); i++) {
      Schema.Field field = schema.fields().get(i);
      names.add(field.name());
      record.setMapField("trellis.field." + i, Map.of("type", field.type().name(), "multiValue", Boolean.toString(field.multiValue())));
    }
    record.setListField(FIELDS, names);
    return record;
  }

  public static Schema decode(ZNRecord record) {
    Objects.requireNonNull(record, "record");
    List<String> names = record.getListField(FIELDS);
    if (names == null || names.isEmpty()) throw new IllegalArgumentException("missing schema fields");
    List<Schema.Field> fields = new ArrayList<>(names.size());
    for (int i = 0; i < names.size(); i++) {
      Map<String, String> info = record.getMapField("trellis.field." + i);
      if (info == null || info.get("type") == null || info.get("multiValue") == null) throw new IllegalArgumentException("missing field metadata");
      String multi = info.get("multiValue");
      if (!multi.equals("true") && !multi.equals("false")) throw new IllegalArgumentException("invalid multiValue");
      fields.add(new Schema.Field(names.get(i), Schema.DataType.valueOf(info.get("type")), Boolean.parseBoolean(multi)));
    }
    return new Schema(record.getId(), fields);
  }
}
