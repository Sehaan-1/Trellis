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

import io.trellis.spi.TableConfig;
import java.util.Objects;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Encodes table identity in a Helix ZNRecord. Does not read or write ZooKeeper. */
public final class TableConfigRecord {
  private static final String SCHEMA = "schemaName";
  private static final String TYPE = "tableType";

  private TableConfigRecord() {}

  public static ZNRecord encode(TableConfig config) {
    Objects.requireNonNull(config, "config");
    ZNRecord record = new ZNRecord(config.tableName());
    record.setSimpleField(SCHEMA, config.schemaName());
    record.setSimpleField(TYPE, config.type().name());
    return record;
  }

  /** Rejects incomplete or unknown records instead of inventing defaults. */
  public static TableConfig decode(ZNRecord record) {
    Objects.requireNonNull(record, "record");
    String schema = record.getSimpleField(SCHEMA);
    String type = record.getSimpleField(TYPE);
    if (schema == null || type == null)
      throw new IllegalArgumentException("missing table metadata");
    return new TableConfig(record.getId(), schema, TableConfig.TableType.valueOf(type));
  }
}
