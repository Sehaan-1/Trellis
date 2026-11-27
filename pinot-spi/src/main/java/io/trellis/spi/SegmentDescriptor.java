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

import io.trellis.spi.storage.BlobStore;
import java.util.List;
import java.util.Objects;

/** Immutable claimed metadata for a segment archive, not proof of validity or availability. */
public record SegmentDescriptor(
    String tableName,
    TableConfig.TableType tableType,
    String segmentName,
    String schemaName,
    List<Schema.Field> schemaFields,
    BlobStore.StoredBlob blob,
    int documents) {
  public SegmentDescriptor {
    Schema.requireName(tableName);
    Objects.requireNonNull(tableType, "tableType");
    Schema.requireName(segmentName);
    Schema schema = new Schema(schemaName, schemaFields);
    schemaName = schema.name();
    schemaFields = schema.fields();
    Objects.requireNonNull(blob, "blob");
    if (documents < 0) throw new IllegalArgumentException("documents must be nonnegative");
  }

  /** Returns an immutable schema snapshot describing the archive. */
  public Schema schema() {
    return new Schema(schemaName, schemaFields);
  }
}
