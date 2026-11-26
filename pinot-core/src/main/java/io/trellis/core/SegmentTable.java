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

import io.trellis.segment.spi.ImmutableSegment;
import io.trellis.spi.Schema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Bridges segment values to Calcite; segment lifetime belongs to the caller. */
final class SegmentTable extends AbstractTable implements ScannableTable {
  private final List<ImmutableSegment> segments;
  private final Schema schema;

  SegmentTable(List<ImmutableSegment> segments, Schema schema) {
    this.segments = segments;
    this.schema = schema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory factory) {
    var builder = factory.builder();
    for (Schema.Field field : schema.fields()) {
      builder
          .add(
              field.name(),
              field.type() == Schema.DataType.STRING ? SqlTypeName.VARCHAR : SqlTypeName.BIGINT)
          .nullable(false);
    }
    return builder.build();
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new Enumerator<>() {
          int segmentIndex;
          int nextDocument;
          Object[] current;

          @Override
          public Object[] current() {
            if (current == null)
              throw new IllegalStateException("enumerator is not positioned on a row");
            return current;
          }

          @Override
          public boolean moveNext() {
            while (segmentIndex < segments.size()) {
              ImmutableSegment segment = segments.get(segmentIndex);
              if (nextDocument >= segment.rowCount()) {
                segmentIndex++;
                nextDocument = 0;
                continue;
              }
              int doc = nextDocument++;
              Object[] values = new Object[schema.fields().size()];
              try {
                for (int i = 0; i < values.length; i++) {
                  Schema.Field field = schema.fields().get(i);
                  values[i] =
                      field.type() == Schema.DataType.STRING
                          ? segment.getString(doc, field.name())
                          : segment.getLong(doc, field.name());
                }
              } catch (IOException e) {
                throw new UncheckedIOException("segment read failed", e);
              }
              current = values;
              return true;
            }
            current = null;
            return false;
          }

          @Override
          public void reset() {
            segmentIndex = 0;
            nextDocument = 0;
            current = null;
          }

          @Override
          public void close() {
            current = null;
          }
        };
      }
    };
  }
}
