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

import io.trellis.segment.spi.BitmapIndexes;
import io.trellis.segment.spi.ImmutableSegment;
import io.trellis.spi.Schema;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import org.roaringbitmap.RoaringBitmap;

/** Builds read-only, in-memory indexes from immutable segment columns. */
public final class RoaringIndexes {
  private RoaringIndexes() {}

  private static void requireColumn(ImmutableSegment segment, String column, Schema.DataType type) {
    Objects.requireNonNull(segment, "segment");
    Objects.requireNonNull(column, "column");
    var field =
        segment
            .schema()
            .field(column)
            .orElseThrow(() -> new IllegalArgumentException("unknown column: " + column));
    if (field.type() != type || field.multiValue())
      throw new IllegalArgumentException("unsupported column: " + column);
  }

  public static BitmapIndexes.StringIndex strings(ImmutableSegment segment, String column)
      throws IOException {
    requireColumn(segment, column, Schema.DataType.STRING);
    Map<String, RoaringBitmap> values = new HashMap<>();
    for (int doc = 0; doc < segment.rowCount(); doc++)
      values.computeIfAbsent(segment.getString(doc, column), key -> new RoaringBitmap()).add(doc);
    return value -> {
      Objects.requireNonNull(value, "value");
      RoaringBitmap result = values.get(value);
      return result == null ? new int[0] : result.toArray();
    };
  }

  public static BitmapIndexes.LongIndex longs(ImmutableSegment segment, String column)
      throws IOException {
    requireColumn(segment, column, Schema.DataType.LONG);
    NavigableMap<Long, RoaringBitmap> values = new TreeMap<>();
    for (int doc = 0; doc < segment.rowCount(); doc++)
      values.computeIfAbsent(segment.getLong(doc, column), key -> new RoaringBitmap()).add(doc);
    return new BitmapIndexes.LongIndex() {
      @Override
      public int[] equalTo(long value) {
        RoaringBitmap result = values.get(value);
        return result == null ? new int[0] : result.toArray();
      }

      @Override
      public int[] between(long lower, long upper) {
        if (lower > upper) return new int[0];
        RoaringBitmap result = new RoaringBitmap();
        for (RoaringBitmap bitmap : values.subMap(lower, true, upper, true).values())
          result.or(bitmap);
        return result.toArray();
      }
    };
  }
}
