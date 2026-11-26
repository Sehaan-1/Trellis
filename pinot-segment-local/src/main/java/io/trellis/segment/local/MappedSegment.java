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

import io.trellis.segment.spi.ImmutableSegment;
import io.trellis.spi.Schema;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;

/** Each operation obtains its own DocValues iterator; concurrent reads are supported. */
final class MappedSegment implements ImmutableSegment {
  private final Closeable release;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final DirectoryReader reader;
  private final Schema schema;

  private MappedSegment(Closeable release, DirectoryReader reader, Schema schema) {
    this.release = release;
    this.reader = reader;
    this.schema = schema;
  }

  /** Reader and release are owned by the returned snapshot. */
  static MappedSegment fromReader(DirectoryReader reader, Schema schema, Closeable release) {
    return new MappedSegment(release, reader, schema);
  }

  static MappedSegment open(Path path) throws IOException {
    if (Files.notExists(path)) throw new NoSuchFileException(path.toString());
    if (!Files.isDirectory(path)) throw new NotDirectoryException(path.toString());
    MMapDirectory directory = new MMapDirectory(path);
    DirectoryReader reader = null;
    try {
      reader = DirectoryReader.open(directory);
      Schema schema = readSchema(reader.getIndexCommit().getUserData());
      if (reader.hasDeletions()) throw new IOException("deleted documents are not supported");
      return new MappedSegment(directory, reader, schema);
    } catch (IOException | RuntimeException e) {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException close) {
          e.addSuppressed(close);
        }
      try {
        directory.close();
      } catch (IOException close) {
        e.addSuppressed(close);
      }
      throw e;
    }
  }

  private static Schema readSchema(Map<String, String> m) throws IOException {
    try {
      if (!"1".equals(m.get("trellis.format")))
        throw new IllegalArgumentException("unknown format");
      int count = Integer.parseInt(m.get("trellis.fields"));
      if (count < 1 || count > m.size() / 2)
        throw new IllegalArgumentException("invalid field count");
      var fields = new ArrayList<Schema.Field>(count);
      for (int i = 0; i < count; i++) {
        fields.add(
            new Schema.Field(
                m.get("trellis.field." + i + ".name"),
                Schema.DataType.valueOf(m.get("trellis.field." + i + ".type")),
                false));
      }
      Schema schema = new Schema(m.get("trellis.schema"), fields);
      LocalSegments.validateSchema(schema);
      return schema;
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IOException("invalid segment metadata", e);
    }
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public int rowCount() {
    return reader.maxDoc();
  }

  private void document(int doc) {
    if (doc < 0 || doc >= rowCount()) throw new IndexOutOfBoundsException("document: " + doc);
  }

  private void column(String name, Schema.DataType type) {
    Objects.requireNonNull(name, "column");
    Schema.Field field =
        schema
            .field(name)
            .orElseThrow(() -> new IllegalArgumentException("unknown column: " + name));
    if (field.type() != type) throw new IllegalArgumentException("wrong column type: " + name);
  }

  @Override
  public long getLong(int doc, String column) throws IOException {
    document(doc);
    column(column, Schema.DataType.LONG);
    NumericDocValues values = MultiDocValues.getNumericValues(reader, column);
    if (values == null || !values.advanceExact(doc))
      throw new IOException("missing numeric value: " + column);
    return values.longValue();
  }

  private SortedDocValues strings(String column) throws IOException {
    column(column, Schema.DataType.STRING);
    SortedDocValues values = MultiDocValues.getSortedValues(reader, column);
    if (values == null && rowCount() != 0)
      throw new IOException("missing string column: " + column);
    return values;
  }

  @Override
  public int dictionaryOrdinal(int doc, String column) throws IOException {
    document(doc);
    SortedDocValues values = strings(column);
    if (values == null || !values.advanceExact(doc))
      throw new IOException("missing string value: " + column);
    return values.ordValue();
  }

  @Override
  public String getString(int doc, String column) throws IOException {
    document(doc);
    SortedDocValues values = strings(column);
    if (values == null || !values.advanceExact(doc))
      throw new IOException("missing string value: " + column);
    return values.lookupOrd(values.ordValue()).utf8ToString();
  }

  @Override
  public int dictionarySize(String column) throws IOException {
    SortedDocValues values = strings(column);
    return values == null ? 0 : values.getValueCount();
  }

  @Override
  public String dictionaryValue(String column, int ordinal) throws IOException {
    SortedDocValues values = strings(column);
    int count = values == null ? 0 : values.getValueCount();
    if (ordinal < 0 || ordinal >= count) throw new IndexOutOfBoundsException("ordinal: " + ordinal);
    return values.lookupOrd(ordinal).utf8ToString();
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) IOUtils.close(reader, release);
  }
}
