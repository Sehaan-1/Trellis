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
import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

/** Creates immutable Lucene doc-values segments; each destination must not already exist. */
public final class LocalSegments {
  private LocalSegments() {}

  /**
   * Writes one segment. The parent directory must exist. On failure the destination may remain and
   * must be removed by the caller; no incomplete commit is published. Rows are consumed once. A row
   * must have exactly the schema fields, with non-null String or Long values as appropriate.
   */
  public static void write(Path destination, Schema schema, Iterable<GenericRow> rows)
      throws IOException {
    Objects.requireNonNull(destination, "destination");
    validateSchema(schema);
    Objects.requireNonNull(rows, "rows");
    Files.createDirectory(destination);
    try (MMapDirectory directory = new MMapDirectory(destination);
        IndexWriter writer =
            new IndexWriter(directory, new IndexWriterConfig().setCommitOnClose(false))) {
      for (GenericRow row : rows) {
        Document doc = document(schema, row);
        writer.addDocument(doc);
      }
      writer.setLiveCommitData(metadata(schema).entrySet());
      writer.commit();
    }
  }

  /** Converts and validates the entire row before it reaches a writer. */
  static Document document(Schema schema, GenericRow row) {
    Objects.requireNonNull(row, "row");
    Map<String, Object> values = row.values();
    if (values.size() != schema.fields().size())
      throw new IllegalArgumentException("row fields do not match schema");
    Document doc = new Document();
    for (Schema.Field field : schema.fields()) {
      Object value = values.get(field.name());
      if (field.type() == Schema.DataType.STRING && value instanceof String string) {
        validateString(string);
        BytesRef bytes = new BytesRef(string);
        if (bytes.length > IndexWriter.MAX_TERM_LENGTH)
          throw new IllegalArgumentException("string exceeds Lucene doc-values limit: " + field.name());
        doc.add(new SortedDocValuesField(field.name(), bytes));
      } else if (field.type() == Schema.DataType.LONG && value instanceof Long number) {
        doc.add(new NumericDocValuesField(field.name(), number));
      } else {
        throw new IllegalArgumentException("missing or invalid value for column: " + field.name());
      }
    }
    return doc;
  }

  /** Opens a committed segment backed by Lucene memory-mapped files. */
  public static ImmutableSegment open(Path path) throws IOException {
    return MappedSegment.open(Objects.requireNonNull(path, "path"));
  }

  static void validateSchema(Schema schema) {
    Objects.requireNonNull(schema, "schema");
    validateString(schema.name());
    for (Schema.Field f : schema.fields()) {
      validateString(f.name());
      if (f.multiValue()
          || (f.type() != Schema.DataType.STRING && f.type() != Schema.DataType.LONG)) {
        throw new IllegalArgumentException("unsupported field: " + f.name());
      }
    }
  }

  private static void validateString(String value) {
    try {
      StandardCharsets.UTF_8
          .newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(java.nio.CharBuffer.wrap(value));
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("string is not valid Unicode", e);
    }
  }

  private static Map<String, String> metadata(Schema schema) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("trellis.format", "1");
    metadata.put("trellis.schema", schema.name());
    metadata.put("trellis.fields", Integer.toString(schema.fields().size()));
    for (int i = 0; i < schema.fields().size(); i++) {
      Schema.Field f = schema.fields().get(i);
      metadata.put("trellis.field." + i + ".name", f.name());
      metadata.put("trellis.field." + i + ".type", f.type().name());
    }
    return metadata;
  }
}
