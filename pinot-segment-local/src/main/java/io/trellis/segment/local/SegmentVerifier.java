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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

/** Validates Trellis segment metadata, Lucene integrity and required forward values. */
public final class SegmentVerifier {
  private SegmentVerifier() {}

  public record Report(int documents, int fields) {}

  /**
   * Checks a trusted, stable local segment against the expected schema and document limit. Acquires
   * Lucene's write lock; callers must not mutate the directory out of band. Lucene integrity checks
   * may use substantial CPU and memory and this method is not a security boundary for arbitrary
   * untrusted content. This does not publish the segment or validate its archive.
   */
  public static Report verify(Path path, Schema expected, int maxDocuments) throws IOException {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(expected, "expected");
    if (maxDocuments < 0) throw new IllegalArgumentException("maxDocuments must be nonnegative");
    BasicFileAttributes attrs =
        Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!attrs.isDirectory()) throw new IOException("segment path must be a non-symlink directory");
    LocalSegments.validateSchema(expected);
    try (MMapDirectory directory = new MMapDirectory(path);
        CheckIndex check = new CheckIndex(directory);
        DirectoryReader reader = DirectoryReader.open(directory);
        ImmutableSegment segment = LocalSegments.open(path)) {
      if (!expected.name().equals(segment.schema().name())
          || !expected.fields().equals(segment.schema().fields()))
        throw new IOException("segment schema mismatch");
      if (reader.hasDeletions() || reader.maxDoc() != segment.rowCount())
        throw new IOException("invalid segment document state");
      if (reader.maxDoc() > maxDocuments) throw new IOException("segment exceeds maxDocuments");
      check.setDoSlowChecks(true);
      if (!check.checkIndex().clean) throw new IOException("Lucene integrity check failed");
      for (var context : reader.leaves()) validateLeaf(context.reader(), expected);
      return new Report(reader.maxDoc(), expected.fields().size());
    }
  }

  private static void validateLeaf(LeafReader leaf, Schema schema) throws IOException {
    if (leaf.getFieldInfos().size() != schema.fields().size())
      throw new IOException("unexpected or missing column metadata");
    for (Schema.Field field : schema.fields()) {
      FieldInfo info = leaf.getFieldInfos().fieldInfo(field.name());
      DocValuesType expected =
          field.type() == Schema.DataType.STRING ? DocValuesType.SORTED : DocValuesType.NUMERIC;
      if (info == null
          || info.getDocValuesType() != expected
          || info.getIndexOptions() != IndexOptions.NONE
          || info.hasNorms()
          || info.getPointDimensionCount() != 0
          || info.getVectorDimension() != 0)
        throw new IOException("invalid column metadata: " + field.name());
      if (field.type() == Schema.DataType.STRING) {
        SortedDocValues values = leaf.getSortedDocValues(field.name());
        if (values == null) throw new IOException("missing string column: " + field.name());
        var decoder =
            StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        for (int ordinal = 0; ordinal < values.getValueCount(); ordinal++) {
          BytesRef value = values.lookupOrd(ordinal);
          try {
            decoder.decode(ByteBuffer.wrap(value.bytes, value.offset, value.length));
          } catch (CharacterCodingException e) {
            throw new IOException("invalid UTF-8 in column: " + field.name(), e);
          }
        }
        for (int doc = 0; doc < leaf.maxDoc(); doc++)
          if (!values.advanceExact(doc))
            throw new IOException("missing string value: " + field.name());
      } else {
        NumericDocValues values = leaf.getNumericDocValues(field.name());
        if (values == null) throw new IOException("missing numeric column: " + field.name());
        for (int doc = 0; doc < leaf.maxDoc(); doc++)
          if (!values.advanceExact(doc))
            throw new IOException("missing numeric value: " + field.name());
      }
    }
  }
}
