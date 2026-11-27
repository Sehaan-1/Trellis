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

import static org.assertj.core.api.Assertions.*;

import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentVerifierTest {
  @TempDir Path root;
  private static final Schema SCHEMA =
      new Schema(
          "games",
          List.of(
              new Schema.Field("team", Schema.DataType.STRING, false),
              new Schema.Field("score", Schema.DataType.LONG, false)));

  private static GenericRow row(String team, long score) {
    return new GenericRow().put("team", team).put("score", score);
  }

  private Path normal(String name, List<GenericRow> rows) throws IOException {
    Path path = root.resolve(name);
    LocalSegments.write(path, SCHEMA, rows);
    return path;
  }

  private Path crafted(String name, List<Document> documents) throws Exception {
    Path path = root.resolve(name);
    Files.createDirectory(path);
    var metadata =
        Map.of(
            "trellis.format",
            "1",
            "trellis.schema",
            "games",
            "trellis.fields",
            "2",
            "trellis.field.0.name",
            "team",
            "trellis.field.0.type",
            "STRING",
            "trellis.field.1.name",
            "score",
            "trellis.field.1.type",
            "LONG");
    try (var directory = new MMapDirectory(path);
        var writer =
            new IndexWriter(
                directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      writer.setLiveCommitData(metadata.entrySet());
      for (Document doc : documents) {
        writer.addDocument(doc);
        writer.commit();
      }
      if (documents.isEmpty()) writer.commit();
    }
    return path;
  }

  private Document document(byte[] team, boolean score) {
    var doc = new Document();
    doc.add(new SortedDocValuesField("team", new BytesRef(team)));
    if (score) doc.add(new NumericDocValuesField("score", 1));
    return doc;
  }

  @Test
  void validatesExpectedSchemaEmptySegmentsAndMultipleLuceneLeaves() throws Exception {
    Path path = normal("normal", List.of(row("red", 2), row("blue", -1)));
    assertThat(SegmentVerifier.verify(path, SCHEMA, 2)).isEqualTo(new SegmentVerifier.Report(2, 2));
    assertThatThrownBy(() -> SegmentVerifier.verify(path, SCHEMA, 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("maxDocuments");
    assertThatThrownBy(() -> SegmentVerifier.verify(path, SCHEMA, -1))
        .isInstanceOf(IllegalArgumentException.class);
    var different = new Schema("other", SCHEMA.fields());
    assertThatThrownBy(() -> SegmentVerifier.verify(path, different, 2))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("schema");
    Path empty = normal("empty", List.of());
    assertThat(SegmentVerifier.verify(empty, SCHEMA, 0))
        .isEqualTo(new SegmentVerifier.Report(0, 2));
    Path many =
        crafted("many", List.of(document("a".getBytes(), true), document("b".getBytes(), true)));
    assertThat(SegmentVerifier.verify(many, SCHEMA, 2)).isEqualTo(new SegmentVerifier.Report(2, 2));
  }

  @Test
  void rejectsMissingForwardValuesAndUnexpectedColumns() throws Exception {
    Path missing =
        crafted(
            "missing", List.of(document("a".getBytes(), true), document("b".getBytes(), false)));
    assertThatThrownBy(() -> SegmentVerifier.verify(missing, SCHEMA, 10))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("missing");
    Document extra = document("a".getBytes(), true);
    extra.add(new NumericDocValuesField("extra", 10));
    Path unexpected = crafted("extra", List.of(extra));
    assertThatThrownBy(() -> SegmentVerifier.verify(unexpected, SCHEMA, 10))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("column metadata");
  }

  @Test
  void rejectsNonUtf8StringDictionary() throws Exception {
    Path path = crafted("utf8", List.of(document(new byte[] {(byte) 0xc3, 0x28}, true)));
    assertThatThrownBy(() -> SegmentVerifier.verify(path, SCHEMA, 5))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("UTF-8");
  }

  @Test
  void rejectsCorruptionAndConcurrentWriter() throws Exception {
    Path path = normal("locked", List.of(row("red", 5)));
    try (var directory = new MMapDirectory(path);
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      assertThatThrownBy(() -> SegmentVerifier.verify(path, SCHEMA, 5))
          .isInstanceOf(IOException.class);
    }
    assertThat(SegmentVerifier.verify(path, SCHEMA, 5).documents()).isEqualTo(1);
    Path corrupt = normal("corrupt", List.of(row("red", 5)));
    Path data;
    try (var files = Files.list(corrupt)) {
      data =
          files.filter(p -> p.getFileName().toString().endsWith(".cfs")).findFirst().orElseThrow();
    }
    try (var file = new RandomAccessFile(data.toFile(), "rw")) {
      long offset = file.length() / 2;
      file.seek(offset);
      int b = file.read();
      file.seek(offset);
      file.write(b ^ 1);
    }
    assertThatThrownBy(() -> SegmentVerifier.verify(corrupt, SCHEMA, 5))
        .isInstanceOf(IOException.class);
  }

  @Test
  void rejectsInvalidPathsAndArguments() throws Exception {
    assertThatThrownBy(() -> SegmentVerifier.verify(root.resolve("missing"), SCHEMA, 1))
        .isInstanceOf(IOException.class);
    Path file = Files.createFile(root.resolve("file"));
    assertThatThrownBy(() -> SegmentVerifier.verify(file, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    Path symlink = root.resolve("link");
    Files.createSymbolicLink(symlink, root);
    assertThatThrownBy(() -> SegmentVerifier.verify(symlink, SCHEMA, 1))
        .isInstanceOf(IOException.class);
    assertThatNullPointerException().isThrownBy(() -> SegmentVerifier.verify(null, SCHEMA, 1));
    assertThatNullPointerException().isThrownBy(() -> SegmentVerifier.verify(root, null, 1));
    var unsupported = new Schema("s", List.of(new Schema.Field("x", Schema.DataType.INT, false)));
    assertThatThrownBy(() -> SegmentVerifier.verify(root, unsupported, 1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
