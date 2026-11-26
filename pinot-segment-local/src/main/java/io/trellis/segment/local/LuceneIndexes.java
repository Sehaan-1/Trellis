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
import io.trellis.segment.spi.SearchIndexes;
import io.trellis.spi.Schema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;

/** In-memory Lucene search indexes. Rebuild from source after process restart. */
public final class LuceneIndexes {
  private static final String FIELD = "value";
  private static final String DOC_ID = "docId";
  private LuceneIndexes() {}

  public static SearchIndexes.TextIndex text(ImmutableSegment segment, String column) throws IOException {
    Objects.requireNonNull(segment, "segment");
    Objects.requireNonNull(column, "column");
    Schema.Field field = segment.schema().field(column).orElseThrow(() -> new IllegalArgumentException("unknown column: " + column));
    if (field.type() != Schema.DataType.STRING || field.multiValue()) throw new IllegalArgumentException("text index requires a single-value string");
    ByteBuffersDirectory directory = new ByteBuffersDirectory();
    try {
      try (Analyzer analyzer = new StandardAnalyzer(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
        for (int i = 0; i < segment.rowCount(); i++) {
          Document doc = new Document();
          doc.add(new StoredField(DOC_ID, i));
          doc.add(new TextField(FIELD, segment.getString(i, column), org.apache.lucene.document.Field.Store.NO));
          writer.addDocument(doc);
        }
        writer.commit();
      }
      return new Text(directory, DirectoryReader.open(directory));
    } catch (IOException | RuntimeException e) {
      try { directory.close(); } catch (IOException close) { e.addSuppressed(close); }
      throw e;
    }
  }

  /** Vectors are assigned document IDs by list position and are copied into Lucene. */
  public static SearchIndexes.VectorIndex vectors(int dimension, List<float[]> vectors) throws IOException {
    if (dimension <= 0 || dimension > Codec.getDefault().knnVectorsFormat().getMaxDimensions(FIELD)) throw new IllegalArgumentException("unsupported dimension");
    Objects.requireNonNull(vectors, "vectors");
    ByteBuffersDirectory directory = new ByteBuffersDirectory();
    try {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        int docId = 0;
        for (float[] vector : vectors) {
          checkVector(vector, dimension);
          Document doc = new Document();
          doc.add(new StoredField(DOC_ID, docId++));
          doc.add(new KnnFloatVectorField(FIELD, vector.clone(), VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(doc);
        }
        writer.commit();
      }
      return new Vector(directory, DirectoryReader.open(directory), dimension);
    } catch (IOException | RuntimeException e) {
      try { directory.close(); } catch (IOException close) { e.addSuppressed(close); }
      throw e;
    }
  }

  private static void checkVector(float[] vector, int dimension) {
    Objects.requireNonNull(vector, "vector");
    if (vector.length != dimension) throw new IllegalArgumentException("vector dimension mismatch");
    for (float value : vector) if (!Float.isFinite(value)) throw new IllegalArgumentException("vector values must be finite");
  }

  private abstract static class Base implements AutoCloseable {
    final ByteBuffersDirectory directory;
    final DirectoryReader reader;
    final IndexSearcher searcher;
    Base(ByteBuffersDirectory directory, DirectoryReader reader) {
      this.directory = directory; this.reader = reader; this.searcher = new IndexSearcher(reader);
    }
    void ensureOpen() { if (reader.getRefCount() <= 0) throw new AlreadyClosedException("index is closed"); }
    List<SearchIndexes.Hit> find(Query query, int limit) throws IOException {
      if (reader.numDocs() == 0) return List.of();
      var hits = searcher.search(query, Math.min(limit, reader.numDocs())).scoreDocs;
      var result = new ArrayList<SearchIndexes.Hit>(hits.length);
      var stored = searcher.storedFields();
      for (var hit : hits) result.add(new SearchIndexes.Hit(stored.document(hit.doc).getField(DOC_ID).numericValue().intValue(), hit.score));
      return List.copyOf(result);
    }
    @Override public void close() throws IOException { IOUtils.close(reader, directory); }
  }

  private static void checkLimit(int limit) { if (limit <= 0) throw new IllegalArgumentException("limit must be positive"); }

  private static final class Text extends Base implements SearchIndexes.TextIndex {
    Text(ByteBuffersDirectory directory, DirectoryReader reader) { super(directory, reader); }
    @Override public List<SearchIndexes.Hit> search(String text, int limit) throws IOException {
      ensureOpen(); Objects.requireNonNull(text, "text"); checkLimit(limit);
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      int count = 0;
      try (Analyzer analyzer = new StandardAnalyzer(); TokenStream stream = analyzer.tokenStream(FIELD, text)) {
        CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
        stream.reset();
        while (stream.incrementToken()) {
          builder.add(new TermQuery(new Term(FIELD, term.toString())), BooleanClause.Occur.MUST);
          count++;
        }
        stream.end();
      }
      return count == 0 ? List.of() : find(builder.build(), limit);
    }
  }

  private static final class Vector extends Base implements SearchIndexes.VectorIndex {
    private final int dimension;
    Vector(ByteBuffersDirectory directory, DirectoryReader reader, int dimension) { super(directory, reader); this.dimension = dimension; }
    @Override public List<SearchIndexes.Hit> nearest(float[] vector, int limit) throws IOException {
      ensureOpen(); checkVector(vector, dimension); checkLimit(limit);
      if (reader.numDocs() == 0) return List.of();
      return find(new KnnFloatVectorQuery(FIELD, vector.clone(), Math.min(limit, reader.numDocs())), limit);
    }
  }
}
