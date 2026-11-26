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
import io.trellis.segment.spi.MutableSegment;
import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.RefCount;

/** Factory for append-only, volatile segments backed by Lucene in-memory doc values. */
public final class InMemorySegments {
  private InMemorySegments() {}

  public static MutableSegment create(Schema schema) throws IOException {
    LocalSegments.validateSchema(schema);
    ByteBuffersDirectory directory = new ByteBuffersDirectory();
    try {
      IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setCommitOnClose(false));
      return new Mutable(schema, directory, writer);
    } catch (IOException | RuntimeException e) {
      try { directory.close(); } catch (IOException close) { e.addSuppressed(close); }
      throw e;
    }
  }

  private static final class Mutable implements MutableSegment {
    private final Schema schema;
    private final IndexWriter writer;
    private final RefCount<ByteBuffersDirectory> directory;
    private int count;
    private boolean closed;

    Mutable(Schema schema, ByteBuffersDirectory directory, IndexWriter writer) {
      this.schema = schema;
      this.writer = writer;
      this.directory = new RefCount<>(directory) {
        @Override protected void release() throws IOException { object.close(); }
      };
    }

    private void ensureOpen() {
      if (closed) throw new AlreadyClosedException("mutable segment is closed");
    }

    @Override public Schema schema() { return schema; }
    @Override public synchronized int rowCount() { ensureOpen(); return count; }

    @Override public synchronized int append(GenericRow row) throws IOException {
      ensureOpen();
      Document document = LocalSegments.document(schema, row);
      try {
        writer.addDocument(document);
      } catch (IOException | RuntimeException e) {
        // Lucene may have consumed a document ID before failing; prevent further use.
        try { close(); } catch (IOException close) { e.addSuppressed(close); }
        throw e;
      }
      return count++;
    }

    @Override public synchronized ImmutableSegment snapshot() throws IOException {
      ensureOpen();
      DirectoryReader reader = DirectoryReader.open(writer);
      directory.incRef();
      return MappedSegment.fromReader(reader, schema, directory::decRef);
    }

    @Override public synchronized void close() throws IOException {
      if (closed) return;
      closed = true;
      try { writer.close(); } finally { directory.decRef(); }
    }
  }
}
