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
package io.trellis.deepstore.local;

import static org.assertj.core.api.Assertions.*;

import io.trellis.spi.storage.BlobStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileBlobStoreTest {
  @TempDir Path root;
  private static final byte[] ABC = "abc".getBytes(StandardCharsets.UTF_8);
  private static final String ABC_HASH = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
  private static final String EMPTY_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  private List<String> names() throws IOException { try (var files = Files.list(root)) { return files.map(p -> p.getFileName().toString()).sorted().toList(); } }

  @Test void storesDeduplicatesAndVerifiesBytes() throws Exception {
    var store = new FileBlobStore(root);
    var source = new ByteArrayInputStream(ABC) { boolean closed; @Override public void close() { closed = true; } };
    var result = store.put(source, 3);
    assertThat(source.closed).isFalse();
    assertThat(result.key().sha256()).isEqualTo(ABC_HASH);
    assertThat(result.size()).isEqualTo(3);
    var output = new ByteArrayOutputStream() { boolean closed; @Override public void close() { closed = true; } };
    assertThat(store.copyTo(result.key(), output)).isEqualTo(3);
    assertThat(output.toByteArray()).isEqualTo(ABC);
    assertThat(output.closed).isFalse();
    assertThat(store.put(new ByteArrayInputStream(ABC), 3)).isEqualTo(result);
    assertThat(store.put(new ByteArrayInputStream(new byte[0]), 0).key().sha256()).isEqualTo(EMPTY_HASH);
    assertThat(names()).containsExactly(ABC_HASH + ".blob", EMPTY_HASH + ".blob");
  }

  @Test void invalidInputAndInterruptedStreamLeaveNoPublishedBlob() throws Exception {
    var store = new FileBlobStore(root);
    assertThatThrownBy(() -> store.put(new ByteArrayInputStream(ABC), 2)).isInstanceOf(IOException.class).hasMessageContaining("maxBytes");
    assertThatThrownBy(() -> store.put(new ByteArrayInputStream(ABC), -1)).isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> store.put(null, 1));
    InputStream broken = new InputStream() {
      int calls;
      @Override public int read() throws IOException { if (calls++ == 0) return 1; throw new IOException("source failed"); }
      @Override public int read(byte[] b, int off, int len) throws IOException { if (calls++ == 0) { b[off] = 1; return 1; } throw new IOException("source failed"); }
    };
    assertThatThrownBy(() -> store.put(broken, 10)).isInstanceOf(IOException.class).hasMessageContaining("source failed");
    assertThat(names()).isEmpty();
    assertThatThrownBy(() -> new FileBlobStore(root.resolve("missing"))).isInstanceOf(IOException.class);
  }

  @Test void corruptionAndSymlinksAreRejectedWithoutOverwrite() throws Exception {
    var store = new FileBlobStore(root);
    var key = store.put(new ByteArrayInputStream(ABC), 3).key();
    Path path = root.resolve(key.sha256() + ".blob");
    Files.writeString(path, "bad");
    var output = new ByteArrayOutputStream();
    assertThatThrownBy(() -> store.copyTo(key, output)).isInstanceOf(IOException.class).hasMessageContaining("checksum");
    assertThat(output.toString(StandardCharsets.UTF_8)).isEqualTo("bad");
    assertThatThrownBy(() -> store.put(new ByteArrayInputStream(ABC), 3)).isInstanceOf(IOException.class).hasMessageContaining("checksum");
    assertThat(Files.readString(path)).isEqualTo("bad");
    Files.delete(path);
    Path outside = Files.createTempFile(root.getParent(), "untrusted", ".txt");
    try {
      Files.writeString(outside, "abc"); Files.createSymbolicLink(path, outside);
      assertThatThrownBy(() -> store.copyTo(key, new ByteArrayOutputStream())).isInstanceOf(IOException.class);
      assertThatThrownBy(() -> store.put(new ByteArrayInputStream(ABC), 3)).isInstanceOf(IOException.class);
    } finally { Files.deleteIfExists(outside); }
    assertThat(names()).containsExactly(key.sha256() + ".blob");
  }

  @Test void concurrentIdenticalUploadsAreIdempotent() throws Exception {
    var store = new FileBlobStore(root); var executor = Executors.newFixedThreadPool(8);
    try {
      var tasks = IntStream.range(0, 30).<java.util.concurrent.Callable<BlobStore.StoredBlob>>mapToObj(i -> () -> store.put(new ByteArrayInputStream(ABC), 3)).toList();
      var results = executor.invokeAll(tasks);
      for (var result : results) assertThat(result.get().key().sha256()).isEqualTo(ABC_HASH);
      assertThat(names()).containsExactly(ABC_HASH + ".blob");
    } finally { executor.shutdownNow(); }
  }

  @Test void validatesKeysAndMissingBlobs() throws Exception {
    assertThatThrownBy(() -> new BlobStore.Key("../escape")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BlobStore.Key(ABC_HASH.toUpperCase())).isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> new BlobStore.Key(null));
    assertThatThrownBy(() -> new BlobStore.StoredBlob(new BlobStore.Key(ABC_HASH), -1)).isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> new BlobStore.StoredBlob(null, 0));
    var store = new FileBlobStore(root);
    assertThatThrownBy(() -> store.copyTo(new BlobStore.Key(ABC_HASH), new ByteArrayOutputStream())).isInstanceOf(java.nio.file.NoSuchFileException.class);
    assertThatNullPointerException().isThrownBy(() -> store.copyTo(null, new ByteArrayOutputStream()));
    assertThatNullPointerException().isThrownBy(() -> store.copyTo(new BlobStore.Key(ABC_HASH), null));
  }
}
