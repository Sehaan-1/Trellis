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

import io.trellis.spi.storage.BlobStore;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Local blob storage for a trusted directory on a filesystem supporting hard links. */
public final class FileBlobStore implements BlobStore {
  private final Path root;

  /** The existing root and its parents must be trusted, with no concurrent out-of-band writers. */
  public FileBlobStore(Path root) throws IOException {
    Objects.requireNonNull(root, "root");
    if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) throw new IOException("blob root must be an existing non-symlink directory");
    this.root = root.toRealPath();
  }

  @Override public StoredBlob put(InputStream source, long maxBytes) throws IOException {
    Objects.requireNonNull(source, "source");
    if (maxBytes < 0) throw new IllegalArgumentException("maxBytes must be nonnegative");
    try (StagedFile temp = new StagedFile(Files.createTempFile(root, ".upload-", ".tmp"))) {
      Path staged = temp.path;
      MessageDigest digest = digest();
      long size = 0;
      try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.WRITE)) {
        byte[] buffer = new byte[65536];
        int count;
        while ((count = source.read(buffer)) != -1) {
          if (count == 0) continue;
          if (count > maxBytes - size) throw new IOException("blob exceeds maxBytes");
          digest.update(buffer, 0, count);
          ByteBuffer bytes = ByteBuffer.wrap(buffer, 0, count);
          while (bytes.hasRemaining()) channel.write(bytes);
          size += count;
        }
        channel.force(true);
      }
      Key key = new Key(HexFormat.of().formatHex(digest.digest()));
      Path target = path(key);
      try {
        // A hard link makes the already-written inode visible without overwriting existing data.
        Files.createLink(target, staged);
      } catch (FileAlreadyExistsException e) {
        long existing = verifyAndCopy(key, OutputStream.nullOutputStream());
        if (existing != size) throw new IOException("existing blob length mismatch", e);
      }
      return new StoredBlob(key, size);
    }
  }

  @Override public long copyTo(Key key, OutputStream destination) throws IOException {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(destination, "destination");
    return verifyAndCopy(key, destination);
  }

  private Path path(Key key) { return root.resolve(key.sha256() + ".blob"); }
  private long verifyAndCopy(Key key, OutputStream destination) throws IOException {
    Path path = path(key);
    BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!attributes.isRegularFile()) throw new IOException("blob path is not a regular file");
    MessageDigest digest = digest();
    long size = 0;
    try (InputStream input = Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      byte[] buffer = new byte[65536];
      int count;
      while ((count = input.read(buffer)) != -1) {
        if (count == 0) continue;
        digest.update(buffer, 0, count);
        destination.write(buffer, 0, count);
        size += count;
      }
    }
    if (!key.sha256().equals(HexFormat.of().formatHex(digest.digest()))) throw new IOException("blob checksum mismatch");
    return size;
  }
  private static final class StagedFile implements Closeable {
    final Path path;
    StagedFile(Path path) { this.path = path; }
    @Override public void close() throws IOException { Files.deleteIfExists(path); }
  }
  private static MessageDigest digest() {
    try { return MessageDigest.getInstance("SHA-256"); }
    catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is unavailable", e); }
  }
}
