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
package io.trellis.spi.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/** Immutable, SHA-256-addressed opaque blob storage. Implementations must support concurrent calls. */
public interface BlobStore {
  record Key(String sha256) {
    public Key {
      Objects.requireNonNull(sha256, "sha256");
      if (!sha256.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("key must be a lowercase SHA-256 hex digest");
    }
  }
  record StoredBlob(Key key, long size) {
    public StoredBlob {
      Objects.requireNonNull(key, "key");
      if (size < 0) throw new IllegalArgumentException("size must be nonnegative");
    }
  }

  /**
   * Streams and publishes a complete blob, rejecting inputs larger than maxBytes. Existing
   * content is never overwritten. The caller retains ownership of source. A failure may have an
   * uncertain outcome; a retry of identical bytes is safe, provided the existing object is intact.
   */
  StoredBlob put(InputStream source, long maxBytes) throws IOException;

  /**
   * Copies the entire blob, verifying its SHA-256 before returning the byte count. The caller owns
   * destination. On any failure, including checksum mismatch, destination may contain partial or
   * corrupt bytes and must be discarded.
   */
  long copyTo(Key key, OutputStream destination) throws IOException;
}
