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
package io.trellis.controller;

import io.trellis.common.ZooKeeperMetadataStore;
import io.trellis.segment.local.SegmentArchives;
import io.trellis.segment.local.SegmentBlobs;
import io.trellis.segment.local.SegmentVerifier;
import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.zookeeper.KeeperException;

/** Registers trusted local segments in a blob store and the metadata store. */
public final class SegmentRegistrar {
  private final BlobStore blobs;
  private final ZooKeeperMetadataStore metadata;

  /** Dependencies remain caller-owned; their authentication and security are caller-managed. */
  public SegmentRegistrar(BlobStore blobs, ZooKeeperMetadataStore metadata) {
    this.blobs = Objects.requireNonNull(blobs, "blobs");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
  }

  /** The registered descriptor and its ZooKeeper data version. */
  public record Registration(SegmentDescriptor descriptor, int version) {
    public Registration {
      Objects.requireNonNull(descriptor, "descriptor");
      if (version < 0) throw new IllegalArgumentException("version must be nonnegative");
    }
  }

  /**
   * Verifies and stores the segment archive, then creates its descriptor. The segment and staging
   * directory must remain trusted and stable throughout; no out-of-band mutation is allowed. There
   * is no cross-store transaction. A failure, including duplicate identity or cleanup failure, may
   * leave an orphaned blob or an uncertain metadata write; reconcile before retrying. ZIPs are not
   * canonical so retries may create a different blob. This does not check table or schema
   * existence, publish an HTTP endpoint, assign a segment, or provide leader fencing.
   */
  public Registration register(
      String tableName,
      TableConfig.TableType type,
      String segmentName,
      Schema expected,
      Path segment,
      Path stagingDirectory,
      SegmentArchives.Limits limits,
      int maxDocuments)
      throws IOException, KeeperException, InterruptedException {
    requireName(tableName);
    Objects.requireNonNull(type, "type");
    requireName(segmentName);
    Objects.requireNonNull(expected, "expected");
    requireName(expected.name());
    for (Schema.Field field : expected.fields()) requireName(field.name());
    Objects.requireNonNull(segment, "segment");
    Objects.requireNonNull(stagingDirectory, "stagingDirectory");
    Objects.requireNonNull(limits, "limits");
    var report = SegmentVerifier.verify(segment, expected, maxDocuments);
    var blob = SegmentBlobs.put(blobs, segment, stagingDirectory, limits, expected, maxDocuments);
    var descriptor =
        new SegmentDescriptor(
            tableName,
            type,
            segmentName,
            expected.name(),
            expected.fields(),
            blob,
            report.documents());
    int version = metadata.createSegment(descriptor);
    return new Registration(descriptor, version);
  }

  static void requireName(String name) {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    try {
      StandardCharsets.UTF_8
          .newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(name));
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("name must be valid Unicode", e);
    }
  }
}
