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
import io.trellis.spi.TableConfig;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.helix.HelixDataAccessor;
import org.apache.zookeeper.KeeperException;

/** Explicit one-shot synchronization of a Helix resource to registered segment names. */
public final class SegmentResourceSync {
  private SegmentResourceSync() {}

  /**
   * Reads the table, schema and descriptor names, validates their schema snapshots, then replaces
   * the Helix segment set only if it still equals expected. Returns the immutable desired names
   * snapshot on success. Both sets must be nonempty and at most maxSegments. No blobs are checked.
   *
   * <p>The caller must keep expected stable, coordinate writers and keep table, schema and
   * descriptor metadata stable throughout the call. ZooKeeper reads may be stale and the metadata
   * reads and Helix update are not atomic. Expected-set comparison cannot fence an ABA or resource
   * deletion/recreation and does not establish controller leadership. A network or cleanup failure
   * can leave an uncertain outcome; inspect Helix and metadata before retrying. The caller owns and
   * secures both clients.
   */
  public static List<String> apply(
      HelixDataAccessor accessor,
      ZooKeeperMetadataStore metadata,
      String tableName,
      TableConfig.TableType type,
      Collection<String> expected,
      int maxSegments)
      throws IOException, KeeperException, InterruptedException {
    Objects.requireNonNull(accessor, "accessor");
    Objects.requireNonNull(metadata, "metadata");
    // Validate expected identities and limits before issuing any metadata reads.
    HelixSegmentResource.idealState(tableName, type, expected, 1, maxSegments);
    List<String> desired =
        SegmentResourceBootstrap.validatedSegments(metadata, tableName, type, maxSegments);
    HelixSegmentResource.replaceSegments(accessor, tableName, type, expected, desired, maxSegments);
    return desired;
  }
}
