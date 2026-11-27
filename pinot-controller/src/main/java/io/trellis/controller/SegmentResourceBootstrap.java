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
import io.trellis.spi.Schema;
import io.trellis.spi.TableConfig;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.helix.HelixAdmin;
import org.apache.zookeeper.KeeperException;

/** One-time creation of a Helix segment resource from registered descriptors. */
public final class SegmentResourceBootstrap {
  private SegmentResourceBootstrap() {}

  /** Created resource identity and number of segment partitions. */
  public record Result(String resourceName, int segments) {
    public Result {
      Objects.requireNonNull(resourceName, "resourceName");
      if (resourceName.isBlank() || segments <= 0)
        throw new IllegalArgumentException("invalid bootstrap result");
    }
  }

  /**
   * Reads the table, its schema and bounded segment names, checks each descriptor's schema
   * snapshot, then creates a full-auto Helix resource. The caller must prevent concurrent metadata
   * mutation for the duration: reads and the Helix write are not a transaction, ZooKeeper versions
   * do not fence delete/recreate, and stale reads or network failures can leave an uncertain
   * outcome. The caller must reconcile Helix before retrying. Empty segment sets are rejected. The
   * cluster and OnlineOffline model must exist, and the caller owns and secures both admin and
   * metadata client. This does not check blobs, continuously reconcile changes, guarantee
   * placement, or fence leaders.
   */
  public static Result create(
      HelixAdmin admin,
      ZooKeeperMetadataStore metadata,
      String cluster,
      String tableName,
      TableConfig.TableType type,
      int replicas,
      int maxSegments)
      throws IOException, KeeperException, InterruptedException {
    Objects.requireNonNull(admin, "admin");
    Objects.requireNonNull(metadata, "metadata");
    Objects.requireNonNull(cluster, "cluster");
    Objects.requireNonNull(type, "type");
    String resource = HelixSegmentResource.resourceName(tableName, type);
    if (replicas <= 0 || maxSegments <= 0)
      throw new IllegalArgumentException("replicas and maxSegments must be positive");
    List<String> segments = validatedSegments(metadata, tableName, type, maxSegments);
    HelixSegmentResource.create(admin, cluster, tableName, type, segments, replicas, maxSegments);
    return new Result(resource, segments.size());
  }

  /** Reads and validates one non-transactional segment-name snapshot. */
  static List<String> validatedSegments(
      ZooKeeperMetadataStore metadata,
      String tableName,
      TableConfig.TableType type,
      int maxSegments)
      throws IOException, KeeperException, InterruptedException {
    TableConfig table = metadata.getTable(tableName, type).value();
    Schema schema = metadata.getSchema(table.schemaName()).value();
    List<String> segments = metadata.listSegmentNames(tableName, type, maxSegments);
    if (segments.isEmpty()) throw new IllegalArgumentException("at least one segment is required");
    for (String name : segments) {
      var descriptor = metadata.getSegment(tableName, type, name).value();
      if (!schema.name().equals(descriptor.schemaName())
          || !schema.fields().equals(descriptor.schemaFields()))
        throw new IOException("segment schema mismatch: " + name);
    }
    return segments;
  }
}
