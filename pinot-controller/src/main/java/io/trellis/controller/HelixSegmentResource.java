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

import io.trellis.common.SegmentHelixIdentity;
import io.trellis.spi.TableConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/** Creates a fixed initial Helix full-auto resource for a table's segments. */
public final class HelixSegmentResource {
  private HelixSegmentResource() {}

  /** Deterministic, ZooKeeper-safe resource name, distinct across table types. */
  public static String resourceName(String tableName, TableConfig.TableType type) {
    return SegmentHelixIdentity.resourceName(tableName, type);
  }

  /** Deterministic partition name for a logical segment. */
  public static String partitionName(String segmentName) {
    return SegmentHelixIdentity.partitionName(segmentName);
  }

  /** Decodes only canonical partition names produced by this class. */
  public static String segmentName(String partitionName) {
    return SegmentHelixIdentity.segmentName(partitionName);
  }

  /**
   * Constructs an independent Helix full-auto ideal state using the pre-provisioned OnlineOffline
   * state model. The segment collection must be stable during this call. No blob or metadata is
   * checked; this is an initial fixed set, not a reconciler or a server-side state model.
   */
  public static IdealState idealState(
      String tableName,
      TableConfig.TableType type,
      Collection<String> segments,
      int replicas,
      int maxSegments) {
    String resource = resourceName(tableName, type);
    Objects.requireNonNull(segments, "segments");
    if (replicas <= 0 || maxSegments <= 0)
      throw new IllegalArgumentException("replicas and maxSegments must be positive");
    var builder = new FullAutoModeISBuilder(resource);
    Set<String> names = new HashSet<>();
    for (String segment : segments) {
      String partition = partitionName(segment);
      if (!names.add(partition)) throw new IllegalArgumentException("duplicate segment name");
      if (names.size() > maxSegments) throw new IllegalArgumentException("too many segments");
      builder.add(partition);
    }
    if (names.isEmpty()) throw new IllegalArgumentException("at least one segment is required");
    builder.setNumPartitions(names.size());
    builder.setNumReplica(replicas);
    builder.setStateModel("OnlineOffline");
    return builder.build();
  }

  /**
   * Creates the resource through the caller-owned Helix admin. Cluster and OnlineOffline model must
   * already exist. Helix performs assignment only when a controller and eligible participants are
   * running. The caller must coordinate creation and reconcile uncertain failures; this call is not
   * fenced, checks no metadata, does not update existing resources and provides no transaction with
   * blob storage or descriptor writes. The caller configures ZooKeeper security.
   */
  public static void create(
      HelixAdmin admin,
      String cluster,
      String tableName,
      TableConfig.TableType type,
      Collection<String> segments,
      int replicas,
      int maxSegments) {
    Objects.requireNonNull(admin, "admin");
    Objects.requireNonNull(cluster, "cluster");
    if (!cluster.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}") || cluster.endsWith("."))
      throw new IllegalArgumentException("invalid cluster name");
    IdealState state = idealState(tableName, type, segments, replicas, maxSegments);
    admin.addResource(cluster, state.getResourceName(), state);
  }

  /**
   * Replaces the segment set of an existing non-bucketized FULL_AUTO OnlineOffline resource.
   * Expected and desired collections must be stable while this method runs and both must be
   * nonempty. Helix's updater retries version conflicts and compares the complete expected set
   * before each attempt. Existing configuration and retained partition assignments are preserved. A
   * missing, incompatible or concurrently changed resource fails rather than being created.
   *
   * <p>This is not a transaction with the segment catalog, not a leader-fencing mechanism, and
   * cannot prevent an ABA change back to the same expected set or identity recreation. The caller
   * must coordinate metadata changes and reconcile an uncertain network outcome before retrying.
   * The caller owns and secures the connected accessor. Helix, not this method, places replicas.
   */
  public static void replaceSegments(
      HelixDataAccessor accessor,
      String tableName,
      TableConfig.TableType type,
      Collection<String> expected,
      Collection<String> desired,
      int maxSegments)
      throws IOException {
    Objects.requireNonNull(accessor, "accessor");
    IdealState previous = idealState(tableName, type, expected, 1, maxSegments);
    IdealState replacement = idealState(tableName, type, desired, 1, maxSegments);
    Set<String> expectedPartitions = Set.copyOf(previous.getPartitionSet());
    Set<String> desiredPartitions = Set.copyOf(replacement.getPartitionSet());
    String resource = previous.getResourceName();
    var key = accessor.keyBuilder().idealStates(resource);
    boolean updated =
        accessor.updateProperty(
            key,
            current -> {
              if (current == null) throw new IllegalStateException("resource missing");
              IdealState state = new IdealState(current);
              if (!resource.equals(state.getResourceName())
                  || state.getBucketSize() != 0
                  || state.getRebalanceMode() != IdealState.RebalanceMode.FULL_AUTO
                  || !"OnlineOffline".equals(state.getStateModelDefRef())
                  || !expectedPartitions.equals(state.getPartitionSet())
                  || state.getNumPartitions() != expectedPartitions.size())
                throw new IllegalStateException("resource incompatible or changed");
              ZNRecord next = new ZNRecord(current);
              next.getListFields().keySet().removeIf(name -> !desiredPartitions.contains(name));
              next.getMapFields()
                  .keySet()
                  .removeIf(
                      name ->
                          expectedPartitions.contains(name) && !desiredPartitions.contains(name));
              for (String name : desiredPartitions)
                if (!next.getListFields().containsKey(name))
                  next.setListField(name, new ArrayList<>());
              IdealState result = new IdealState(next);
              result.setNumPartitions(desiredPartitions.size());
              return result.getRecord();
            },
            replacement);
    if (!updated) throw new IOException("resource update failed or expected segment set changed");
  }
}
