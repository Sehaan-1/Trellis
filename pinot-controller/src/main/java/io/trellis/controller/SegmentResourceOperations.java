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
import io.trellis.common.ZooKeeperMetadataStore;
import io.trellis.spi.TableConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.zookeeper.KeeperException;

/** Explicit operations on a table's desired Helix resource, not its live replica state. */
public final class SegmentResourceOperations {
  private final ZooKeeperMetadataStore metadata;
  private final HelixAdmin admin;
  private final HelixDataAccessor accessor;
  private final String cluster;
  private final int replicas;
  private final int maxSegments;

  /**
   * Dependencies remain caller-owned and must point to the intended secured metadata and cluster.
   * Callers must coordinate concurrent writers; these operations do not fence leaders or provide a
   * transaction across metadata and Helix.
   */
  public SegmentResourceOperations(
      ZooKeeperMetadataStore metadata,
      HelixAdmin admin,
      HelixDataAccessor accessor,
      String cluster,
      int replicas,
      int maxSegments) {
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.admin = Objects.requireNonNull(admin, "admin");
    this.accessor = Objects.requireNonNull(accessor, "accessor");
    this.cluster = Objects.requireNonNull(cluster, "cluster");
    if (!cluster.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}") || cluster.endsWith("."))
      throw new IllegalArgumentException("invalid cluster name");
    if (replicas <= 0 || maxSegments <= 0)
      throw new IllegalArgumentException("limits must be positive");
    this.replicas = replicas;
    this.maxSegments = maxSegments;
  }

  /** An immutable desired-state observation; not a live or consistent replica snapshot. */
  public record Snapshot(String resourceName, String replicas, List<String> segments) {
    public Snapshot {
      Objects.requireNonNull(resourceName, "resourceName");
      Objects.requireNonNull(replicas, "replicas");
      segments = List.copyOf(segments);
    }
  }

  boolean usesMetadata(ZooKeeperMetadataStore store) {
    return metadata == store;
  }

  /** Reads the existing desired Helix state. Missing and unreadable resources both fail. */
  public Snapshot snapshot(String tableName, TableConfig.TableType type) throws IOException {
    String resource = HelixSegmentResource.resourceName(tableName, type);
    IdealState state = accessor.getProperty(accessor.keyBuilder().idealStates(resource));
    if (state == null) throw new IOException("resource missing or unreadable");
    if (!resource.equals(state.getResourceName())
        || state.getBucketSize() != 0
        || state.getRebalanceMode() != IdealState.RebalanceMode.FULL_AUTO
        || !"OnlineOffline".equals(state.getStateModelDefRef())
        || state.getReplicas() == null
        || state.getNumPartitions() != state.getPartitionSet().size()
        || state.getPartitionSet().size() > maxSegments)
      throw new IOException("resource incompatible or exceeds segment limit");
    try {
      List<String> segments = new ArrayList<>(state.getPartitionSet().size());
      for (String partition : state.getPartitionSet())
        segments.add(SegmentHelixIdentity.segmentName(partition));
      segments.sort(String::compareTo);
      return new Snapshot(resource, state.getReplicas(), segments);
    } catch (IllegalArgumentException e) {
      throw new IOException("invalid resource partition", e);
    }
  }

  /** A segment and instance IDs observed in Helix's ONLINE state, not verified reachability. */
  public record ObservedSegment(String segmentName, List<String> onlineInstances) {
    public ObservedSegment {
      Objects.requireNonNull(segmentName, "segmentName");
      onlineInstances = List.copyOf(onlineInstances);
    }
  }

  /** An immutable external-view observation, which may be stale or incomplete. */
  public record Observed(String resourceName, List<ObservedSegment> segments) {
    public Observed {
      Objects.requireNonNull(resourceName, "resourceName");
      segments = List.copyOf(segments);
    }
  }

  /**
   * Reads Helix's eventually consistent external view. Missing and unreadable views fail; absent
   * partitions are not evidence that a segment has no replicas. Only exact ONLINE states are
   * returned, without checking live instances, reachability, blob integrity or query readiness. The
   * configured segment cap and per-segment instance cap reject oversized outputs, but Helix
   * materializes the entire external view before either limit is checked.
   */
  public Observed observed(String tableName, TableConfig.TableType type, int maxInstances)
      throws IOException {
    if (maxInstances <= 0) throw new IllegalArgumentException("maxInstances must be positive");
    String resource = HelixSegmentResource.resourceName(tableName, type);
    ExternalView view = accessor.getProperty(accessor.keyBuilder().externalView(resource));
    if (view == null) throw new IOException("external view missing or unreadable");
    if (!resource.equals(view.getResourceName())
        || view.getBucketSize() != 0
        || view.getPartitionSet().size() > maxSegments)
      throw new IOException("external view incompatible or exceeds segment limit");
    List<ObservedSegment> segments = new ArrayList<>(view.getPartitionSet().size());
    try {
      for (String partition : view.getPartitionSet()) {
        String segment = SegmentHelixIdentity.segmentName(partition);
        var states = view.getStateMap(partition);
        if (states == null || states.size() > maxInstances)
          throw new IOException("invalid or excessive instance states");
        List<String> online = new ArrayList<>();
        for (var entry : states.entrySet()) {
          if (entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null)
            throw new IOException("invalid instance state");
          if ("ONLINE".equals(entry.getValue())) online.add(entry.getKey());
        }
        online.sort(String::compareTo);
        segments.add(new ObservedSegment(segment, online));
      }
    } catch (IllegalArgumentException e) {
      throw new IOException("invalid external view partition", e);
    }
    segments.sort(java.util.Comparator.comparing(ObservedSegment::segmentName));
    return new Observed(resource, segments);
  }

  /** Creates one resource from stable, schema-checked metadata. */
  public SegmentResourceBootstrap.Result bootstrap(String tableName, TableConfig.TableType type)
      throws IOException, KeeperException, InterruptedException {
    return SegmentResourceBootstrap.create(
        admin, metadata, cluster, tableName, type, replicas, maxSegments);
  }

  /** One-shot sync from stable metadata, only if the resource has the expected segment set. */
  public List<String> sync(
      String tableName, TableConfig.TableType type, Collection<String> expected)
      throws IOException, KeeperException, InterruptedException {
    return SegmentResourceSync.apply(accessor, metadata, tableName, type, expected, maxSegments);
  }
}
