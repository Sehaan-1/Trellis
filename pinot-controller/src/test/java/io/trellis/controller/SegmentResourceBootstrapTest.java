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

import static org.assertj.core.api.Assertions.*;

import io.trellis.common.ZooKeeperMetadataStore;
import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentResourceBootstrapTest {
  private TestingServer zk;
  private CuratorFramework curator;
  private ZKHelixAdmin admin;
  private ZooKeeperMetadataStore metadata;
  private static final TableConfig.TableType TYPE = TableConfig.TableType.OFFLINE;
  private static final Schema SCHEMA =
      new Schema("games", List.of(new Schema.Field("team", Schema.DataType.STRING, false)));

  private static SegmentDescriptor descriptor(
      String table, TableConfig.TableType type, String segment, Schema schema) {
    return new SegmentDescriptor(
        table,
        type,
        segment,
        schema.name(),
        schema.fields(),
        new BlobStore.StoredBlob(new BlobStore.Key("a".repeat(64)), 12),
        1);
  }

  @BeforeEach
  void start() throws Exception {
    zk = new TestingServer();
    curator =
        CuratorFrameworkFactory.newClient(
            zk.getConnectString(), new ExponentialBackoffRetry(50, 3));
    curator.start();
    if (!curator.blockUntilConnected(10, TimeUnit.SECONDS))
      throw new IllegalStateException("ZooKeeper did not start");
    metadata = new ZooKeeperMetadataStore(curator, "/metadata");
    admin = new ZKHelixAdmin(zk.getConnectString());
    assertThat(admin.addCluster("cluster")).isTrue();
    admin.addStateModelDef(
        "cluster",
        "OnlineOffline",
        BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
  }

  @AfterEach
  void stop() throws Exception {
    if (admin != null) admin.close();
    if (curator != null) curator.close();
    if (zk != null) zk.close();
  }

  private void table() throws Exception {
    metadata.createSchema(SCHEMA);
    metadata.createTable(new TableConfig("events", SCHEMA.name(), TYPE));
  }

  private String resource() {
    return HelixSegmentResource.resourceName("events", TYPE);
  }

  @Test
  void buildsRealHelixResourceFromScopedDescriptors() throws Exception {
    table();
    metadata.createSegment(descriptor("events", TYPE, "first", SCHEMA));
    metadata.createSegment(descriptor("events", TYPE, "second / å", SCHEMA));
    metadata.createSegment(descriptor("other", TYPE, "excluded", SCHEMA));
    metadata.createSegment(
        descriptor("events", TableConfig.TableType.REALTIME, "excluded", SCHEMA));
    var result = SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 2, 2);
    assertThat(result).isEqualTo(new SegmentResourceBootstrap.Result(resource(), 2));
    IdealState state = admin.getResourceIdealState("cluster", resource());
    assertThat(state.getRebalanceMode()).isEqualTo(IdealState.RebalanceMode.FULL_AUTO);
    assertThat(state.getReplicas()).isEqualTo("2");
    assertThat(state.getPartitionSet())
        .containsExactlyInAnyOrder(
            HelixSegmentResource.partitionName("first"),
            HelixSegmentResource.partitionName("second / å"));
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 2, 2))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void rejectsMissingTableSchemaEmptyOrTooManySegmentsBeforeCreatingResource() throws Exception {
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 1, 2))
        .isInstanceOf(KeeperException.NoNodeException.class);
    metadata.createTable(new TableConfig("events", SCHEMA.name(), TYPE));
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 1, 2))
        .isInstanceOf(KeeperException.NoNodeException.class);
    metadata.createSchema(SCHEMA);
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 1, 2))
        .isInstanceOf(IllegalArgumentException.class);
    metadata.createSegment(descriptor("events", TYPE, "first", SCHEMA));
    metadata.createSegment(descriptor("events", TYPE, "second", SCHEMA));
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 1, 1))
        .isInstanceOf(IOException.class);
    assertThat(admin.getResourceIdealState("cluster", resource())).isNull();
  }

  @Test
  void rejectsMismatchedSchemaSnapshots() throws Exception {
    table();
    var different =
        new Schema(SCHEMA.name(), List.of(new Schema.Field("team", Schema.DataType.LONG, false)));
    metadata.createSegment(descriptor("events", TYPE, "bad", different));
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 1, 1))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("schema mismatch");
    assertThat(admin.getResourceIdealState("cluster", resource())).isNull();
  }

  @Test
  void validatesArgumentsAndResult() throws Exception {
    assertThatNullPointerException()
        .isThrownBy(
            () -> SegmentResourceBootstrap.create(null, metadata, "cluster", "events", TYPE, 1, 1));
    assertThatNullPointerException()
        .isThrownBy(
            () -> SegmentResourceBootstrap.create(admin, null, "cluster", "events", TYPE, 1, 1));
    assertThatNullPointerException()
        .isThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, null, "events", TYPE, 1, 1));
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", null, 1, 1));
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 0, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 1, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> new SegmentResourceBootstrap.Result(null, 1));
    assertThatThrownBy(() -> new SegmentResourceBootstrap.Result(" ", 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SegmentResourceBootstrap.Result("resource", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void syncsAddedAndRemovedDescriptorsThroughRealHelix() throws Exception {
    table();
    metadata.createSegment(descriptor("events", TYPE, "a", SCHEMA));
    metadata.createSegment(descriptor("events", TYPE, "b", SCHEMA));
    SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 2, 10);
    metadata.deleteSegment("events", TYPE, "b", 0);
    metadata.createSegment(descriptor("events", TYPE, "c", SCHEMA));
    var manager =
        HelixManagerFactory.getZKHelixManager(
            "cluster", "observer", InstanceType.SPECTATOR, zk.getConnectString());
    try {
      manager.connect();
      List<String> desired =
          SegmentResourceSync.apply(
              manager.getHelixDataAccessor(), metadata, "events", TYPE, List.of("a", "b"), 10);
      assertThat(desired).containsExactly("a", "c");
      assertThatThrownBy(() -> desired.add("d")).isInstanceOf(UnsupportedOperationException.class);
      assertThat(admin.getResourceIdealState("cluster", resource()).getPartitionSet())
          .containsExactlyInAnyOrder(
              HelixSegmentResource.partitionName("a"), HelixSegmentResource.partitionName("c"));
      assertThatThrownBy(
              () ->
                  SegmentResourceSync.apply(
                      manager.getHelixDataAccessor(),
                      metadata,
                      "events",
                      TYPE,
                      List.of("a", "b"),
                      10))
          .isInstanceOf(IOException.class);
    } finally {
      manager.disconnect();
    }
  }

  @Test
  void syncRejectsSchemaMismatchEmptyCatalogAndBadArgumentsWithoutUpdating() throws Exception {
    table();
    metadata.createSegment(descriptor("events", TYPE, "a", SCHEMA));
    SegmentResourceBootstrap.create(admin, metadata, "cluster", "events", TYPE, 1, 10);
    var manager =
        HelixManagerFactory.getZKHelixManager(
            "cluster", "observer", InstanceType.SPECTATOR, zk.getConnectString());
    try {
      manager.connect();
      var accessor = manager.getHelixDataAccessor();
      var wrong =
          new Schema(SCHEMA.name(), List.of(new Schema.Field("team", Schema.DataType.LONG, false)));
      metadata.createSegment(descriptor("events", TYPE, "bad", wrong));
      assertThatThrownBy(
              () -> SegmentResourceSync.apply(accessor, metadata, "events", TYPE, List.of("a"), 10))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("schema mismatch");
      metadata.deleteSegment("events", TYPE, "bad", 0);
      metadata.deleteSegment("events", TYPE, "a", 0);
      assertThatThrownBy(
              () -> SegmentResourceSync.apply(accessor, metadata, "events", TYPE, List.of("a"), 10))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(admin.getResourceIdealState("cluster", resource()).getPartitionSet())
          .containsExactly(HelixSegmentResource.partitionName("a"));
      assertThatNullPointerException()
          .isThrownBy(
              () -> SegmentResourceSync.apply(null, metadata, "events", TYPE, List.of("a"), 10));
      assertThatNullPointerException()
          .isThrownBy(
              () -> SegmentResourceSync.apply(accessor, null, "events", TYPE, List.of("a"), 10));
      assertThatThrownBy(
              () -> SegmentResourceSync.apply(accessor, metadata, "events", TYPE, List.of(), 10))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () -> SegmentResourceSync.apply(accessor, metadata, "events", TYPE, List.of("a"), 0))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      manager.disconnect();
    }
  }
}
