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
package io.trellis.common;

import static org.assertj.core.api.Assertions.*;

import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZooKeeperMetadataStoreTest {
  private TestingServer server;
  private CuratorFramework client;
  private ZooKeeperMetadataStore store;

  private static Schema schema(String name) {
    return new Schema(name, List.of(new Schema.Field("x", Schema.DataType.STRING, false)));
  }

  private CuratorFramework newClient() throws Exception {
    CuratorFramework c =
        CuratorFrameworkFactory.newClient(
            server.getConnectString(), new ExponentialBackoffRetry(50, 3));
    c.start();
    if (!c.blockUntilConnected(10, TimeUnit.SECONDS)) {
      c.close();
      throw new IllegalStateException("ZooKeeper did not start");
    }
    return c;
  }

  @BeforeEach
  void start() throws Exception {
    server = new TestingServer();
    client = newClient();
    store = new ZooKeeperMetadataStore(client, "/trellis/test");
  }

  @AfterEach
  void stop() throws Exception {
    if (client != null) client.close();
    if (server != null) server.close();
  }

  @Test
  void schemaCrudAndVersionChecksSurviveClientReplacement() throws Exception {
    var original = schema("name/with space");
    assertThat(store.createSchema(original)).isZero();
    assertThatThrownBy(() -> store.createSchema(original))
        .isInstanceOf(KeeperException.NodeExistsException.class);
    var loaded = store.getSchema(original.name());
    assertThat(loaded.version()).isZero();
    assertThat(loaded.value().name()).isEqualTo(original.name());
    assertThat(loaded.value().fields()).isEqualTo(original.fields());
    var changed =
        new Schema(original.name(), List.of(new Schema.Field("v", Schema.DataType.LONG, true)));
    assertThat(store.updateSchema(changed, 0)).isEqualTo(1);
    assertThatThrownBy(() -> store.updateSchema(original, 0))
        .isInstanceOf(KeeperException.BadVersionException.class);
    assertThatThrownBy(() -> store.deleteSchema(original.name(), 0))
        .isInstanceOf(KeeperException.BadVersionException.class);
    assertThatThrownBy(() -> store.deleteSchema(original.name(), -1))
        .isInstanceOf(IllegalArgumentException.class);
    try (var another = newClient()) {
      var second = new ZooKeeperMetadataStore(another, "/trellis/test");
      assertThat(second.getSchema(original.name()).value().fields()).isEqualTo(changed.fields());
      second.deleteSchema(original.name(), 1);
    }
    assertThatThrownBy(() -> store.getSchema(original.name()))
        .isInstanceOf(KeeperException.NoNodeException.class);
  }

  @Test
  void hybridTableTypesAreIndependentAndVersioned() throws Exception {
    var offline = new TableConfig("events", "s", TableConfig.TableType.OFFLINE);
    var realtime = new TableConfig("events", "s", TableConfig.TableType.REALTIME);
    assertThat(store.createTable(offline)).isZero();
    assertThat(store.createTable(realtime)).isZero();
    var changed = new TableConfig("events", "other", TableConfig.TableType.OFFLINE);
    assertThat(store.updateTable(changed, 0)).isEqualTo(1);
    assertThat(store.getTable("events", TableConfig.TableType.OFFLINE).value()).isEqualTo(changed);
    assertThat(store.getTable("events", TableConfig.TableType.REALTIME).value())
        .isEqualTo(realtime);
    assertThatThrownBy(() -> store.updateTable(offline, 0))
        .isInstanceOf(KeeperException.BadVersionException.class);
    store.deleteTable("events", TableConfig.TableType.OFFLINE, 1);
    assertThatThrownBy(() -> store.getTable("events", TableConfig.TableType.OFFLINE))
        .isInstanceOf(KeeperException.NoNodeException.class);
    assertThat(store.getTable("events", TableConfig.TableType.REALTIME).version()).isZero();
  }

  @Test
  void competingWritersCannotBothUpdateSameVersion() throws Exception {
    store.createSchema(schema("s"));
    try (var another = newClient()) {
      var other = new ZooKeeperMetadataStore(another, "/trellis/test");
      var executor = Executors.newFixedThreadPool(2);
      var start = new CountDownLatch(1);
      try {
        java.util.concurrent.Callable<Boolean> left =
            () -> {
              start.await();
              try {
                store.updateSchema(schema("s"), 0);
                return true;
              } catch (KeeperException.BadVersionException e) {
                return false;
              }
            };
        java.util.concurrent.Callable<Boolean> right =
            () -> {
              start.await();
              try {
                other.updateSchema(schema("s"), 0);
                return true;
              } catch (KeeperException.BadVersionException e) {
                return false;
              }
            };
        var a = executor.submit(left);
        var b = executor.submit(right);
        start.countDown();
        assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
            .containsExactlyInAnyOrder(true, false);
        assertThat(store.getSchema("s").version()).isEqualTo(1);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  void corruptAndWrongIdentityRecordsAreRejected() throws Exception {
    store.createSchema(schema("s"));
    String path = "/trellis/test/schemas/cw";
    client.setData().forPath(path, new byte[] {1, 2, 3});
    assertThatThrownBy(() -> store.getSchema("s")).isInstanceOf(IOException.class);
    ZNRecord other = SchemaRecord.encode(schema("different"));
    other.setSimpleField("trellis.metadata.version", "1");
    client.setData().forPath(path, new ZNRecordSerializer().serialize(other));
    assertThatThrownBy(() -> store.getSchema("s")).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> store.getSchema("\uD800"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ZooKeeperMetadataStore(client, "relative"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static SegmentDescriptor segment(
      String table, TableConfig.TableType type, String name, int rows) {
    return new SegmentDescriptor(
        table,
        type,
        name,
        "schema",
        List.of(new Schema.Field("x", Schema.DataType.STRING, false)),
        new BlobStore.StoredBlob(new BlobStore.Key("a".repeat(64)), 123),
        rows);
  }

  @Test
  void segmentCrudUsesVersionsAndIndependentTableIdentities() throws Exception {
    var a = segment("table / å", TableConfig.TableType.OFFLINE, "segment / 1", 3);
    var b = segment(a.tableName(), TableConfig.TableType.REALTIME, a.segmentName(), 4);
    var c = segment("different", a.tableType(), a.segmentName(), 5);
    assertThat(store.createSegment(a)).isZero();
    assertThat(store.createSegment(b)).isZero();
    assertThat(store.createSegment(c)).isZero();
    assertThatThrownBy(() -> store.createSegment(a))
        .isInstanceOf(KeeperException.NodeExistsException.class);
    assertThat(store.getSegment(a.tableName(), a.tableType(), a.segmentName()).value())
        .isEqualTo(a);
    var changed = segment(a.tableName(), a.tableType(), a.segmentName(), 10);
    assertThat(store.updateSegment(changed, 0)).isEqualTo(1);
    assertThatThrownBy(() -> store.updateSegment(a, 0))
        .isInstanceOf(KeeperException.BadVersionException.class);
    assertThatThrownBy(() -> store.deleteSegment(a.tableName(), a.tableType(), a.segmentName(), 0))
        .isInstanceOf(KeeperException.BadVersionException.class);
    assertThatThrownBy(() -> store.deleteSegment(a.tableName(), a.tableType(), a.segmentName(), -1))
        .isInstanceOf(IllegalArgumentException.class);
    try (var another = newClient()) {
      var other = new ZooKeeperMetadataStore(another, "/trellis/test");
      assertThat(other.getSegment(a.tableName(), a.tableType(), a.segmentName()).value())
          .isEqualTo(changed);
      other.deleteSegment(a.tableName(), a.tableType(), a.segmentName(), 1);
    }
    assertThatThrownBy(() -> store.getSegment(a.tableName(), a.tableType(), a.segmentName()))
        .isInstanceOf(KeeperException.NoNodeException.class);
    assertThat(store.getSegment(b.tableName(), b.tableType(), b.segmentName()).value())
        .isEqualTo(b);
    assertThat(store.getSegment(c.tableName(), c.tableType(), c.segmentName()).value())
        .isEqualTo(c);
  }

  @Test
  void segmentRecordsRejectCorruptionWrongIdentitiesAndInvalidNames() throws Exception {
    var original = segment("table", TableConfig.TableType.OFFLINE, "segment", 1);
    store.createSegment(original);
    String path = "/trellis/test/segments/OFFLINE/dGFibGU/c2VnbWVudA";
    client.setData().forPath(path, new byte[] {1, 2, 3});
    assertThatThrownBy(() -> store.getSegment("table", TableConfig.TableType.OFFLINE, "segment"))
        .isInstanceOf(IOException.class);
    for (var other :
        List.of(
            segment("other", TableConfig.TableType.OFFLINE, "segment", 1),
            segment("table", TableConfig.TableType.REALTIME, "segment", 1),
            segment("table", TableConfig.TableType.OFFLINE, "other", 1))) {
      ZNRecord record = SegmentDescriptorRecord.encode(other);
      record.setSimpleField("trellis.metadata.version", "1");
      client.setData().forPath(path, new ZNRecordSerializer().serialize(record));
      assertThatThrownBy(() -> store.getSegment("table", TableConfig.TableType.OFFLINE, "segment"))
          .isInstanceOf(IOException.class);
    }
    assertThatThrownBy(() -> store.getSegment("table", TableConfig.TableType.OFFLINE, "\uD800"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> store.createSegment(null));
    assertThatNullPointerException().isThrownBy(() -> store.updateSegment(null, 0));
    assertThatNullPointerException().isThrownBy(() -> store.getSegment("table", null, "segment"));
  }

  @Test
  void competingSegmentUpdatesOnlyOneSucceeds() throws Exception {
    var original = segment("table", TableConfig.TableType.OFFLINE, "segment", 1);
    store.createSegment(original);
    try (var another = newClient()) {
      var other = new ZooKeeperMetadataStore(another, "/trellis/test");
      var executor = Executors.newFixedThreadPool(2);
      var start = new CountDownLatch(1);
      try {
        java.util.concurrent.Callable<Boolean> left =
            () -> {
              start.await();
              try {
                store.updateSegment(original, 0);
                return true;
              } catch (KeeperException.BadVersionException e) {
                return false;
              }
            };
        java.util.concurrent.Callable<Boolean> right =
            () -> {
              start.await();
              try {
                other.updateSegment(original, 0);
                return true;
              } catch (KeeperException.BadVersionException e) {
                return false;
              }
            };
        var a = executor.submit(left);
        var b = executor.submit(right);
        start.countDown();
        assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
            .containsExactlyInAnyOrder(true, false);
        assertThat(store.getSegment("table", TableConfig.TableType.OFFLINE, "segment").version())
            .isEqualTo(1);
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
