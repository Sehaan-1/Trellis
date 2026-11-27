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
import io.trellis.deepstore.local.FileBlobStore;
import io.trellis.segment.local.LocalSegments;
import io.trellis.segment.local.SegmentArchives;
import io.trellis.segment.local.SegmentBlobs;
import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import io.trellis.spi.TableConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentRegistrarTest {
  @TempDir Path root;
  private TestingServer server;
  private CuratorFramework client;
  private ZooKeeperMetadataStore metadata;
  private FileBlobStore blobs;
  private SegmentRegistrar registrar;
  private Path source;
  private Path stage;
  private static final Schema SCHEMA =
      new Schema("games", List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
  private static final SegmentArchives.Limits LIMITS =
      new SegmentArchives.Limits(1000000, 100, 1000000, 1000000);
  private static final TableConfig.TableType TYPE = TableConfig.TableType.OFFLINE;

  @BeforeEach
  void start() throws Exception {
    server = new TestingServer();
    client =
        CuratorFrameworkFactory.newClient(
            server.getConnectString(), new ExponentialBackoffRetry(50, 3));
    client.start();
    if (!client.blockUntilConnected(10, TimeUnit.SECONDS))
      throw new IllegalStateException("ZooKeeper did not start");
    metadata = new ZooKeeperMetadataStore(client, "/controller");
    blobs = new FileBlobStore(Files.createDirectory(root.resolve("blobs")));
    stage = Files.createDirectory(root.resolve("stage"));
    source = root.resolve("segment");
    LocalSegments.write(source, SCHEMA, List.of(new GenericRow().put("team", "red")));
    registrar = new SegmentRegistrar(blobs, metadata);
  }

  @AfterEach
  void stop() throws Exception {
    if (client != null) client.close();
    if (server != null) server.close();
  }

  @Test
  void registersAndRestoresRealSegment() throws Exception {
    var result = registrar.register("events", TYPE, "part-1", SCHEMA, source, stage, LIMITS, 1);
    assertThat(result.version()).isZero();
    var descriptor = result.descriptor();
    assertThat(descriptor.documents()).isEqualTo(1);
    assertThat(descriptor.schemaFields()).isEqualTo(SCHEMA.fields());
    assertThat(descriptor.blob().size()).isPositive();
    assertThat(metadata.getSegment("events", TYPE, "part-1").value()).isEqualTo(descriptor);
    Path target = root.resolve("restored");
    assertThat(
            SegmentBlobs.prepare(
                    blobs, descriptor.blob().key(), stage, target, LIMITS, descriptor.schema(), 1)
                .segment()
                .documents())
        .isEqualTo(1);
    try (var restored = LocalSegments.open(target)) {
      assertThat(restored.rowCount()).isEqualTo(1);
    }
    try (var files = Files.list(stage)) {
      assertThat(files.toList()).isEmpty();
    }
  }

  @Test
  void duplicateCannotReplaceMetadata() throws Exception {
    var first = registrar.register("events", TYPE, "part", SCHEMA, source, stage, LIMITS, 1);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "part", SCHEMA, source, stage, LIMITS, 1))
        .isInstanceOf(KeeperException.NodeExistsException.class);
    assertThat(metadata.getSegment("events", TYPE, "part").value()).isEqualTo(first.descriptor());
  }

  @Test
  void invalidSegmentsAndLimitsDoNotCreateMetadata() throws Exception {
    var mismatch = new Schema("wrong", SCHEMA.fields());
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "part", mismatch, source, stage, LIMITS, 1))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "part", SCHEMA, source, stage, LIMITS, 0))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(
            () ->
                registrar.register(
                    "events",
                    TYPE,
                    "part",
                    SCHEMA,
                    source,
                    stage,
                    new SegmentArchives.Limits(1, 100, 1000000, 1000000),
                    1))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(() -> metadata.getSegment("events", TYPE, "part"))
        .isInstanceOf(KeeperException.NoNodeException.class);
    try (var files = Files.list(stage)) {
      assertThat(files.toList()).isEmpty();
    }
  }

  @Test
  void validatesPublicContractsBeforeStoring() throws Exception {
    assertThatNullPointerException().isThrownBy(() -> new SegmentRegistrar(null, metadata));
    assertThatNullPointerException().isThrownBy(() -> new SegmentRegistrar(blobs, null));
    assertThatThrownBy(
            () -> registrar.register(" ", TYPE, "part", SCHEMA, source, stage, LIMITS, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "\uD800", SCHEMA, source, stage, LIMITS, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException()
        .isThrownBy(
            () -> registrar.register("events", null, "part", SCHEMA, source, stage, LIMITS, 1));
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "part", SCHEMA, source, stage, LIMITS, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SegmentRegistrar.Registration(null, 0))
        .isInstanceOf(NullPointerException.class);
    try (var files = Files.list(root.resolve("blobs"))) {
      assertThat(files.toList()).isEmpty();
    }
  }
}
