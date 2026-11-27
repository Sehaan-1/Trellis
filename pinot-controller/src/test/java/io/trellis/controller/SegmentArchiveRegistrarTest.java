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
import io.trellis.segment.local.SegmentArchiveWriter;
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

class SegmentArchiveRegistrarTest {
  @TempDir Path root;
  private TestingServer server;
  private CuratorFramework client;
  private ZooKeeperMetadataStore metadata;
  private FileBlobStore blobs;
  private SegmentArchiveRegistrar registrar;
  private Path archive;
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
    registrar = new SegmentArchiveRegistrar(blobs, metadata);
    stage = Files.createDirectory(root.resolve("stage"));
    Path segment = root.resolve("segment");
    LocalSegments.write(segment, SCHEMA, List.of(new GenericRow().put("team", "red")));
    archive = root.resolve("segment.zip");
    try (var output = Files.newOutputStream(archive)) {
      SegmentArchiveWriter.write(segment, output, LIMITS, SCHEMA, 1);
    }
  }

  @AfterEach
  void stop() throws Exception {
    if (client != null) client.close();
    if (server != null) server.close();
  }

  private void emptyStage() throws IOException {
    try (var files = Files.list(stage)) {
      assertThat(files.toList()).isEmpty();
    }
  }

  @Test
  void registersOriginalArchiveAndRestoresIt() throws Exception {
    var registered = registrar.register("events", TYPE, "one", SCHEMA, archive, stage, LIMITS, 1);
    var d = registered.descriptor();
    assertThat(registered.version()).isZero();
    assertThat(d.blob().size()).isEqualTo(Files.size(archive));
    assertThat(d.documents()).isEqualTo(1);
    assertThat(metadata.getSegment("events", TYPE, "one").value()).isEqualTo(d);
    Path target = root.resolve("restored");
    assertThat(SegmentBlobs.prepare(blobs, d, stage, target, LIMITS, 1).segment().documents())
        .isEqualTo(1);
    try (var segment = LocalSegments.open(target)) {
      assertThat(segment.rowCount()).isEqualTo(1);
    }
    emptyStage();
  }

  @Test
  void invalidArchiveOrSchemaNeverCreatesMetadataOrBlob() throws Exception {
    Path invalid = Files.write(root.resolve("bad.zip"), new byte[] {1, 2, 3});
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", SCHEMA, invalid, stage, LIMITS, 1))
        .isInstanceOf(IOException.class);
    var mismatch = new Schema("wrong", SCHEMA.fields());
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", mismatch, archive, stage, LIMITS, 1))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", SCHEMA, archive, stage, LIMITS, 0))
        .isInstanceOf(IOException.class);
    var tiny = new SegmentArchives.Limits(1, 100, 1000000, 1000000);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", SCHEMA, archive, stage, tiny, 1))
        .isInstanceOf(IOException.class);
    assertThatThrownBy(() -> metadata.getSegment("events", TYPE, "one"))
        .isInstanceOf(KeeperException.NoNodeException.class);
    try (var files = Files.list(root.resolve("blobs"))) {
      assertThat(files.toList()).isEmpty();
    }
    emptyStage();
  }

  @Test
  void duplicateLeavesOriginalDescriptorAndStagingClean() throws Exception {
    var first = registrar.register("events", TYPE, "one", SCHEMA, archive, stage, LIMITS, 1);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", SCHEMA, archive, stage, LIMITS, 1))
        .isInstanceOf(KeeperException.NodeExistsException.class);
    assertThat(metadata.getSegment("events", TYPE, "one").value()).isEqualTo(first.descriptor());
    emptyStage();
  }

  @Test
  void validatesArgumentsPathsAndSymlinks() throws Exception {
    assertThatNullPointerException().isThrownBy(() -> new SegmentArchiveRegistrar(null, metadata));
    assertThatNullPointerException().isThrownBy(() -> new SegmentArchiveRegistrar(blobs, null));
    assertThatThrownBy(
            () -> registrar.register(" ", TYPE, "one", SCHEMA, archive, stage, LIMITS, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", SCHEMA, archive, stage, LIMITS, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException()
        .isThrownBy(
            () -> registrar.register("events", null, "one", SCHEMA, archive, stage, LIMITS, 1));
    Path link = root.resolve("link.zip");
    Files.createSymbolicLink(link, archive);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", SCHEMA, link, stage, LIMITS, 1))
        .isInstanceOf(IOException.class);
    Path linkStage = root.resolve("link-stage");
    Files.createSymbolicLink(linkStage, stage);
    assertThatThrownBy(
            () -> registrar.register("events", TYPE, "one", SCHEMA, archive, linkStage, LIMITS, 1))
        .isInstanceOf(IOException.class);
    emptyStage();
  }
}
