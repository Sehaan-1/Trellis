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
import io.trellis.spi.GenericRow;
import io.trellis.spi.Schema;
import io.trellis.spi.TableConfig;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ControllerRuntimeTest {
  @TempDir Path root;
  private TestingServer zk;
  private CuratorFramework curator;
  private ZKHelixAdmin admin;
  private ZooKeeperMetadataStore metadata;
  private HelixManager manager;

  @BeforeEach
  void start() throws Exception {
    zk = new TestingServer();
    curator =
        CuratorFrameworkFactory.newClient(
            zk.getConnectString(), new ExponentialBackoffRetry(50, 3));
    curator.start();
    if (!curator.blockUntilConnected(10, TimeUnit.SECONDS))
      throw new IllegalStateException("ZooKeeper did not start");
    metadata = new ZooKeeperMetadataStore(curator, "/controller");
    admin = new ZKHelixAdmin(zk.getConnectString());
    assertThat(admin.addCluster("cluster")).isTrue();
    manager =
        HelixManagerFactory.getZKHelixManager(
            "cluster", "controller", InstanceType.CONTROLLER, zk.getConnectString());
  }

  @AfterEach
  void stop() throws Exception {
    if (manager != null) manager.disconnect();
    if (admin != null) admin.close();
    if (curator != null) curator.close();
    if (zk != null) zk.close();
  }

  @Test
  void startsHttpAndHelixAndClosesWithoutOwningCurator() throws Exception {
    var runtime = ControllerRuntime.start(manager, URI.create("http://127.0.0.1:0/"), metadata);
    int port = runtime.port();
    assertThat(port).isPositive();
    assertThat(runtime.isConnected()).isTrue();
    try {
      var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
      var status =
          http.send(
              HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/controller/status"))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(status.statusCode()).isEqualTo(200);
      assertThat(status.headers().firstValue("Cache-Control")).contains("no-store");
      var statusJson = new com.fasterxml.jackson.databind.ObjectMapper().readTree(status.body());
      assertThat(statusJson.path("connected").isBoolean()).isTrue();
      assertThat(statusJson.path("connected").booleanValue()).isTrue();
      assertThat(statusJson.path("leader").isBoolean()).isTrue();

      var body =
          "{\"name\":\"games\",\"fields\":[{\"name\":\"team\",\"type\":\"STRING\",\"multiValue\":false}]}";
      var request =
          HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/schemas"))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();
      assertThat(http.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
          .isEqualTo(201);
      assertThat(metadata.getSchema("games").value().fields()).hasSize(1);
    } finally {
      runtime.close();
    }
    runtime.close();
    assertThat(runtime.isConnected()).isFalse();
    assertThat(runtime.isLeader()).isFalse();
    assertThat(manager.isConnected()).isFalse();
    assertThat(curator.getZookeeperClient().isConnected()).isTrue();
    assertThatThrownBy(runtime::port).isInstanceOf(IllegalStateException.class);
    assertThat(metadata.getSchema("games").value().name()).isEqualTo("games");
  }

  @Test
  void statusResourceReportsClosedController() throws Exception {
    assertThatNullPointerException().isThrownBy(() -> new ControllerStatusResource(null));
    var controller = HelixController.start(manager);
    var resource = new ControllerStatusResource(controller);
    controller.close();
    var response = resource.get();
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getHeaderString("Cache-Control")).isEqualTo("no-store");
    assertThat(response.getEntity()).isEqualTo(new ControllerStatusResource.Status(false, false));
  }

  @Test
  void httpStartupFailuresDisconnectManager() throws Exception {
    assertThatThrownBy(
            () -> ControllerRuntime.start(manager, URI.create("https://127.0.0.1:1/"), metadata))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(manager.isConnected()).isFalse();
    try (var socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();
      assertThatThrownBy(
              () ->
                  ControllerRuntime.start(
                      manager, URI.create("http://127.0.0.1:" + port + "/"), metadata))
          .isInstanceOf(Exception.class);
      assertThat(manager.isConnected()).isFalse();
    }
    assertThat(curator.getZookeeperClient().isConnected()).isTrue();
  }

  @Test
  void validatesArgumentsBeforeAcquiringManager() {
    URI bind = URI.create("http://127.0.0.1:0/");
    assertThatNullPointerException()
        .isThrownBy(() -> ControllerRuntime.start(null, bind, metadata));
    assertThatNullPointerException()
        .isThrownBy(() -> ControllerRuntime.start(manager, null, metadata));
    assertThatNullPointerException().isThrownBy(() -> ControllerRuntime.start(manager, bind, null));
    assertThat(manager.isConnected()).isFalse();
  }

  @Test
  void runtimeCanEnableUploadOnlyWithSameMetadataStore() throws Exception {
    Path stage = Files.createDirectory(root.resolve("stage"));
    var blobs = new FileBlobStore(Files.createDirectory(root.resolve("blobs")));
    var limits = new SegmentArchives.Limits(1000, 10, 1000, 1000);
    var upload = new SegmentArchiveUpload(metadata, blobs, stage, limits, 1);
    var wrong = new ZooKeeperMetadataStore(curator, "/other");
    assertThatThrownBy(
            () ->
                ControllerRuntime.start(manager, URI.create("http://127.0.0.1:0/"), wrong, upload))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(manager.isConnected()).isFalse();
    try (var runtime =
        ControllerRuntime.start(manager, URI.create("http://127.0.0.1:0/"), metadata, upload)) {
      var request =
          HttpRequest.newBuilder(
                  URI.create(
                      "http://127.0.0.1:"
                          + runtime.port()
                          + "/tables/OFFLINE/missing/segments/x/content"))
              .header("Content-Type", "application/octet-stream")
              .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1}))
              .build();
      assertThat(
              HttpClient.newHttpClient()
                  .send(request, HttpResponse.BodyHandlers.ofString())
                  .statusCode())
          .isEqualTo(404);
    }
    assertThat(manager.isConnected()).isFalse();
  }

  @Test
  void integratedRuntimeUploadsAndBootstrapsRegisteredSegment() throws Exception {
    admin.addStateModelDef(
        "cluster",
        "OnlineOffline",
        BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
    var schema =
        new Schema(
            "games", java.util.List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
    metadata.createSchema(schema);
    metadata.createTable(new TableConfig("events", schema.name(), TableConfig.TableType.OFFLINE));
    Path stage = Files.createDirectory(root.resolve("stage"));
    var blobs = new FileBlobStore(Files.createDirectory(root.resolve("blobs")));
    var limits = new SegmentArchives.Limits(1000000, 100, 1000000, 1000000);
    var config =
        new ControllerRuntime.IntegratedConfig(
            URI.create("http://127.0.0.1:0/"), stage, limits, 1, 1, 10);
    Path source = root.resolve("source");
    LocalSegments.write(source, schema, java.util.List.of(new GenericRow().put("team", "red")));
    var zip = new java.io.ByteArrayOutputStream();
    SegmentArchiveWriter.write(source, zip, limits, schema, 1);
    try (var runtime = ControllerRuntime.startIntegrated(manager, admin, metadata, blobs, config)) {
      var http = HttpClient.newHttpClient();
      String base = "http://127.0.0.1:" + runtime.port();
      var status =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/controller/status")).GET().build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(status.statusCode()).isEqualTo(200);
      assertThat(
              new com.fasterxml.jackson.databind.ObjectMapper()
                  .readTree(status.body())
                  .path("connected")
                  .booleanValue())
          .isTrue();

      var upload =
          HttpRequest.newBuilder(URI.create(base + "/tables/OFFLINE/events/segments/one/content"))
              .header("Content-Type", "application/octet-stream")
              .POST(HttpRequest.BodyPublishers.ofByteArray(zip.toByteArray()))
              .build();
      var uploaded = http.send(upload, HttpResponse.BodyHandlers.ofString());
      assertThat(uploaded.statusCode()).as("upload: %s", uploaded.body()).isEqualTo(201);
      assertThat(
              metadata
                  .getSegment("events", TableConfig.TableType.OFFLINE, "one")
                  .value()
                  .documents())
          .isEqualTo(1);
      var bootstrap =
          HttpRequest.newBuilder(URI.create(base + "/tables/OFFLINE/events/assignment"))
              .POST(HttpRequest.BodyPublishers.noBody())
              .build();
      var created = http.send(bootstrap, HttpResponse.BodyHandlers.ofString());
      assertThat(created.statusCode()).as("bootstrap: %s", created.body()).isEqualTo(201);
      var resource = HelixSegmentResource.resourceName("events", TableConfig.TableType.OFFLINE);
      assertThat(admin.getResourceIdealState("cluster", resource).getPartitionSet())
          .containsExactly(HelixSegmentResource.partitionName("one"));
    }
    assertThat(manager.isConnected()).isFalse();
    assertThat(curator.getZookeeperClient().isConnected()).isTrue();
    assertThat(admin.getClusters()).contains("cluster");
  }

  @Test
  void integratedUploadHonorsConfiguredConcurrency() throws Exception {
    var schema =
        new Schema(
            "games", java.util.List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
    metadata.createSchema(schema);
    metadata.createTable(new TableConfig("events", "games", TableConfig.TableType.OFFLINE));
    Path stage = Files.createDirectory(root.resolve("stage"));
    var blobs = new FileBlobStore(Files.createDirectory(root.resolve("blobs")));
    var limits = new SegmentArchives.Limits(1000, 10, 1000, 1000);
    var config =
        new ControllerRuntime.IntegratedConfig(
            URI.create("http://127.0.0.1:0/"), stage, limits, 1, 1, 1, 2);
    var upload = config.createUpload(metadata, blobs);
    var entered = new java.util.concurrent.CountDownLatch(2);
    var release = new java.util.concurrent.CountDownLatch(1);
    java.util.function.Supplier<java.io.InputStream> blocked =
        () ->
            new java.io.InputStream() {
              @Override
              public int read() throws java.io.IOException {
                entered.countDown();
                try {
                  if (!release.await(10, TimeUnit.SECONDS))
                    throw new java.io.IOException("timed out");
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new java.io.IOException(e);
                }
                throw new java.io.IOException("intentional failure");
              }
            };
    var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      var first =
          executor.submit(
              () -> upload.upload("events", TableConfig.TableType.OFFLINE, "one", blocked.get()));
      var second =
          executor.submit(
              () -> upload.upload("events", TableConfig.TableType.OFFLINE, "two", blocked.get()));
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(
              () ->
                  upload.upload(
                      "events",
                      TableConfig.TableType.OFFLINE,
                      "three",
                      java.io.InputStream.nullInputStream()))
          .isInstanceOf(SegmentArchiveUpload.TooManyUploadsException.class);
      release.countDown();
      assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
          .isInstanceOf(java.util.concurrent.ExecutionException.class)
          .hasCauseInstanceOf(java.io.IOException.class);
      assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS))
          .isInstanceOf(java.util.concurrent.ExecutionException.class)
          .hasCauseInstanceOf(java.io.IOException.class);
      // A metadata lookup after both failures proves permits were released.
      assertThatThrownBy(
              () ->
                  upload.upload(
                      "missing",
                      TableConfig.TableType.OFFLINE,
                      "three",
                      java.io.InputStream.nullInputStream()))
          .isInstanceOf(org.apache.zookeeper.KeeperException.NoNodeException.class);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void integratedRuntimeValidatesConfigAndCleansUpAfterBindFailure() throws Exception {
    Path stage = Files.createDirectory(root.resolve("stage"));
    var blobs = new FileBlobStore(Files.createDirectory(root.resolve("blobs")));
    var limits = new SegmentArchives.Limits(1000, 10, 1000, 1000);
    URI bind = URI.create("http://127.0.0.1:0/");
    assertThatNullPointerException()
        .isThrownBy(() -> new ControllerRuntime.IntegratedConfig(null, stage, limits, 1, 1, 1));
    assertThatThrownBy(() -> new ControllerRuntime.IntegratedConfig(bind, stage, limits, -1, 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ControllerRuntime.IntegratedConfig(bind, stage, limits, 1, 0, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ControllerRuntime.IntegratedConfig(bind, stage, limits, 1, 1, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(
            new ControllerRuntime.IntegratedConfig(bind, stage, limits, 1, 1, 1)
                .maxConcurrentUploads())
        .isEqualTo(1);
    assertThatThrownBy(
            () -> new ControllerRuntime.IntegratedConfig(bind, stage, limits, 1, 1, 1, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new ControllerRuntime.IntegratedConfig(bind, stage, limits, 1, 1, 1, -1))
        .isInstanceOf(IllegalArgumentException.class);
    var missing =
        new ControllerRuntime.IntegratedConfig(bind, root.resolve("missing"), limits, 1, 1, 1);
    assertThatThrownBy(
            () -> ControllerRuntime.startIntegrated(manager, admin, metadata, blobs, missing))
        .isInstanceOf(java.io.IOException.class);
    assertThat(manager.isConnected()).isFalse();
    try (var socket = new ServerSocket(0)) {
      var occupied =
          new ControllerRuntime.IntegratedConfig(
              URI.create("http://127.0.0.1:" + socket.getLocalPort() + "/"),
              stage,
              limits,
              1,
              1,
              1);
      assertThatThrownBy(
              () -> ControllerRuntime.startIntegrated(manager, admin, metadata, blobs, occupied))
          .isInstanceOf(Exception.class);
      assertThat(manager.isConnected()).isFalse();
    }
    assertThat(curator.getZookeeperClient().isConnected()).isTrue();
  }
}
