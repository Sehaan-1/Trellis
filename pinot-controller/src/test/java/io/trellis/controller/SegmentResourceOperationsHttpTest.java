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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
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
import org.apache.helix.model.ExternalView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentResourceOperationsHttpTest {
  private TestingServer zk;
  private CuratorFramework curator;
  private ZKHelixAdmin admin;
  private HelixManager manager;
  private ZooKeeperMetadataStore metadata;
  private SegmentResourceOperations operations;
  private MetadataHttpServer server;
  private static final TableConfig.TableType TYPE = TableConfig.TableType.OFFLINE;
  private static final Schema SCHEMA =
      new Schema("games", List.of(new Schema.Field("team", Schema.DataType.STRING, false)));

  private SegmentDescriptor descriptor(String name, Schema schema) {
    return new SegmentDescriptor(
        "events",
        TYPE,
        name,
        schema.name(),
        schema.fields(),
        new BlobStore.StoredBlob(new BlobStore.Key("a".repeat(64)), 1),
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
    metadata.createSchema(SCHEMA);
    metadata.createTable(new TableConfig("events", SCHEMA.name(), TYPE));
    metadata.createSegment(descriptor("a", SCHEMA));
    admin = new ZKHelixAdmin(zk.getConnectString());
    assertThat(admin.addCluster("cluster")).isTrue();
    admin.addStateModelDef(
        "cluster",
        "OnlineOffline",
        BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
    manager =
        HelixManagerFactory.getZKHelixManager(
            "cluster", "observer", InstanceType.SPECTATOR, zk.getConnectString());
    manager.connect();
    operations =
        new SegmentResourceOperations(
            metadata, admin, manager.getHelixDataAccessor(), "cluster", 2, 10);
  }

  @AfterEach
  void stop() throws Exception {
    if (server != null) server.close();
    if (manager != null) manager.disconnect();
    if (admin != null) admin.close();
    if (curator != null) curator.close();
    if (zk != null) zk.close();
  }

  private HttpResponse<String> request(String method, String body) throws Exception {
    var builder =
        HttpRequest.newBuilder(
            URI.create("http://127.0.0.1:" + server.port() + "/tables/OFFLINE/events/assignment"));
    if (body != null) builder.header("Content-Type", "application/json");
    return HttpClient.newHttpClient()
        .send(
            builder
                .method(
                    method,
                    body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void bootstrapsReadsAndSyncsDesiredState() throws Exception {
    server =
        MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata, null, operations);
    assertThat(request("POST", null).statusCode()).isEqualTo(201);
    var read = request("GET", null);
    assertThat(read.statusCode()).isEqualTo(200);
    assertThat(read.body()).contains("replicas", "segments", "a");
    var snapshot = operations.snapshot("events", TYPE);
    assertThat(snapshot.segments()).containsExactly("a");
    assertThatThrownBy(() -> snapshot.segments().add("x"))
        .isInstanceOf(UnsupportedOperationException.class);
    metadata.createSegment(descriptor("b", SCHEMA));
    var sync = request("PUT", "{\"expectedSegments\":[\"a\"]}");
    assertThat(sync.statusCode()).as(sync.body()).isEqualTo(200);
    assertThat(sync.body()).contains("a", "b");
    assertThat(operations.snapshot("events", TYPE).segments()).containsExactly("a", "b");
    assertThat(request("PUT", "{\"expectedSegments\":[\"a\"]}").statusCode()).isEqualTo(500);
    assertThat(request("PUT", "{}").statusCode()).isEqualTo(400);
  }

  @Test
  void blocksIncompatibleMetadataBeforeHelixUpdate() throws Exception {
    server =
        MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata, null, operations);
    assertThat(request("POST", null).statusCode()).isEqualTo(201);
    var bad =
        new Schema(SCHEMA.name(), List.of(new Schema.Field("team", Schema.DataType.LONG, false)));
    metadata.createSegment(descriptor("bad", bad));
    assertThat(request("PUT", "{\"expectedSegments\":[\"a\"]}").statusCode()).isEqualTo(500);
    assertThat(operations.snapshot("events", TYPE).segments()).containsExactly("a");
  }

  @Test
  void operationsRequireOptInAndCorrectStore() throws Exception {
    server = MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata);
    assertThat(request("GET", null).statusCode()).isEqualTo(404);
    var other = new ZooKeeperMetadataStore(curator, "/other");
    assertThatThrownBy(
            () ->
                MetadataHttpServer.start(
                    URI.create("http://127.0.0.1:0/"), other, null, operations))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operations.snapshot("missing", TYPE))
        .isInstanceOf(java.io.IOException.class);
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new SegmentResourceOperations(
                    null, admin, manager.getHelixDataAccessor(), "cluster", 1, 1));
    assertThatThrownBy(
            () ->
                new SegmentResourceOperations(
                    metadata, admin, manager.getHelixDataAccessor(), "bad/name", 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new SegmentResourceOperations(
                    metadata, admin, manager.getHelixDataAccessor(), "cluster", 0, 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void controllerRuntimeCanEnableResourceOperationsWithoutOwningAdmin() throws Exception {
    var controller =
        HelixManagerFactory.getZKHelixManager(
            "cluster", "controller", InstanceType.CONTROLLER, zk.getConnectString());
    try (var runtime =
        ControllerRuntime.start(
            controller, URI.create("http://127.0.0.1:0/"), metadata, null, operations)) {
      var request =
          HttpRequest.newBuilder(
                  URI.create(
                      "http://127.0.0.1:" + runtime.port() + "/tables/OFFLINE/events/assignment"))
              .POST(HttpRequest.BodyPublishers.noBody())
              .build();
      assertThat(
              HttpClient.newHttpClient()
                  .send(request, HttpResponse.BodyHandlers.ofString())
                  .statusCode())
          .isEqualTo(201);
    }
    assertThat(controller.isConnected()).isFalse();
    assertThat(operations.snapshot("events", TYPE).segments()).containsExactly("a");
  }

  @Test
  void readsBoundedExternalViewWithoutClaimingReadiness() throws Exception {
    server =
        MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata, null, operations);
    String resource = HelixSegmentResource.resourceName("events", TYPE);
    var accessor = manager.getHelixDataAccessor();
    assertThatThrownBy(() -> operations.observed("events", TYPE, 3))
        .isInstanceOf(java.io.IOException.class);
    // Synthetic Helix external view, persisted through the real ZooKeeper-backed accessor.
    var view = new ExternalView(resource);
    view.setState(HelixSegmentResource.partitionName("b"), "offline-node", "OFFLINE");
    view.setState(HelixSegmentResource.partitionName("a"), "z-node", "ONLINE");
    view.setState(HelixSegmentResource.partitionName("a"), "a-node", "ONLINE");
    view.setState(HelixSegmentResource.partitionName("a"), "error-node", "ERROR");
    assertThat(accessor.setProperty(accessor.keyBuilder().externalView(resource), view)).isTrue();
    var result = operations.observed("events", TYPE, 3);
    assertThat(result.resourceName()).isEqualTo(resource);
    assertThat(result.segments())
        .containsExactly(
            new SegmentResourceOperations.ObservedSegment("a", List.of("a-node", "z-node")),
            new SegmentResourceOperations.ObservedSegment("b", List.of()));
    assertThatThrownBy(() -> result.segments().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> result.segments().get(0).onlineInstances().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> operations.observed("events", TYPE, 2))
        .isInstanceOf(java.io.IOException.class);
    assertThatThrownBy(() -> operations.observed("events", TYPE, 0))
        .isInstanceOf(IllegalArgumentException.class);
    String base =
        "http://127.0.0.1:" + server.port() + "/tables/OFFLINE/events/assignment/observed";
    var http = HttpClient.newHttpClient();
    var read =
        http.send(
            HttpRequest.newBuilder(URI.create(base + "?maxInstances=3")).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(read.statusCode()).isEqualTo(200);
    assertThat(read.body())
        .contains("a-node", "z-node", "onlineInstances")
        .doesNotContain("offline-node", "error-node");
    assertThat(
            http.send(
                    HttpRequest.newBuilder(URI.create(base + "?maxInstances=0")).GET().build(),
                    HttpResponse.BodyHandlers.ofString())
                .statusCode())
        .isEqualTo(400);
    assertThat(
            http.send(
                    HttpRequest.newBuilder(URI.create(base)).GET().build(),
                    HttpResponse.BodyHandlers.ofString())
                .statusCode())
        .isEqualTo(400);
    assertThat(
            http.send(
                    HttpRequest.newBuilder(URI.create(base + "?maxInstances=2")).GET().build(),
                    HttpResponse.BodyHandlers.ofString())
                .statusCode())
        .isEqualTo(500);
    view.setState("not-a-valid-partition", "node", "ONLINE");
    assertThat(accessor.setProperty(accessor.keyBuilder().externalView(resource), view)).isTrue();
    assertThatThrownBy(() -> operations.observed("events", TYPE, 3))
        .isInstanceOf(java.io.IOException.class);
  }
}
