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
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ControllerRuntimeTest {
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
}
