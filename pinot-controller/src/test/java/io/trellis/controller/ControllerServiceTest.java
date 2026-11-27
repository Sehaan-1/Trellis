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

import io.trellis.segment.local.SegmentArchives;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ControllerServiceTest {
  @TempDir Path root;

  @Test
  void startsOwnedClientsAndStopsHttp() throws Exception {
    try (var zk = new TestingServer()) {
      var admin = new ZKHelixAdmin(zk.getConnectString());
      try {
        assertThat(admin.addCluster("serviceCluster")).isTrue();
        var stage = Files.createDirectory(root.resolve("stage"));
        var blobs = Files.createDirectory(root.resolve("blobs"));
        var runtime =
            new ControllerRuntime.IntegratedConfig(
                URI.create("http://127.0.0.1:0/"),
                stage,
                new SegmentArchives.Limits(10000, 10, 10000, 10000),
                10,
                1,
                10,
                2);
        var config =
            new ControllerService.Config(
                zk.getConnectString(),
                "/metadata",
                "serviceCluster",
                "controller",
                blobs,
                10,
                runtime);
        var service = ControllerService.start(config);
        try {
          var uri = URI.create("http://127.0.0.1:" + service.port() + "/controller/status");
          var response =
              HttpClient.newHttpClient()
                  .send(
                      HttpRequest.newBuilder(uri).GET().build(),
                      HttpResponse.BodyHandlers.ofString());
          assertThat(response.statusCode()).isEqualTo(200);
          assertThat(response.body()).contains("\"connected\":true");
          var schema =
              HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + service.port() + "/schemas"))
                  .header("Content-Type", "application/json")
                  .POST(
                      HttpRequest.BodyPublishers.ofString(
                          "{\"name\":\"games\",\"fields\":[{\"name\":\"team\",\"type\":\"STRING\",\"multiValue\":false}]}"))
                  .build();
          assertThat(
                  HttpClient.newHttpClient()
                      .send(schema, HttpResponse.BodyHandlers.ofString())
                      .statusCode())
              .isEqualTo(201);

        } finally {
          service.close();
        }
        service.close();
        assertThatThrownBy(service::port).isInstanceOf(IllegalStateException.class);
      } finally {
        admin.close();
      }
    }
  }

  @Test
  void rejectsBadSettingsAndBindFailure() throws Exception {
    try (var zk = new TestingServer()) {
      var admin = new ZKHelixAdmin(zk.getConnectString());
      try {
        admin.addCluster("cluster");
        var stage = Files.createDirectory(root.resolve("stage"));
        var blobs = Files.createDirectory(root.resolve("blobs"));
        try (var socket = new ServerSocket(0)) {
          var runtime =
              new ControllerRuntime.IntegratedConfig(
                  URI.create("http://127.0.0.1:" + socket.getLocalPort() + "/"),
                  stage,
                  new SegmentArchives.Limits(10000, 10, 10000, 10000),
                  10,
                  1,
                  10);
          assertThatThrownBy(
                  () ->
                      new ControllerService.Config(
                          zk.getConnectString(),
                          "/metadata",
                          "cluster",
                          "controller",
                          blobs,
                          0,
                          runtime))
              .isInstanceOf(IllegalArgumentException.class);
          var config =
              new ControllerService.Config(
                  zk.getConnectString(), "/metadata", "cluster", "controller", blobs, 10, runtime);
          assertThatThrownBy(() -> ControllerService.start(config)).isInstanceOf(Exception.class);
        }
      } finally {
        admin.close();
      }
    }
  }

  @Test
  void mainConfigRequiresExplicitSecurityAcknowledgementAndLimits() {
    var p = new Properties();
    assertThatThrownBy(() -> ControllerMain.config(p)).isInstanceOf(IllegalArgumentException.class);
    p.setProperty("trellis.maxArchiveBytes", "10000");
    p.setProperty("trellis.maxFiles", "10");
    p.setProperty("trellis.maxFileBytes", "10000");
    p.setProperty("trellis.maxTotalBytes", "10000");
    p.setProperty("trellis.bindUri", "http://127.0.0.1:0/");
    p.setProperty("trellis.stagingDirectory", root.toString());
    p.setProperty("trellis.maxDocuments", "10");
    p.setProperty("trellis.replicas", "1");
    p.setProperty("trellis.maxSegments", "10");
    p.setProperty("trellis.maxConcurrentUploads", "2");
    p.setProperty("trellis.zookeeper", "127.0.0.1:2181");
    p.setProperty("trellis.metadataRoot", "/metadata");
    p.setProperty("trellis.cluster", "cluster");
    p.setProperty("trellis.instance", "controller");
    p.setProperty("trellis.blobDirectory", root.toString());
    p.setProperty("trellis.connectTimeoutSeconds", "10");
    p.setProperty("trellis.externalSecurityAcknowledged", "false");
    assertThatThrownBy(() -> ControllerMain.config(p)).isInstanceOf(IllegalArgumentException.class);
    p.setProperty("trellis.externalSecurityAcknowledged", "true");
    assertThat(ControllerMain.config(p).runtime().maxConcurrentUploads()).isEqualTo(2);
    p.setProperty("trellis.maxConcurrentUploads", "0");
    assertThatThrownBy(() -> ControllerMain.config(p)).isInstanceOf(IllegalArgumentException.class);
  }
}
