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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetadataHttpServerTest {
  private TestingServer zk;
  private CuratorFramework curator;
  private MetadataHttpServer server;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private String base;
  private static final String SCHEMA =
      "{\"name\":\"events\",\"fields\":[{\"name\":\"x\",\"type\":\"STRING\",\"multiValue\":false}]}";
  private static final String SCHEMA2 =
      "{\"name\":\"events\",\"fields\":[{\"name\":\"x\",\"type\":\"LONG\",\"multiValue\":false}]}";
  private static final String TABLE =
      "{\"tableName\":\"events\",\"schemaName\":\"events\",\"type\":\"OFFLINE\"}";

  @BeforeEach
  void start() throws Exception {
    zk = new TestingServer();
    curator =
        CuratorFrameworkFactory.newClient(
            zk.getConnectString(), new ExponentialBackoffRetry(50, 3));
    curator.start();
    if (!curator.blockUntilConnected(10, TimeUnit.SECONDS))
      throw new IllegalStateException("ZooKeeper did not start");
    server =
        MetadataHttpServer.start(
            URI.create("http://127.0.0.1:0/"), new ZooKeeperMetadataStore(curator, "/controller"));
    base = "http://127.0.0.1:" + server.port();
  }

  @AfterEach
  void stop() throws Exception {
    if (server != null) server.close();
    if (curator != null) curator.close();
    if (zk != null) zk.close();
  }

  private HttpRequest request(String method, String path, String body, String etag) {
    var builder =
        HttpRequest.newBuilder(URI.create(base + path))
            .timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json");
    if (etag != null) builder.header("If-Match", etag);
    if (body != null) builder.header("Content-Type", "application/json");
    return builder
        .method(
            method,
            body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body))
        .build();
  }

  private HttpResponse<String> call(String method, String path, String body, String etag)
      throws Exception {
    return http.send(request(method, path, body, etag), HttpResponse.BodyHandlers.ofString());
  }

  private String etag(HttpResponse<?> r) {
    return r.headers().firstValue("ETag").orElseThrow();
  }

  @Test
  void schemasSupportOptimisticCrudOverRealHttp() throws Exception {
    var created = call("POST", "/schemas", SCHEMA, null);
    assertThat(created.statusCode()).as("Response: %s", created.body()).isEqualTo(201);
    assertThat(etag(created)).isEqualTo("\"0\"");
    assertThat(created.headers().firstValue("Location"))
        .hasValueSatisfying(location -> assertThat(location).endsWith("/schemas/events"));
    assertThat(call("POST", "/schemas", SCHEMA, null).statusCode()).isEqualTo(409);
    var read = call("GET", "/schemas/events", null, null);
    assertThat(read.statusCode()).isEqualTo(200);
    assertThat(read.body()).contains("events", "STRING", "multiValue");
    assertThat(etag(read)).isEqualTo("\"0\"");
    assertThat(call("PUT", "/schemas/events", SCHEMA2, null).statusCode()).isEqualTo(428);
    assertThat(call("PUT", "/schemas/events", SCHEMA2, "*").statusCode()).isEqualTo(400);
    var updated = call("PUT", "/schemas/events", SCHEMA2, "\"0\"");
    assertThat(updated.statusCode()).isEqualTo(204);
    assertThat(etag(updated)).isEqualTo("\"1\"");
    assertThat(call("PUT", "/schemas/events", SCHEMA, "\"0\"").statusCode()).isEqualTo(412);
    assertThat(call("DELETE", "/schemas/events", null, "\"0\"").statusCode()).isEqualTo(412);
    assertThat(call("DELETE", "/schemas/events", null, "\"1\"").statusCode()).isEqualTo(204);
    assertThat(call("GET", "/schemas/events", null, null).statusCode()).isEqualTo(404);
  }

  @Test
  void tableTypeIsolationAndBadRequests() throws Exception {
    assertThat(call("POST", "/tables", TABLE, null).statusCode()).isEqualTo(201);
    String realtime = TABLE.replace("OFFLINE", "REALTIME");
    assertThat(call("POST", "/tables", realtime, null).statusCode()).isEqualTo(201);
    var read = call("GET", "/tables/OFFLINE/events", null, null);
    assertThat(read.statusCode()).isEqualTo(200);
    assertThat(read.body()).contains("OFFLINE", "events");
    assertThat(call("PUT", "/tables/OFFLINE/events", realtime, "\"0\"").statusCode())
        .isEqualTo(400);
    assertThat(
            call("PUT", "/tables/OFFLINE/events", TABLE.replace("\"events\"", "\"wrong\""), "\"0\"")
                .statusCode())
        .isEqualTo(400);
    assertThat(call("DELETE", "/tables/OFFLINE/events", null, "\"0\"").statusCode()).isEqualTo(204);
    assertThat(call("GET", "/tables/REALTIME/events", null, null).statusCode()).isEqualTo(200);
    assertThat(call("GET", "/tables/NOPE/events", null, null).statusCode()).isEqualTo(400);
    assertThat(
            call("POST", "/schemas", "{\"name\":\"invalid/path\",\"fields\":[]}", null)
                .statusCode())
        .isEqualTo(400);
    assertThat(
            call(
                    "POST",
                    "/schemas",
                    "{\"name\":\"x\",\"fields\":[{\"name\":\"n\",\"type\":\"LONG\"}]}",
                    null)
                .statusCode())
        .isEqualTo(400);
  }

  @Test
  void concurrentWritesHaveOneWinner() throws Exception {
    assertThat(call("POST", "/schemas", SCHEMA, null).statusCode()).isEqualTo(201);
    var a =
        http.sendAsync(
            request("PUT", "/schemas/events", SCHEMA, "\"0\""),
            HttpResponse.BodyHandlers.ofString());
    var b =
        http.sendAsync(
            request("PUT", "/schemas/events", SCHEMA2, "\"0\""),
            HttpResponse.BodyHandlers.ofString());
    assertThat(
            List.of(
                a.get(10, TimeUnit.SECONDS).statusCode(), b.get(10, TimeUnit.SECONDS).statusCode()))
        .containsExactlyInAnyOrder(204, 412);
  }

  @Test
  void closingHttpServerDoesNotCloseCuratorClient() throws Exception {
    server.close();
    server.close();
    var store = new ZooKeeperMetadataStore(curator, "/after-close");
    var schema =
        new io.trellis.spi.Schema(
            "s", List.of(new io.trellis.spi.Schema.Field("x", io.trellis.spi.Schema.DataType.LONG, false)));
    assertThat(store.createSchema(schema)).isZero();
  }

  @Test
  void rejectsUnsafeBindUri() {
    var store = new ZooKeeperMetadataStore(curator, "/other");
    assertThatThrownBy(() -> MetadataHttpServer.start(URI.create("https://127.0.0.1:443/"), store))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> MetadataHttpServer.start(URI.create("http://127.0.0.1:1/path"), store))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
