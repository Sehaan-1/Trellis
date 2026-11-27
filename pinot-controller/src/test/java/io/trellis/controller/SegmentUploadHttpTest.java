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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentUploadHttpTest {
  @TempDir Path root;
  private TestingServer zk;
  private CuratorFramework client;
  private ZooKeeperMetadataStore metadata;
  private FileBlobStore blobs;
  private MetadataHttpServer server;
  private SegmentArchiveUpload upload;
  private Path stage;
  private byte[] archive;
  private static final Schema SCHEMA =
      new Schema("games", List.of(new Schema.Field("team", Schema.DataType.STRING, false)));
  private static final SegmentArchives.Limits LIMITS =
      new SegmentArchives.Limits(1000000, 100, 1000000, 1000000);
  private static final TableConfig.TableType TYPE = TableConfig.TableType.OFFLINE;

  @BeforeEach
  void start() throws Exception {
    zk = new TestingServer();
    client =
        CuratorFrameworkFactory.newClient(
            zk.getConnectString(), new ExponentialBackoffRetry(50, 3));
    client.start();
    if (!client.blockUntilConnected(10, TimeUnit.SECONDS))
      throw new IllegalStateException("ZooKeeper did not start");
    metadata = new ZooKeeperMetadataStore(client, "/metadata");
    metadata.createSchema(SCHEMA);
    metadata.createTable(new TableConfig("events", SCHEMA.name(), TYPE));
    blobs = new FileBlobStore(Files.createDirectory(root.resolve("blobs")));
    stage = Files.createDirectory(root.resolve("stage"));
    upload = new SegmentArchiveUpload(metadata, blobs, stage, LIMITS, 1);
    Path source = root.resolve("source");
    LocalSegments.write(source, SCHEMA, List.of(new GenericRow().put("team", "red")));
    var output = new java.io.ByteArrayOutputStream();
    SegmentArchiveWriter.write(source, output, LIMITS, SCHEMA, 1);
    archive = output.toByteArray();
  }

  @AfterEach
  void stop() throws Exception {
    if (server != null) server.close();
    if (client != null) client.close();
    if (zk != null) zk.close();
  }

  private HttpResponse<String> post(String table, String segment, byte[] payload) throws Exception {
    var uri =
        URI.create(
            "http://127.0.0.1:"
                + server.port()
                + "/tables/OFFLINE/"
                + table
                + "/segments/"
                + segment
                + "/content");
    var request =
        HttpRequest.newBuilder(uri)
            .header("Content-Type", "application/octet-stream")
            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
            .build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
  }

  private void emptyStage() throws Exception {
    try (var files = Files.list(stage)) {
      assertThat(files.toList()).isEmpty();
    }
  }

  @Test
  void uploadsVerifiedArchiveAndRestoresIt() throws Exception {
    server = MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata, upload);
    var response = post("events", "part", archive);
    assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
    assertThat(response.headers().firstValue("ETag")).contains("\"0\"");
    assertThat(response.headers().firstValue("Location"))
        .hasValueSatisfying(
            location -> assertThat(location).endsWith("/tables/OFFLINE/events/segments/part"));
    var descriptor = metadata.getSegment("events", TYPE, "part").value();
    assertThat(descriptor.documents()).isEqualTo(1);
    assertThat(descriptor.blob().size()).isEqualTo(archive.length);
    assertThat(
            SegmentBlobs.prepare(blobs, descriptor, stage, root.resolve("target"), LIMITS, 1)
                .segment()
                .documents())
        .isEqualTo(1);
    assertThat(post("events", "part", archive).statusCode()).isEqualTo(409);
    emptyStage();
  }

  @Test
  void rejectsOversizedInvalidAndUnknownTableAndCleansStaging() throws Exception {
    server = MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata, upload);
    assertThat(post("events", "large", new byte[1000001]).statusCode()).isEqualTo(413);
    assertThat(post("missing", "part", archive).statusCode()).isEqualTo(404);
    assertThat(post("events", "bad", new byte[] {1, 2, 3}).statusCode()).isEqualTo(500);
    assertThatThrownBy(() -> metadata.getSegment("events", TYPE, "bad"))
        .isInstanceOf(org.apache.zookeeper.KeeperException.NoNodeException.class);
    emptyStage();
  }

  @Test
  void uploadRequiresOptInAndMatchingMetadataStore() throws Exception {
    server = MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata);
    assertThat(post("events", "part", archive).statusCode()).isEqualTo(404);
    var other = new ZooKeeperMetadataStore(client, "/other");
    assertThatThrownBy(
            () -> MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), other, upload))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException()
        .isThrownBy(
            () -> MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata, null));
  }

  @Test
  void inputIsCallerOwnedAndArgumentValidationIsStrict() throws Exception {
    class Source extends java.io.ByteArrayInputStream {
      boolean closed;

      Source(byte[] b) {
        super(b);
      }

      @Override
      public void close() {
        closed = true;
      }
    }
    var source = new Source(archive);
    assertThat(upload.upload("events", TYPE, "direct", source).descriptor().documents())
        .isEqualTo(1);
    assertThat(source.closed).isFalse();
    assertThatNullPointerException().isThrownBy(() -> upload.upload("events", TYPE, "x", null));
    assertThatNullPointerException()
        .isThrownBy(() -> new SegmentArchiveUpload(null, blobs, stage, LIMITS, 1));
    assertThatNullPointerException()
        .isThrownBy(() -> new SegmentArchiveUpload(metadata, null, stage, LIMITS, 1));
    assertThatThrownBy(() -> new SegmentArchiveUpload(metadata, blobs, stage, LIMITS, -1))
        .isInstanceOf(IllegalArgumentException.class);
    Path link = root.resolve("link");
    Files.createSymbolicLink(link, stage);
    assertThatThrownBy(() -> new SegmentArchiveUpload(metadata, blobs, link, LIMITS, 1))
        .isInstanceOf(java.io.IOException.class);
    emptyStage();
  }

  @Test
  void rejectsZeroProgressInputAndReleasesPermitWithoutClosingInput() throws Exception {
    class ZeroProgress extends java.io.InputStream {
      boolean closed;
      int reads;

      @Override
      public int read() {
        throw new AssertionError("unexpected single-byte read");
      }

      @Override
      public int read(byte[] bytes, int offset, int length) {
        assertThat(length).isPositive();
        reads++;
        if (reads == 1) {
          bytes[offset] = 1;
          return 1;
        }
        return 0;
      }

      @Override
      public void close() {
        closed = true;
      }
    }
    var source = new ZeroProgress();
    assertThatThrownBy(() -> upload.upload("events", TYPE, "bad", source))
        .isInstanceOf(java.io.IOException.class)
        .hasMessage("input stream made no progress");
    assertThat(source.reads).isEqualTo(2);
    assertThat(source.closed).isFalse();
    assertThatThrownBy(() -> metadata.getSegment("events", TYPE, "bad"))
        .isInstanceOf(org.apache.zookeeper.KeeperException.NoNodeException.class);
    emptyStage();
    assertThat(
            upload
                .upload("events", TYPE, "good", new java.io.ByteArrayInputStream(archive))
                .descriptor()
                .documents())
        .isEqualTo(1);
    emptyStage();
  }

  @Test
  void rejectsConcurrentUploadsAndReleasesPermitAfterCompletionAndFailure() throws Exception {
    server = MetadataHttpServer.start(URI.create("http://127.0.0.1:0/"), metadata, upload);
    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    var slow =
        new java.io.FilterInputStream(new java.io.ByteArrayInputStream(archive)) {
          boolean first = true;

          @Override
          public int read(byte[] bytes, int offset, int length) throws java.io.IOException {
            if (first) {
              first = false;
              entered.countDown();
              try {
                if (!release.await(10, TimeUnit.SECONDS))
                  throw new java.io.IOException("timed out");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(e);
              }
            }
            return super.read(bytes, offset, length);
          }
        };
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      var first = executor.submit(() -> upload.upload("events", TYPE, "first", slow));
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(
              () ->
                  upload.upload(
                      "events", TYPE, "second", new java.io.ByteArrayInputStream(archive)))
          .isInstanceOf(SegmentArchiveUpload.TooManyUploadsException.class);
      var rejected = post("events", "second", archive);
      assertThat(rejected.statusCode()).isEqualTo(429);
      assertThat(rejected.headers().firstValue("Cache-Control")).contains("no-store");
      release.countDown();
      assertThat(first.get(20, TimeUnit.SECONDS).descriptor().segmentName()).isEqualTo("first");
      assertThat(post("events", "bad", new byte[] {1, 2, 3}).statusCode()).isEqualTo(500);
      assertThat(post("events", "second", archive).statusCode()).isEqualTo(201);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
    emptyStage();
  }

  @Test
  void concurrentLimitCanBeConfiguredAndMustBePositive() throws Exception {
    assertThatThrownBy(() -> new SegmentArchiveUpload(metadata, blobs, stage, LIMITS, 1, 0))
        .isInstanceOf(IllegalArgumentException.class);
    var two = new SegmentArchiveUpload(metadata, blobs, stage, LIMITS, 1, 2);
    var entered = new java.util.concurrent.CountDownLatch(2);
    var release = new java.util.concurrent.CountDownLatch(1);
    java.util.function.Supplier<java.io.InputStream> source =
        () ->
            new java.io.FilterInputStream(new java.io.ByteArrayInputStream(archive)) {
              boolean first = true;

              @Override
              public int read(byte[] bytes, int offset, int length) throws java.io.IOException {
                if (first) {
                  first = false;
                  entered.countDown();
                  try {
                    if (!release.await(10, TimeUnit.SECONDS))
                      throw new java.io.IOException("timed out");
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.IOException(e);
                  }
                }
                return super.read(bytes, offset, length);
              }
            };
    var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      var first = executor.submit(() -> two.upload("events", TYPE, "one", source.get()));
      var second = executor.submit(() -> two.upload("events", TYPE, "two", source.get()));
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(
              () -> two.upload("events", TYPE, "three", new java.io.ByteArrayInputStream(archive)))
          .isInstanceOf(SegmentArchiveUpload.TooManyUploadsException.class);
      release.countDown();
      assertThat(first.get(20, TimeUnit.SECONDS).version()).isZero();
      assertThat(second.get(20, TimeUnit.SECONDS).version()).isZero();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
    emptyStage();
  }
}
