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

import io.trellis.common.ZooKeeperMetadataStore;
import java.net.URI;
import java.util.Objects;
import org.apache.helix.HelixManager;

/** Coordinates lifecycle of the existing Helix controller and metadata HTTP server. */
public final class ControllerRuntime implements AutoCloseable {
  private final HelixController controller;
  private final MetadataHttpServer http;
  private boolean closed;

  private ControllerRuntime(HelixController controller, MetadataHttpServer http) {
    this.controller = controller;
    this.http = http;
  }

  /**
   * Connects a fresh controller manager and starts the metadata API. The manager must be configured
   * for an existing cluster; on successful connection this runtime owns it. The caller owns the
   * metadata store/client and configures both clients' security. If HTTP startup fails, the Helix
   * controller is closed and cleanup errors are suppressed on the startup failure.
   *
   * <p>The HTTP API is unauthenticated and is NOT restricted to the Helix leader. Only bind to a
   * trusted interface behind a secured proxy. Leadership observations are not fencing tokens. This
   * The basic overload does not expose uploads; neither overload provisions a cluster or starts a
   * reconciliation loop.
   */
  public static ControllerRuntime start(
      HelixManager manager, URI bindUri, ZooKeeperMetadataStore metadata) throws Exception {
    return startInternal(manager, bindUri, metadata, null, null);
  }

  /** Starts a controller runtime with the optional upload endpoint explicitly enabled. */
  public static ControllerRuntime start(
      HelixManager manager,
      URI bindUri,
      ZooKeeperMetadataStore metadata,
      SegmentArchiveUpload upload)
      throws Exception {
    Objects.requireNonNull(upload, "upload");
    Objects.requireNonNull(metadata, "metadata");
    if (!upload.usesMetadata(metadata))
      throw new IllegalArgumentException("upload must use the same metadata store");
    return startInternal(manager, bindUri, metadata, upload, null);
  }

  /** Starts a runtime with explicitly selected optional upload and resource operations. */
  public static ControllerRuntime start(
      HelixManager manager,
      URI bindUri,
      ZooKeeperMetadataStore metadata,
      SegmentArchiveUpload upload,
      SegmentResourceOperations operations)
      throws Exception {
    Objects.requireNonNull(metadata, "metadata");
    if (upload != null && !upload.usesMetadata(metadata))
      throw new IllegalArgumentException("upload must use the same metadata store");
    if (operations != null && !operations.usesMetadata(metadata))
      throw new IllegalArgumentException("operations must use the same metadata store");
    return startInternal(manager, bindUri, metadata, upload, operations);
  }

  private static ControllerRuntime startInternal(
      HelixManager manager,
      URI bindUri,
      ZooKeeperMetadataStore metadata,
      SegmentArchiveUpload upload,
      SegmentResourceOperations operations)
      throws Exception {
    Objects.requireNonNull(manager, "manager");
    Objects.requireNonNull(bindUri, "bindUri");
    Objects.requireNonNull(metadata, "metadata");
    HelixController controller = HelixController.start(manager);
    try {
      MetadataHttpServer http = MetadataHttpServer.start(bindUri, metadata, upload, operations);
      return new ControllerRuntime(controller, http);
    } catch (Exception | Error failure) {
      try {
        controller.close();
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  /** Actual HTTP port; unavailable after close. */
  public synchronized int port() {
    if (closed) throw new IllegalStateException("runtime is closed");
    return http.port();
  }

  /** A point-in-time connection observation, not a liveness guarantee. */
  public synchronized boolean isConnected() {
    return !closed && controller.isConnected();
  }

  /** A point-in-time leadership observation, not a fencing token. */
  public synchronized boolean isLeader() {
    return !closed && controller.isLeader();
  }

  /** Stops HTTP before disconnecting Helix; repeated closes are harmless. */
  @Override
  public synchronized void close() throws Exception {
    if (closed) return;
    closed = true;
    try (HelixController ignored = controller;
        MetadataHttpServer server = http) {
      // Try-with-resources closes HTTP first and still disconnects Helix on failure.
    }
  }
}
