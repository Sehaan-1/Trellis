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
import io.trellis.deepstore.local.FileBlobStore;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;

/** Owns a local-storage controller and its ZooKeeper clients. */
public final class ControllerService implements AutoCloseable {
  private final CuratorFramework curator;
  private final ZKHelixAdmin admin;
  private final ControllerRuntime runtime;
  private boolean closed;

  /** Explicit service settings; directories and the Helix cluster must already exist. */
  public record Config(
      String zooKeeper,
      String metadataRoot,
      String cluster,
      String instance,
      Path blobDirectory,
      int connectTimeoutSeconds,
      ControllerRuntime.IntegratedConfig runtime) {
    public Config {
      Objects.requireNonNull(zooKeeper, "zooKeeper");
      Objects.requireNonNull(metadataRoot, "metadataRoot");
      Objects.requireNonNull(cluster, "cluster");
      Objects.requireNonNull(instance, "instance");
      Objects.requireNonNull(blobDirectory, "blobDirectory");
      Objects.requireNonNull(runtime, "runtime");
      if (zooKeeper.isBlank()
          || cluster.isBlank()
          || instance.isBlank()
          || connectTimeoutSeconds <= 0)
        throw new IllegalArgumentException("invalid controller service settings");
    }
  }

  private ControllerService(
      CuratorFramework curator, ZKHelixAdmin admin, ControllerRuntime runtime) {
    this.curator = curator;
    this.admin = admin;
    this.runtime = runtime;
  }

  /**
   * Starts with local blobs. Uses ZooKeeper's process-level authentication configuration; no
   * credentials or ACLs are configured here. HTTP is unauthenticated and not leader-fenced. Caller
   * must secure all network paths and coordinate metadata/assignment writers externally. Startup
   * does not provision the cluster or perform reconciliation.
   */
  public static ControllerService start(Config config) throws Exception {
    Objects.requireNonNull(config, "config");
    var blobs = new FileBlobStore(config.blobDirectory());
    CuratorFramework curator =
        CuratorFrameworkFactory.newClient(config.zooKeeper(), new ExponentialBackoffRetry(100, 3));
    ZKHelixAdmin admin = null;
    try {
      curator.start();
      if (!curator.blockUntilConnected(config.connectTimeoutSeconds(), TimeUnit.SECONDS))
        throw new IllegalStateException("metadata ZooKeeper connection timed out");
      var metadata = new ZooKeeperMetadataStore(curator, config.metadataRoot());
      admin = new ZKHelixAdmin(config.zooKeeper());
      var manager =
          HelixManagerFactory.getZKHelixManager(
              config.cluster(), config.instance(), InstanceType.CONTROLLER, config.zooKeeper());
      var runtime =
          ControllerRuntime.startIntegrated(manager, admin, metadata, blobs, config.runtime());
      return new ControllerService(curator, admin, runtime);
    } catch (Exception | Error failure) {
      if (admin != null) {
        try {
          admin.close();
        } catch (RuntimeException | Error cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      try {
        curator.close();
      } catch (RuntimeException | Error cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  /** Actual bound HTTP port. */
  public synchronized int port() {
    if (closed) throw new IllegalStateException("service is closed");
    return runtime.port();
  }

  /** Stops HTTP and the controller before closing the admin and metadata clients. */
  @Override
  public synchronized void close() throws Exception {
    if (closed) return;
    closed = true;
    AutoCloseable adminClose = admin::close;
    try (curator;
        adminClose;
        runtime) {
      // Resources close in reverse order, even when an earlier close fails.
    }
  }
}
