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
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/** Unauthenticated HTTP metadata API with optional uploads; use a secured network boundary. */
public final class MetadataHttpServer implements AutoCloseable {
  private final Server server;
  private final AtomicBoolean closed = new AtomicBoolean();

  private MetadataHttpServer(Server server) {
    this.server = server;
  }

  /** Starts the HTTP service; the caller retains ownership of the metadata store and its client. */
  public static MetadataHttpServer start(URI bindUri, ZooKeeperMetadataStore store)
      throws Exception {
    return startInternal(bindUri, store, null);
  }

  /** Starts the metadata server with an explicitly configured upload endpoint. */
  public static MetadataHttpServer start(
      URI bindUri, ZooKeeperMetadataStore store, SegmentArchiveUpload upload) throws Exception {
    Objects.requireNonNull(upload, "upload");
    Objects.requireNonNull(store, "store");
    if (!upload.usesMetadata(store))
      throw new IllegalArgumentException("upload must use the same metadata store");
    return startInternal(bindUri, store, upload);
  }

  private static MetadataHttpServer startInternal(
      URI bindUri, ZooKeeperMetadataStore store, SegmentArchiveUpload upload) throws Exception {
    Objects.requireNonNull(bindUri, "bindUri");
    Objects.requireNonNull(store, "store");
    if (!"http".equalsIgnoreCase(bindUri.getScheme())
        || bindUri.getHost() == null
        || bindUri.getPort() < 0
        || !"/".equals(bindUri.getPath())
        || bindUri.getQuery() != null
        || bindUri.getFragment() != null
        || bindUri.getUserInfo() != null)
      throw new IllegalArgumentException(
          "bind URI must be http://host:port/ without credentials, query or fragment");
    ResourceConfig config =
        new ResourceConfig()
            .register(MetadataResource.class)
            .register(JacksonFeature.class)
            .register(
                new AbstractBinder() {
                  @Override
                  protected void configure() {
                    bind(store).to(ZooKeeperMetadataStore.class);
                  }
                });
    if (upload != null) {
      config
          .register(SegmentUploadResource.class)
          .register(
              new AbstractBinder() {
                @Override
                protected void configure() {
                  bind(upload).to(SegmentArchiveUpload.class);
                }
              });
    }
    Server server = JettyHttpContainerFactory.createServer(bindUri, config, false);
    // Jersey configures the port but not the connector host; honor the requested bind address.
    ((ServerConnector) server.getConnectors()[0]).setHost(bindUri.getHost());
    try {
      server.start();
      return new MetadataHttpServer(server);
    } catch (Exception e) {
      try {
        server.stop();
      } catch (Exception close) {
        e.addSuppressed(close);
      }
      server.destroy();
      throw e;
    }
  }

  /** Actual bound port, including when port zero was requested. */
  public int port() {
    return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      try {
        server.stop();
      } finally {
        server.destroy();
      }
    }
  }
}
