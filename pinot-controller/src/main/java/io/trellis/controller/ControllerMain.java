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

import io.trellis.segment.local.SegmentArchives;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** Process entry point; configuration is supplied via JVM system properties. */
public final class ControllerMain {
  private ControllerMain() {}

  static ControllerService.Config config(Properties properties) {
    var limits =
        new SegmentArchives.Limits(
            Long.parseLong(required(properties, "trellis.maxArchiveBytes")),
            Integer.parseInt(required(properties, "trellis.maxFiles")),
            Long.parseLong(required(properties, "trellis.maxFileBytes")),
            Long.parseLong(required(properties, "trellis.maxTotalBytes")));
    var runtime =
        new ControllerRuntime.IntegratedConfig(
            URI.create(required(properties, "trellis.bindUri")),
            Path.of(required(properties, "trellis.stagingDirectory")),
            limits,
            Integer.parseInt(required(properties, "trellis.maxDocuments")),
            Integer.parseInt(required(properties, "trellis.replicas")),
            Integer.parseInt(required(properties, "trellis.maxSegments")),
            Integer.parseInt(required(properties, "trellis.maxConcurrentUploads")));
    if (!"true".equals(required(properties, "trellis.externalSecurityAcknowledged")))
      throw new IllegalArgumentException("external security must be acknowledged");
    return new ControllerService.Config(
        required(properties, "trellis.zookeeper"),
        required(properties, "trellis.metadataRoot"),
        required(properties, "trellis.cluster"),
        required(properties, "trellis.instance"),
        Path.of(required(properties, "trellis.blobDirectory")),
        Integer.parseInt(required(properties, "trellis.connectTimeoutSeconds")),
        runtime);
  }

  private static String required(Properties properties, String name) {
    String value = properties.getProperty(name);
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("missing property: " + name);
    return value;
  }

  /** Starts the service and waits until process shutdown. */
  public static void main(String[] args) throws Exception {
    if (args.length != 0)
      throw new IllegalArgumentException("use JVM system properties, not arguments");
    var service = ControllerService.start(config(System.getProperties()));
    var stopped = new CountDownLatch(1);
    Thread hook =
        new Thread(
            () -> {
              try {
                service.close();
              } catch (Exception e) {
                e.printStackTrace(System.err);
              } finally {
                stopped.countDown();
              }
            },
            "controller-shutdown");
    try {
      Runtime.getRuntime().addShutdownHook(hook);
    } catch (RuntimeException | Error failure) {
      try {
        service.close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
    try {
      System.out.println("Controller HTTP port: " + service.port());
      stopped.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      try {
        Runtime.getRuntime().removeShutdownHook(hook);
      } catch (IllegalStateException ignored) {
      }
      service.close();
    }
  }
}
