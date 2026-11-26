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

import java.util.Objects;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;

/** Owns one Helix controller manager lifecycle; Helix performs election and failover. */
public final class HelixController implements AutoCloseable {
  private final HelixManager manager;
  private boolean closed;

  private HelixController(HelixManager manager) {
    this.manager = manager;
  }

  /**
   * Connects a fresh, unconnected CONTROLLER manager for an already provisioned cluster. The caller
   * configures ZooKeeper security and must not operate on the manager concurrently. After a
   * successful start this wrapper owns the manager; on connection failure it disconnects it.
   */
  public static HelixController start(HelixManager manager) throws Exception {
    Objects.requireNonNull(manager, "manager");
    if (manager.getInstanceType() != InstanceType.CONTROLLER)
      throw new IllegalArgumentException("manager must be a CONTROLLER");
    if (manager.isConnected()) throw new IllegalStateException("manager is already connected");
    try {
      manager.connect();
      if (!manager.isConnected()) throw new IllegalStateException("manager did not connect");
      return new HelixController(manager);
    } catch (Exception e) {
      try {
        manager.disconnect();
      } catch (RuntimeException close) {
        e.addSuppressed(close);
      }
      if (e instanceof InterruptedException) Thread.currentThread().interrupt();
      throw e;
    }
  }

  /** Connection state is an observation, not a guarantee that the session will remain live. */
  public synchronized boolean isConnected() {
    return !closed && manager.isConnected();
  }

  /**
   * Leadership is an observation, not a fencing token or authorization to perform a later write.
   */
  public synchronized boolean isLeader() {
    return !closed && manager.isConnected() && manager.isLeader();
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      manager.disconnect();
    }
  }
}
