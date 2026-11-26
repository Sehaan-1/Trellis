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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.apache.curator.test.TestingServer;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.junit.jupiter.api.Test;

class HelixControllerTest {
  private static HelixManager manager(String cluster, String id, InstanceType type, TestingServer server) {
    return HelixManagerFactory.getZKHelixManager(cluster, id, type, server.getConnectString());
  }

  @Test void twoControllersElectLeaderAndFailOver() throws Exception {
    try (var zk = new TestingServer()) {
      var admin = new ZKHelixAdmin(zk.getConnectString());
      try {
        assertThat(admin.addCluster("cluster")).isTrue();
        try (var first = HelixController.start(manager("cluster", "controller-a", InstanceType.CONTROLLER, zk));
             var second = HelixController.start(manager("cluster", "controller-b", InstanceType.CONTROLLER, zk))) {
          assertThat(first.isConnected()).isTrue();
          assertThat(second.isConnected()).isTrue();
          await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(first.isLeader() ^ second.isLeader()).isTrue());
          HelixController leader = first.isLeader() ? first : second;
          HelixController standby = leader == first ? second : first;
          leader.close(); leader.close();
          assertThat(leader.isConnected()).isFalse();
          assertThat(leader.isLeader()).isFalse();
          await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(standby.isLeader()).isTrue());
        }
      } finally { admin.close(); }
    }
  }

  @Test void rejectsWrongTypeAndAlreadyConnectedManager() throws Exception {
    try (var zk = new TestingServer()) {
      var admin = new ZKHelixAdmin(zk.getConnectString());
      try {
        assertThat(admin.addCluster("cluster")).isTrue();
        var participant = manager("cluster", "p", InstanceType.PARTICIPANT, zk);
        assertThatThrownBy(() -> HelixController.start(participant)).isInstanceOf(IllegalArgumentException.class);
        assertThat(participant.isConnected()).isFalse();
        var existing = manager("cluster", "existing", InstanceType.CONTROLLER, zk);
        existing.connect();
        try {
          assertThatThrownBy(() -> HelixController.start(existing)).isInstanceOf(IllegalStateException.class);
          assertThat(existing.isConnected()).isTrue();
        } finally { existing.disconnect(); }
        assertThatNullPointerException().isThrownBy(() -> HelixController.start(null));
      } finally { admin.close(); }
    }
  }
}
