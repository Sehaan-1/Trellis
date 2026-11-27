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

import io.trellis.spi.TableConfig;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.junit.jupiter.api.Test;

class HelixSegmentResourceTest {
  private static final TableConfig.TableType OFFLINE = TableConfig.TableType.OFFLINE;

  @Test
  void nameMappingIsSafeDistinctAndReversible() {
    String segment = "segment / å and spaces";
    String partition = HelixSegmentResource.partitionName(segment);
    assertThat(partition).matches("[A-Za-z0-9_-]+");
    assertThat(HelixSegmentResource.segmentName(partition)).isEqualTo(segment);
    assertThat(HelixSegmentResource.resourceName("table / å", OFFLINE))
        .isNotEqualTo(
            HelixSegmentResource.resourceName("table / å", TableConfig.TableType.REALTIME));
    for (String invalid : List.of("x", "s_-", "s_YQ==", "s__w", "s_IA"))
      assertThatThrownBy(() -> HelixSegmentResource.segmentName(invalid))
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HelixSegmentResource.partitionName(" "))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HelixSegmentResource.partitionName("\uD800"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HelixSegmentResource.partitionName("a".repeat(129)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(
            HelixSegmentResource.segmentName(HelixSegmentResource.partitionName("a".repeat(128))))
        .hasSize(128);
    assertThatNullPointerException().isThrownBy(() -> HelixSegmentResource.partitionName(null));
    assertThatNullPointerException().isThrownBy(() -> HelixSegmentResource.segmentName(null));
    assertThatNullPointerException()
        .isThrownBy(() -> HelixSegmentResource.resourceName("table", null));
  }

  @Test
  void buildsFullAutoIdealStateWithExactPartitions() {
    IdealState state =
        HelixSegmentResource.idealState("events", OFFLINE, List.of("a", "b / c"), 2, 2);
    assertThat(state.getResourceName())
        .isEqualTo(HelixSegmentResource.resourceName("events", OFFLINE));
    assertThat(state.getRebalanceMode()).isEqualTo(IdealState.RebalanceMode.FULL_AUTO);
    assertThat(state.getStateModelDefRef()).isEqualTo("OnlineOffline");
    assertThat(state.getReplicas()).isEqualTo("2");
    assertThat(state.getNumPartitions()).isEqualTo(2);
    assertThat(state.getPartitionSet())
        .containsExactlyInAnyOrder(
            HelixSegmentResource.partitionName("a"), HelixSegmentResource.partitionName("b / c"));
    assertThatThrownBy(() -> HelixSegmentResource.idealState("t", OFFLINE, List.of(), 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HelixSegmentResource.idealState("t", OFFLINE, List.of("x", "x"), 1, 2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HelixSegmentResource.idealState("t", OFFLINE, List.of("x", "y"), 1, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HelixSegmentResource.idealState("t", OFFLINE, List.of("x"), 0, 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> HelixSegmentResource.idealState("t", OFFLINE, List.of("x"), 1, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException()
        .isThrownBy(() -> HelixSegmentResource.idealState("t", OFFLINE, null, 1, 1));
  }

  @Test
  void createsResourceThroughRealHelixAdmin() throws Exception {
    try (var zk = new TestingServer()) {
      var admin = new ZKHelixAdmin(zk.getConnectString());
      try {
        assertThat(admin.addCluster("cluster")).isTrue();
        admin.addStateModelDef(
            "cluster",
            "OnlineOffline",
            BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
        HelixSegmentResource.create(
            admin, "cluster", "events", OFFLINE, List.of("first", "second"), 2, 10);
        String resource = HelixSegmentResource.resourceName("events", OFFLINE);
        IdealState state = admin.getResourceIdealState("cluster", resource);
        assertThat(state.getRebalanceMode()).isEqualTo(IdealState.RebalanceMode.FULL_AUTO);
        assertThat(state.getReplicas()).isEqualTo("2");
        assertThat(state.getPartitionSet())
            .containsExactlyInAnyOrder(
                HelixSegmentResource.partitionName("first"),
                HelixSegmentResource.partitionName("second"));
        assertThatThrownBy(
                () ->
                    HelixSegmentResource.create(
                        admin, "cluster", "events", OFFLINE, List.of("replacement"), 1, 10))
            .isInstanceOf(RuntimeException.class);
        assertThat(admin.getResourceIdealState("cluster", resource).getPartitionSet())
            .isEqualTo(state.getPartitionSet());
        assertThatThrownBy(
                () ->
                    HelixSegmentResource.create(
                        admin, "bad/cluster", "events", OFFLINE, List.of("x"), 1, 1))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatNullPointerException()
            .isThrownBy(
                () ->
                    HelixSegmentResource.create(
                        null, "cluster", "events", OFFLINE, List.of("x"), 1, 1));
      } finally {
        admin.close();
      }
    }
  }

  @Test
  void replacesExactExpectedSetPreservingRetainedState() throws Exception {
    try (var zk = new TestingServer()) {
      var admin = new ZKHelixAdmin(zk.getConnectString());
      var manager =
          HelixManagerFactory.getZKHelixManager(
              "cluster", "observer", InstanceType.SPECTATOR, zk.getConnectString());
      try {
        assertThat(admin.addCluster("cluster")).isTrue();
        admin.addStateModelDef(
            "cluster",
            "OnlineOffline",
            BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
        HelixSegmentResource.create(admin, "cluster", "events", OFFLINE, List.of("a", "b"), 2, 10);
        String resource = HelixSegmentResource.resourceName("events", OFFLINE);
        IdealState original = admin.getResourceIdealState("cluster", resource);
        String a = HelixSegmentResource.partitionName("a"),
            b = HelixSegmentResource.partitionName("b");
        original.setPreferenceList(a, List.of("node"));
        original.setPartitionState(b, "node", "ONLINE");
        original.getRecord().setSimpleField("custom", "keep");
        admin.setResourceIdealState("cluster", resource, original);
        manager.connect();
        var accessor = manager.getHelixDataAccessor();
        HelixSegmentResource.replaceSegments(
            accessor, "events", OFFLINE, List.of("a", "b"), List.of("a", "c"), 10);
        IdealState result = admin.getResourceIdealState("cluster", resource);
        assertThat(result.getPartitionSet())
            .containsExactlyInAnyOrder(a, HelixSegmentResource.partitionName("c"));
        assertThat(result.getNumPartitions()).isEqualTo(2);
        assertThat(result.getPreferenceList(a)).containsExactly("node");
        assertThat(result.getRecord().getMapField(b)).isNull();
        assertThat(result.getReplicas()).isEqualTo("2");
        assertThat(result.getRecord().getSimpleField("custom")).isEqualTo("keep");
        assertThatThrownBy(
                () ->
                    HelixSegmentResource.replaceSegments(
                        accessor, "events", OFFLINE, List.of("a", "b"), List.of("a"), 10))
            .isInstanceOf(IOException.class);
        assertThatThrownBy(
                () ->
                    HelixSegmentResource.replaceSegments(
                        accessor, "missing", OFFLINE, List.of("a"), List.of("b"), 10))
            .isInstanceOf(IOException.class);
        assertThat(
                admin.getResourceIdealState(
                    "cluster", HelixSegmentResource.resourceName("missing", OFFLINE)))
            .isNull();
        assertThatThrownBy(
                () ->
                    HelixSegmentResource.replaceSegments(
                        accessor, "events", OFFLINE, List.of("a", "c"), List.of(), 10))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatNullPointerException()
            .isThrownBy(
                () ->
                    HelixSegmentResource.replaceSegments(
                        null, "events", OFFLINE, List.of("a"), List.of("b"), 10));
      } finally {
        manager.disconnect();
        admin.close();
      }
    }
  }

  @Test
  void competingUpdatesDoNotBothOverwriteSameExpectedSet() throws Exception {
    try (var zk = new TestingServer()) {
      var admin = new ZKHelixAdmin(zk.getConnectString());
      var manager =
          HelixManagerFactory.getZKHelixManager(
              "cluster", "observer", InstanceType.SPECTATOR, zk.getConnectString());
      try {
        assertThat(admin.addCluster("cluster")).isTrue();
        admin.addStateModelDef(
            "cluster",
            "OnlineOffline",
            BuiltInStateModelDefinitions.OnlineOffline.getStateModelDefinition());
        HelixSegmentResource.create(admin, "cluster", "events", OFFLINE, List.of("a"), 1, 10);
        manager.connect();
        var accessor = manager.getHelixDataAccessor();
        var executor = Executors.newFixedThreadPool(2);
        var latch = new CountDownLatch(1);
        try {
          java.util.concurrent.Callable<Boolean> left =
              () -> {
                latch.await();
                try {
                  HelixSegmentResource.replaceSegments(
                      accessor, "events", OFFLINE, List.of("a"), List.of("left"), 10);
                  return true;
                } catch (IOException e) {
                  return false;
                }
              };
          java.util.concurrent.Callable<Boolean> right =
              () -> {
                latch.await();
                try {
                  HelixSegmentResource.replaceSegments(
                      accessor, "events", OFFLINE, List.of("a"), List.of("right"), 10);
                  return true;
                } catch (IOException e) {
                  return false;
                }
              };
          var a = executor.submit(left);
          var b = executor.submit(right);
          latch.countDown();
          assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
              .containsExactlyInAnyOrder(true, false);
        } finally {
          executor.shutdownNow();
        }
      } finally {
        manager.disconnect();
        admin.close();
      }
    }
  }
}
