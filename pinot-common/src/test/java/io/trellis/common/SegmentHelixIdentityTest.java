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
package io.trellis.common;

import static org.assertj.core.api.Assertions.*;

import io.trellis.spi.TableConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class SegmentHelixIdentityTest {
  @Test
  void roundTripsAndPreservesEstablishedNames() {
    for (var type : TableConfig.TableType.values()) {
      for (String name : List.of("a", "events / å", "a".repeat(128), "😀".repeat(32))) {
        String resource = SegmentHelixIdentity.resourceName(name, type);
        assertThat(SegmentHelixIdentity.table(resource))
            .isEqualTo(new SegmentHelixIdentity.Table(name, type));
        assertThat(resource).matches("[A-Za-z0-9_-]+");
        assertThat(SegmentHelixIdentity.segmentName(SegmentHelixIdentity.partitionName(name)))
            .isEqualTo(name);
      }
    }
    assertThat(SegmentHelixIdentity.resourceName("events", TableConfig.TableType.OFFLINE))
        .isEqualTo("trellis_OFFLINE_ZXZlbnRz");
    assertThat(SegmentHelixIdentity.partitionName("part / å")).isEqualTo("s_cGFydCAvIMOl");
    assertThat(SegmentHelixIdentity.resourceName("events", TableConfig.TableType.OFFLINE))
        .isNotEqualTo(SegmentHelixIdentity.resourceName("events", TableConfig.TableType.REALTIME));
  }

  @Test
  void rejectsInvalidNamesAndKeys() {
    for (String name : List.of("", " ", "\uD800", "a".repeat(129), "😀".repeat(33))) {
      assertThatThrownBy(() -> SegmentHelixIdentity.partitionName(name))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () -> SegmentHelixIdentity.resourceName(name, TableConfig.TableType.OFFLINE))
          .isInstanceOf(IllegalArgumentException.class);
    }
    for (String name :
        List.of("s_", "x_YQ", "s_YQ==", "s_-", "s__w", "s_IA", "s_" + "A".repeat(172)))
      assertThatThrownBy(() -> SegmentHelixIdentity.segmentName(name))
          .isInstanceOf(IllegalArgumentException.class);
    for (String name :
        List.of(
            "trellis_OTHER_YQ",
            "trellis_OFFLINE_",
            "trellis_OFFLINE_YQ==",
            "trellis_REALTIME__w",
            "trellis_OFFLINE_IA",
            "trellis_OFFLINE_" + "A".repeat(172)))
      assertThatThrownBy(() -> SegmentHelixIdentity.table(name))
          .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> SegmentHelixIdentity.partitionName(null));
    assertThatNullPointerException().isThrownBy(() -> SegmentHelixIdentity.segmentName(null));
    assertThatNullPointerException()
        .isThrownBy(() -> SegmentHelixIdentity.resourceName(null, TableConfig.TableType.OFFLINE));
    assertThatNullPointerException().isThrownBy(() -> SegmentHelixIdentity.resourceName("a", null));
    assertThatNullPointerException().isThrownBy(() -> SegmentHelixIdentity.table(null));
    assertThatNullPointerException().isThrownBy(() -> new SegmentHelixIdentity.Table("a", null));
  }
}
