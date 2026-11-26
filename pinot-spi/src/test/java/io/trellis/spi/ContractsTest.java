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
package io.trellis.spi;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ContractsTest {
  @Test
  void schemaValidatesAndCopiesFields() {
    var field = new Schema.Field("score", Schema.DataType.INT, false);
    var input = new ArrayList<>(List.of(field));
    var schema = new Schema("baseball", input);
    input.clear();
    assertThat(schema.name()).isEqualTo("baseball");
    assertThat(schema.fields()).containsExactly(field);
    assertThat(schema.field("score")).contains(field);
    assertThat(schema.field("SCORE")).isEmpty();
    assertThatThrownBy(() -> schema.fields().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> new Schema("s", List.of(field, field)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Schema("s", List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Schema.Field(" ", Schema.DataType.STRING, false))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> new Schema.Field("x", null, false));
  }

  @Test
  void tableIdentityValidates() {
    var config = new TableConfig("events", "eventSchema", TableConfig.TableType.REALTIME);
    assertThat(config.tableName()).isEqualTo("events");
    assertThat(config.schemaName()).isEqualTo("eventSchema");
    assertThat(config.type()).isEqualTo(TableConfig.TableType.REALTIME);
    assertThatThrownBy(() -> new TableConfig("", "s", TableConfig.TableType.OFFLINE))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> new TableConfig("t", "s", null));
  }

  @Test
  void rowDistinguishesNullAndMissingAndSnapshots() {
    var row = new GenericRow().put("a", null);
    var snapshot = row.values();
    row.put("a", 1).put("b", "hello");
    assertThat(snapshot).containsEntry("a", null).doesNotContainKey("b");
    assertThatThrownBy(() -> snapshot.clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThat(row.contains("a")).isTrue();
    assertThat(row.get("b")).isEqualTo("hello");
    row.clear();
    assertThat(row.contains("a")).isFalse();
    assertThat(row.get("a")).isNull();
    assertThatThrownBy(() -> row.put(" ", 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decoderContractCanBeImplemented() {
    RecordDecoder de