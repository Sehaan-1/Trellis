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

import io.trellis.spi.Schema;
import java.util.List;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.junit.jupiter.api.Test;

class SchemaRecordTest {
  @Test void preservesFieldsOrderAndRejectsInvalidRecords() {
    var schema = new Schema("events", List.of(new Schema.Field("text", Schema.DataType.STRING, false), new Schema.Field("nums", Schema.DataType.LONG, true)));
    var decoded = SchemaRecord.decode(SchemaRecord.encode(schema));
    assertThat(decoded.name()).isEqualTo(schema.name());
    assertThat(decoded.fields()).isEqualTo(schema.fields());
    assertThatNullPointerException().isThrownBy(() -> SchemaRecord.encode(null));
    assertThatNullPointerException().isThrownBy(() -> SchemaRecord.decode(null));
    var invalid = new ZNRecord("s");
    assertThatThrownBy(() -> SchemaRecord.decode(invalid)).isInstanceOf(IllegalArgumentException.class);
    invalid.setListField("trellis.fields", List.of("x"));
    assertThatThrownBy(() -> SchemaRecord.decode(invalid)).isInstanceOf(IllegalArgumentException.class);
    invalid.setMapField("trellis.field.0", Map.of("type", "LONG", "multiValue", "yes"));
    assertThatThrownBy(() -> SchemaRecord.decode(invalid)).isInstanceOf(IllegalArgumentException.class);
    invalid.setMapField("trellis.field.0", Map.of("type", "WRONG", "multiValue", "true"));
    assertThatThrownBy(() -> SchemaRecord.decode(invalid)).isInstanceOf(IllegalArgumentException.class);
  }
}
