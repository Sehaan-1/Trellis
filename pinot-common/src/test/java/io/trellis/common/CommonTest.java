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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.junit.jupiter.api.Test;

class CommonTest {
  @Test void parsesQueriesAndRejectsInvalidSql() throws Exception {
    assertThat(SqlQueries.parse("SELECT team, COUNT(*) FROM games GROUP BY team").getKind()).isEqualTo(SqlKind.SELECT);
    assertThatThrownBy(() -> SqlQueries.parse("SELECT FROM")).isInstanceOf(SqlParseException.class);
    assertThatThrownBy(() -> SqlQueries.parse("SELECT 1; SELECT 2")).isInstanceOf(SqlParseException.class);
    assertThatNullPointerException().isThrownBy(() -> SqlQueries.parse(null));
  }
  @Test void helixRecordRoundTripAndInvalidRecords() {
    for (var type : TableConfig.TableType.values()) {
      var config = new TableConfig("events", "eventSchema", type);
      var record = TableConfigRecord.encode(config);
      assertThat(record.getId()).isEqualTo("events");
      assertThat(TableConfigRecord.decode(record)).isEqualTo(config);
    }
    var missing = new ZNRecord("events");
    assertThatThrownBy(() -> TableConfigRecord.decode(missing)).isInstanceOf(IllegalArgumentException.class);
    missing.setSimpleField("schemaName", "s");
    missing.setSimpleField("tableType", "OTHER");
    assertThatThrownBy(() -> TableConfigRecord.decode(missing)).isInstanceOf(IllegalArgumentException.class);
    assertThatNullPointerException().isThrownBy(() -> TableConfigRecord.encode(null));
    assertThatNullPointerException().isThrownBy(() -> TableConfigRecord.decode(null));
  }
}
