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
package io.trellis.core;

import io.trellis.common.SqlQueries;
import io.trellis.segment.spi.ImmutableSegment;
import io.trellis.spi.Schema;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;

/** Local, materializing Calcite execution over one logical table of immutable segments. */
public final class LocalQueryExecutor {
  private LocalQueryExecutor() {}

  /**
   * Runs one syntactic query. Segment schemas must match exactly; the caller owns their lifetime
   * and must not close them concurrently. Names are case-sensitive. The result limit rejects,
   * rather than truncates, excess output rows. This is not a bound on intermediate work or memory.
   */
  public static QueryResult execute(
      String tableName, List<? extends ImmutableSegment> segments, String sql, int maxRows)
      throws SQLException {
    Objects.requireNonNull(tableName, "tableName");
    if (tableName.isBlank()) throw new IllegalArgumentException("table name must not be blank");
    Objects.requireNonNull(sql, "sql");
    if (maxRows <= 0) throw new IllegalArgumentException("maxRows must be positive");
    List<ImmutableSegment> inputs = List.copyOf(Objects.requireNonNull(segments, "segments"));
    if (inputs.isEmpty()) throw new IllegalArgumentException("at least one segment is required");
    Schema schema = inputs.get(0).schema();
    for (Schema.Field field : schema.fields()) {
      if (field.multiValue()
          || (field.type() != Schema.DataType.STRING && field.type() != Schema.DataType.LONG))
        throw new IllegalArgumentException("unsupported field: " + field.name());
    }
    for (ImmutableSegment segment : inputs) {
      if (!schema.name().equals(segment.schema().name())
          || !schema.fields().equals(segment.schema().fields()))
        throw new IllegalArgumentException("segment schemas do not match");
    }
    try {
      if (!SqlQueries.parse(sql).getKind().belongsTo(SqlKind.QUERY))
        throw new SQLException("only queries are supported");
    } catch (SqlParseException e) {
      throw new SQLException("invalid SQL query", e);
    }
    Properties properties = new Properties();
    properties.setProperty("caseSensitive", "true");
    properties.setProperty("unquotedCasing", "UNCHANGED");
    properties.setProperty("quotedCasing", "UNCHANGED");
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
      connection
          .unwrap(CalciteConnection.class)
          .getRootSchema()
          .add(tableName, new SegmentTable(inputs, schema));
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(sql)) {
        ResultSetMetaData metadata = result.getMetaData();
        List<String> columns = new ArrayList<>();
        for (int i = 1; i <= metadata.getColumnCount(); i++)
          columns.add(metadata.getColumnLabel(i));
        List<List<Object>> rows = new ArrayList<>();
        while (result.next()) {
          if (rows.size() == maxRows)
            throw new SQLDataException("query exceeds maxRows: " + maxRows);
          List<Object> row = new ArrayList<>(columns.size());
          for (int i = 1; i <= columns.size(); i++) row.add(result.getObject(i));
          rows.add(row);
        }
        return new QueryResult(columns, rows);
      }
    }
  }
}
