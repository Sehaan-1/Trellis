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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Materialized result; lists are immutable snapshots, while cell values are shallow references. */
public record QueryResult(List<String> columns, List<List<Object>> rows) {
  public QueryResult {
    columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
    Objects.requireNonNull(rows, "rows");
    List<List<Object>> copy = new ArrayList<>(rows.size());
    for (List<Object> row : rows) {
      Objects.requireNonNull(row, "row");
      if (row.size() != columns.size()) throw new IllegalArgumentException("row width does not match columns");
      copy.add(Collections.unmodifiableList(new ArrayList<>(row)));
    }
    rows = Collections.unmodifiableList(copy);
  }
}
