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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable ordered schema. Field names are case sensitive. */
public final class Schema {
  public enum DataType {
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    STRING,
    BYTES,
    TIMESTAMP
  }

  public record Field(String name, DataType type, boolean multiValue) {
    public Field {
      requireName(name);
      Objects.requireNonNull(type, "type");
    }
  }

  private final String name;
  private final List<Field> fields;
  private final Map<String, Field> byName;

  public Schema(String name, List<Field> fields) {
    this.name = requireName(name);
    Objects.requireNonNull(fields, "fields");
    if (fields.isEmpty())
      throw new IllegalArgumentException("schema must contain at least one field");
    Map<String, Field> index = new LinkedHashMap<>();
    for (Field field : fields) {
      Objects.requireNonNull(field, "field");
      if (index.putIfAbsent(field.name(), field) != null)
        throw new IllegalArgumentException("duplicate field: " + field.name());
    }
    this.fields = List.copyOf(fields);
    this.byName = Map.copyOf(index);
  }

  public String name() {
    return name;
  }

  public List<Field> fields() {
    return fields;
  }

  public Optional<Field> field(String name) {
    return Optional.ofNullable(byName.get(Objects.requireNonNull(name, "name")));
  }

  static String requireName(String name) {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    return name;
  }
}
