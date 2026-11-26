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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Mutable, not thread-safe row. Values are held by reference; null values are supported. */
public final class GenericRow {
  private final Map<String, Object> values = new LinkedHashMap<>();

  public GenericRow put(String name, Object value) {
    Schema.requireName(name);
    values.put(name, value);
    return this;
  }
  public Object get(String name) { return values.get(Objects.requireNonNull(name, "name")); }
  public boolean contains(String name) { return values.containsKey(Objects.requireNonNull(name, "name")); }
  /** Returns an unmodifiable shallow snapshot, not a live view. */
  public Map<String, Object> values() { return Collections.unmodifiableMap(new LinkedHashMap<>(values)); }
  public void clear() { values.clear(); }
}
