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
package io.trellis.segment.spi;

import io.trellis.spi.Schema;
import java.io.IOException;

/** Read-only columnar segment. Document IDs are zero-based. Close must not race with reads. */
public interface ImmutableSegment extends AutoCloseable {
  Schema schema();

  int rowCount();

  long getLong(int documentId, String column) throws IOException;

  String getString(int documentId, String column) throws IOException;

  /** Ordinals are zero-based in Lucene UTF-8 byte order, not locale collation order. */
  int dictionaryOrdinal(int documentId, String column) throws IOException;

  int dictionarySize(String column) throws IOException;

  String dictionaryValue(String column, int ordinal) throws IOException;

  @Override
  void close() throws IOException;
}
