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

import java.io.IOException;
import java.util.List;

/** Search contracts return at most the requested limit in descending Lucene relevance score. */
public final class SearchIndexes {
  private SearchIndexes() {}
  public record Hit(int documentId, float score) {}
  public interface TextIndex extends AutoCloseable {
    /** All tokens emitted by the analyzer must match. No tokens means no results. */
    List<Hit> search(String text, int limit) throws IOException;
    @Override void close() throws IOException;
  }
  public interface VectorIndex extends AutoCloseable {
    /** Approximate nearest neighbors under Lucene's Euclidean similarity. */
    List<Hit> nearest(float[] vector, int limit) throws IOException;
    @Override void close() throws IOException;
  }
}
