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

/** In-memory bitmap query contracts. Returned document IDs are sorted, unique snapshots. */
public final class BitmapIndexes {
  private BitmapIndexes() {}
  @FunctionalInterface
  public interface StringIndex { int[] equalTo(String value); }
  public interface LongIndex {
    int[] equalTo(long value);
    /** Inclusive endpoints; an inverted interval yields no matches. */
    int[] between(long lower, long upper);
  }
}
