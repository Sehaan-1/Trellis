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
package io.trellis.segment.local;

import io.trellis.spi.Schema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/** Extracts and validates a local archive without publishing it. */
public final class SegmentPreparation {
  private SegmentPreparation() {}

  /** Statistics from extraction and validation. */
  public record Report(SegmentArchives.Report archive, SegmentVerifier.Report segment) {}

  /**
   * Extracts a flat ZIP into a new target and verifies it. The archive, parent, and target must be
   * on a trusted filesystem and remain stable without out-of-band changes. The target is visible
   * during preparation; consumers must wait for success. On failure, cleanup is attempted, and a
   * cleanup failure is suppressed on the original exception. This does not publish atomically,
   * authenticate content, guarantee durability, or bound the verifier's CPU or memory.
   */
  public static Report prepare(
      Path archive, Path target, SegmentArchives.Limits limits, Schema expected, int maxDocuments)
      throws IOException {
    return prepare(archive, target, limits, expected, maxDocuments, null);
  }

  static Report prepare(
      Path archive,
      Path target,
      SegmentArchives.Limits limits,
      Schema expected,
      int maxDocuments,
      Integer expectedDocuments)
      throws IOException {
    Objects.requireNonNull(archive, "archive");
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(limits, "limits");
    Objects.requireNonNull(expected, "expected");
    if (maxDocuments < 0) throw new IllegalArgumentException("maxDocuments must be nonnegative");
    LocalSegments.validateSchema(expected);
    SegmentArchives.Report extraction = SegmentArchives.extract(archive, target, limits);
    try {
      SegmentVerifier.Report verified = SegmentVerifier.verify(target, expected, maxDocuments);
      if (expectedDocuments != null && verified.documents() != expectedDocuments)
        throw new IOException("segment document count mismatch");
      return new Report(extraction, verified);
    } catch (IOException | RuntimeException | Error failure) {
      try {
        try (var files = Files.list(target)) {
          for (Path file : files.toList()) Files.delete(file);
        }
        Files.delete(target);
      } catch (IOException cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }
}
