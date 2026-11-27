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

import io.trellis.spi.TableConfig;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Canonical Helix identities shared by controllers and participants. */
public final class SegmentHelixIdentity {
  private SegmentHelixIdentity() {}

  private static final int MAX_NAME_BYTES = 128;

  /** Logical table identity; it is not an authorization or metadata lookup. */
  public record Table(String name, TableConfig.TableType type) {
    public Table {
      encodeName(name);
      Objects.requireNonNull(type, "type");
    }
  }

  /** Returns a deterministic ZooKeeper-safe resource name. */
  public static String resourceName(String tableName, TableConfig.TableType type) {
    Objects.requireNonNull(type, "type");
    return "trellis_" + type.name() + "_" + encodeName(tableName);
  }

  /** Decodes only canonical resource names produced by this class. */
  public static Table table(String resourceName) {
    Objects.requireNonNull(resourceName, "resourceName");
    String offline = "trellis_OFFLINE_";
    String realtime = "trellis_REALTIME_";
    TableConfig.TableType type;
    String encoded;
    if (resourceName.startsWith(offline)) {
      type = TableConfig.TableType.OFFLINE;
      encoded = resourceName.substring(offline.length());
    } else if (resourceName.startsWith(realtime)) {
      type = TableConfig.TableType.REALTIME;
      encoded = resourceName.substring(realtime.length());
    } else {
      throw new IllegalArgumentException("invalid resource name");
    }
    String name = decodeName(encoded);
    if (!resourceName(name, type).equals(resourceName))
      throw new IllegalArgumentException("noncanonical resource name");
    return new Table(name, type);
  }

  /** Returns a deterministic partition name for a logical segment. */
  public static String partitionName(String segmentName) {
    return "s_" + encodeName(segmentName);
  }

  /** Decodes only canonical partition names produced by this class. */
  public static String segmentName(String partitionName) {
    Objects.requireNonNull(partitionName, "partitionName");
    if (!partitionName.startsWith("s_"))
      throw new IllegalArgumentException("invalid partition name");
    String name = decodeName(partitionName.substring(2));
    if (!partitionName(name).equals(partitionName))
      throw new IllegalArgumentException("noncanonical partition name");
    return name;
  }

  private static String decodeName(String encoded) {
    if (encoded.length() > 171) throw new IllegalArgumentException("encoded name too long");
    try {
      byte[] bytes = Base64.getUrlDecoder().decode(encoded);
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes))
          .toString();
    } catch (CharacterCodingException | IllegalArgumentException e) {
      throw new IllegalArgumentException("invalid encoded name", e);
    }
  }

  private static String encodeName(String name) {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    try {
      ByteBuffer bytes =
          StandardCharsets.UTF_8
              .newEncoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .encode(CharBuffer.wrap(name));
      if (bytes.remaining() > MAX_NAME_BYTES)
        throw new IllegalArgumentException("name exceeds UTF-8 byte limit");
      byte[] copy = new byte[bytes.remaining()];
      bytes.get(copy);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(copy);
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("name must be valid Unicode", e);
    }
  }
}
