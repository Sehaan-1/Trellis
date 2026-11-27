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

import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import org.apache.curator.framework.CuratorFramework;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;

/** Version-checked persistent metadata operations using the caller-owned Curator client. */
public final class ZooKeeperMetadataStore {
  /** The ZooKeeper data version, not an identity or globally unique generation. */
  public record Versioned<T>(T value, int version) {
    public Versioned {
      Objects.requireNonNull(value, "value");
      if (version < 0) throw new IllegalArgumentException("negative version");
    }
  }

  private static final String FORMAT = "trellis.metadata.version";
  private static final ZNRecordSerializer SERIALIZER = new ZNRecordSerializer();
  // Helix's serializer configures a shared ObjectMapper while serializing/deserializing.
  private static final Object SERIALIZER_LOCK = new Object();
  private final CuratorFramework client;
  private final String base;

  /** Caller must start and close the client and configure its authentication and ACL provider. */
  public ZooKeeperMetadataStore(CuratorFramework client, String basePath) {
    this.client = Objects.requireNonNull(client, "client");
    Objects.requireNonNull(basePath, "basePath");
    PathUtils.validatePath(basePath);
    this.base = basePath.equals("/") ? "" : basePath;
  }

  public int createSchema(Schema schema) throws KeeperException, InterruptedException, IOException {
    Objects.requireNonNull(schema, "schema");
    validateSchemaNames(schema);
    return create(schemaPath(schema.name()), SchemaRecord.encode(schema));
  }

  public Versioned<Schema> getSchema(String name)
      throws KeeperException, InterruptedException, IOException {
    var stored = read(schemaPath(name));
    try {
      Schema schema = SchemaRecord.decode(stored.value());
      if (!schema.name().equals(name))
        throw new IllegalArgumentException("schema identity mismatch");
      return new Versioned<>(schema, stored.version());
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IOException("invalid schema metadata", e);
    }
  }

  public int updateSchema(Schema schema, int version)
      throws KeeperException, InterruptedException, IOException {
    Objects.requireNonNull(schema, "schema");
    validateSchemaNames(schema);
    return update(schemaPath(schema.name()), SchemaRecord.encode(schema), version);
  }

  public void deleteSchema(String name, int version)
      throws KeeperException, InterruptedException, IOException {
    delete(schemaPath(name), version);
  }

  public int createTable(TableConfig config)
      throws KeeperException, InterruptedException, IOException {
    Objects.requireNonNull(config, "config");
    nameKey(config.schemaName());
    return create(tablePath(config.tableName(), config.type()), TableConfigRecord.encode(config));
  }

  public Versioned<TableConfig> getTable(String name, TableConfig.TableType type)
      throws KeeperException, InterruptedException, IOException {
    var stored = read(tablePath(name, type));
    try {
      TableConfig config = TableConfigRecord.decode(stored.value());
      if (!config.tableName().equals(name) || config.type() != type)
        throw new IllegalArgumentException("table identity mismatch");
      return new Versioned<>(config, stored.version());
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IOException("invalid table metadata", e);
    }
  }

  public int updateTable(TableConfig config, int version)
      throws KeeperException, InterruptedException, IOException {
    Objects.requireNonNull(config, "config");
    nameKey(config.schemaName());
    return update(
        tablePath(config.tableName(), config.type()), TableConfigRecord.encode(config), version);
  }

  public void deleteTable(String name, TableConfig.TableType type, int version)
      throws KeeperException, InterruptedException, IOException {
    delete(tablePath(name, type), version);
  }

  /** Creates a descriptor; this does not validate or publish the referenced blob. */
  public int createSegment(SegmentDescriptor descriptor)
      throws KeeperException, InterruptedException, IOException {
    Objects.requireNonNull(descriptor, "descriptor");
    validateSchemaNames(descriptor.schema());
    return create(
        segmentPath(descriptor.tableName(), descriptor.tableType(), descriptor.segmentName()),
        SegmentDescriptorRecord.encode(descriptor));
  }

  /** Reads a descriptor, rejecting malformed data and identity mismatches. */
  public Versioned<SegmentDescriptor> getSegment(
      String tableName, TableConfig.TableType type, String segmentName)
      throws KeeperException, InterruptedException, IOException {
    var stored = read(segmentPath(tableName, type, segmentName));
    try {
      SegmentDescriptor descriptor = SegmentDescriptorRecord.decode(stored.value());
      if (!descriptor.tableName().equals(tableName)
          || descriptor.tableType() != type
          || !descriptor.segmentName().equals(segmentName))
        throw new IllegalArgumentException("segment identity mismatch");
      validateSchemaNames(descriptor.schema());
      return new Versioned<>(descriptor, stored.version());
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IOException("invalid segment metadata", e);
    }
  }

  /** Replaces a descriptor under the same identity using the expected ZooKeeper version. */
  public int updateSegment(SegmentDescriptor descriptor, int version)
      throws KeeperException, InterruptedException, IOException {
    Objects.requireNonNull(descriptor, "descriptor");
    validateSchemaNames(descriptor.schema());
    return update(
        segmentPath(descriptor.tableName(), descriptor.tableType(), descriptor.segmentName()),
        SegmentDescriptorRecord.encode(descriptor),
        version);
  }

  /** Deletes one descriptor using the expected ZooKeeper version, without deleting its blob. */
  public void deleteSegment(
      String tableName, TableConfig.TableType type, String segmentName, int version)
      throws KeeperException, InterruptedException, IOException {
    delete(segmentPath(tableName, type, segmentName), version);
  }

  private static void validateSchemaNames(Schema schema) {
    nameKey(schema.name());
    for (Schema.Field field : schema.fields()) nameKey(field.name());
  }

  private String schemaPath(String name) {
    return base + "/schemas/" + nameKey(name);
  }

  private String tablePath(String name, TableConfig.TableType type) {
    Objects.requireNonNull(type, "type");
    return base + "/tables/" + type.name() + "/" + nameKey(name);
  }

  private String segmentPath(String tableName, TableConfig.TableType type, String segmentName) {
    Objects.requireNonNull(type, "type");
    return base
        + "/segments/"
        + type.name()
        + "/"
        + nameKey(tableName)
        + "/"
        + nameKey(segmentName);
  }

  private static String nameKey(String name) {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    try {
      ByteBuffer bytes =
          StandardCharsets.UTF_8
              .newEncoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .encode(CharBuffer.wrap(name));
      byte[] copy = new byte[bytes.remaining()];
      bytes.get(copy);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(copy);
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException("name must be valid Unicode", e);
    }
  }

  private static void requireVersion(int version) {
    if (version < 0) throw new IllegalArgumentException("version must be nonnegative");
  }

  private static byte[] encode(ZNRecord record) throws IOException {
    record.setSimpleField(FORMAT, "1");
    try {
      synchronized (SERIALIZER_LOCK) {
        return SERIALIZER.serialize(record);
      }
    } catch (RuntimeException e) {
      throw new IOException("could not serialize metadata", e);
    }
  }

  private static ZNRecord decode(byte[] data) throws IOException {
    Object value;
    try {
      synchronized (SERIALIZER_LOCK) {
        value = SERIALIZER.deserialize(data);
      }
    } catch (RuntimeException e) {
      throw new IOException("could not deserialize metadata", e);
    }
    if (!(value instanceof ZNRecord record) || !"1".equals(record.getSimpleField(FORMAT)))
      throw new IOException("invalid or unsupported metadata format");
    return record;
  }

  private int create(String path, ZNRecord record)
      throws KeeperException, InterruptedException, IOException {
    byte[] data = encode(record);
    try {
      client.create().creatingParentsIfNeeded().forPath(path, data);
      return 0;
    } catch (KeeperException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      throw new IOException("metadata create failed", e);
    }
  }

  private Versioned<ZNRecord> read(String path)
      throws KeeperException, InterruptedException, IOException {
    Stat stat = new Stat();
    byte[] data;
    try {
      data = client.getData().storingStatIn(stat).forPath(path);
    } catch (KeeperException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      throw new IOException("metadata read failed", e);
    }
    return new Versioned<>(decode(data), stat.getVersion());
  }

  private int update(String path, ZNRecord record, int version)
      throws KeeperException, InterruptedException, IOException {
    requireVersion(version);
    byte[] data = encode(record);
    try {
      return client.setData().withVersion(version).forPath(path, data).getVersion();
    } catch (KeeperException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      throw new IOException("metadata update failed", e);
    }
  }

  private void delete(String path, int version)
      throws KeeperException, InterruptedException, IOException {
    requireVersion(version);
    try {
      client.delete().withVersion(version).forPath(path);
    } catch (KeeperException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      throw new IOException("metadata delete failed", e);
    }
  }
}
