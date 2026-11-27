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
package io.trellis.controller;

import io.trellis.common.ZooKeeperMetadataStore;
import io.trellis.spi.Schema;
import io.trellis.spi.SegmentDescriptor;
import io.trellis.spi.TableConfig;
import io.trellis.spi.storage.BlobStore;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.EntityTag;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.util.List;
import java.util.Objects;
import org.apache.zookeeper.KeeperException;

/** HTTP metadata endpoints. Not an authentication or authorization boundary. */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class MetadataResource {
  public record FieldBody(String name, String type, Boolean multiValue) {}

  public record SchemaBody(String name, List<FieldBody> fields) {}

  public record TableBody(String tableName, String schemaName, String type) {}

  public record SegmentBody(
      String tableName,
      String type,
      String segmentName,
      String schemaName,
      List<FieldBody> fields,
      String sha256,
      Long bytes,
      Integer documents) {}

  public record ErrorBody(String error) {}

  private final ZooKeeperMetadataStore store;

  @Inject
  public MetadataResource(ZooKeeperMetadataStore store) {
    this.store = Objects.requireNonNull(store, "store");
  }

  static String name(String s) {
    if (s == null || !s.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"))
      throw new IllegalArgumentException("invalid resource name");
    return s;
  }

  private static Schema schema(SchemaBody body) {
    if (body == null || body.fields() == null) throw new IllegalArgumentException("missing schema");
    List<Schema.Field> fields =
        body.fields().stream()
            .map(
                f -> {
                  if (f == null || f.type() == null || f.multiValue() == null)
                    throw new IllegalArgumentException("invalid field");
                  return new Schema.Field(
                      name(f.name()), Schema.DataType.valueOf(f.type()), f.multiValue());
                })
            .toList();
    return new Schema(name(body.name()), fields);
  }

  private static SchemaBody body(Schema schema) {
    return new SchemaBody(
        schema.name(),
        schema.fields().stream()
            .map(f -> new FieldBody(f.name(), f.type().name(), f.multiValue()))
            .toList());
  }

  private static SegmentDescriptor segment(SegmentBody body) {
    if (body == null
        || body.type() == null
        || body.sha256() == null
        || body.bytes() == null
        || body.documents() == null) throw new IllegalArgumentException("missing segment metadata");
    Schema snapshot = schema(new SchemaBody(body.schemaName(), body.fields()));
    return new SegmentDescriptor(
        name(body.tableName()),
        type(body.type()),
        name(body.segmentName()),
        snapshot.name(),
        snapshot.fields(),
        new BlobStore.StoredBlob(new BlobStore.Key(body.sha256()), body.bytes()),
        body.documents());
  }

  private static SegmentBody body(SegmentDescriptor segment) {
    return new SegmentBody(
        segment.tableName(),
        segment.tableType().name(),
        segment.segmentName(),
        segment.schemaName(),
        body(segment.schema()).fields(),
        segment.blob().key().sha256(),
        segment.blob().size(),
        segment.documents());
  }

  static int segmentLimit(String value) {
    if (value == null || !value.matches("[1-9][0-9]{0,4}"))
      throw new IllegalArgumentException("max must be between 1 and 10000");
    int count = Integer.parseInt(value);
    if (count > 10000) throw new IllegalArgumentException("max must be between 1 and 10000");
    return count;
  }

  private static TableConfig table(TableBody body) {
    if (body == null || body.type() == null) throw new IllegalArgumentException("missing table");
    return new TableConfig(
        name(body.tableName()),
        name(body.schemaName()),
        TableConfig.TableType.valueOf(body.type()));
  }

  private static TableBody body(TableConfig table) {
    return new TableBody(table.tableName(), table.schemaName(), table.type().name());
  }

  static TableConfig.TableType type(String type) {
    try {
      return TableConfig.TableType.valueOf(type);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("invalid table type", e);
    }
  }

  private static int ifMatch(String value) {
    if (value == null) throw new WebApplicationException("If-Match required", 428);
    if (!value.matches("\"(?:0|[1-9][0-9]*)\""))
      throw new IllegalArgumentException("If-Match must be a strong numeric ETag");
    try {
      return Integer.parseInt(value.substring(1, value.length() - 1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid ETag", e);
    }
  }

  static EntityTag tag(int version) {
    return new EntityTag(Integer.toString(version));
  }

  @FunctionalInterface
  interface Action {
    Response run() throws Exception;
  }

  static Response perform(Action action) {
    try {
      return action.run();
    } catch (WebApplicationException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      return Response.status(400).entity(new ErrorBody("invalid request")).build();
    } catch (SegmentArchiveUpload.TooManyUploadsException e) {
      return Response.status(429).entity(new ErrorBody("too many concurrent uploads")).build();
    } catch (SegmentArchiveUpload.TooLargeException e) {
      return Response.status(413).entity(new ErrorBody("archive exceeds size limit")).build();
    } catch (ZooKeeperMetadataStore.SegmentLimitExceededException e) {
      return Response.status(422).entity(new ErrorBody("segment count exceeds max")).build();
    } catch (KeeperException.NodeExistsException e) {
      return Response.status(409).entity(new ErrorBody("already exists")).build();
    } catch (KeeperException.BadVersionException e) {
      return Response.status(412).entity(new ErrorBody("version mismatch")).build();
    } catch (KeeperException.NoNodeException e) {
      return Response.status(404).entity(new ErrorBody("not found")).build();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Response.status(503)
          .entity(new ErrorBody("metadata unavailable; write outcome may be unknown"))
          .build();
    } catch (KeeperException e) {
      return Response.status(503)
          .entity(new ErrorBody("metadata unavailable; write outcome may be unknown"))
          .build();
    } catch (Exception e) {
      return Response.serverError()
          .entity(new ErrorBody("metadata operation failed; write outcome may be unknown"))
          .build();
    }
  }

  @POST
  @Path("schemas")
  public Response createSchema(SchemaBody request, @Context UriInfo uri) {
    return perform(
        () -> {
          Schema s = schema(request);
          int version = store.createSchema(s);
          return Response.created(uri.getAbsolutePathBuilder().path(s.name()).build())
              .tag(tag(version))
              .build();
        });
  }

  @GET
  @Path("schemas/{name}")
  public Response getSchema(@PathParam("name") String name) {
    return perform(
        () -> {
          var s = store.getSchema(name(name));
          return Response.ok(body(s.value())).tag(tag(s.version())).build();
        });
  }

  @PUT
  @Path("schemas/{name}")
  public Response updateSchema(
      @PathParam("name") String name, @HeaderParam("If-Match") String match, SchemaBody request) {
    return perform(
        () -> {
          int v = ifMatch(match);
          Schema s = schema(request);
          if (!name(name).equals(s.name())) throw new IllegalArgumentException("identity mismatch");
          return Response.noContent().tag(tag(store.updateSchema(s, v))).build();
        });
  }

  @DELETE
  @Path("schemas/{name}")
  public Response deleteSchema(
      @PathParam("name") String name, @HeaderParam("If-Match") String match) {
    return perform(
        () -> {
          int v = ifMatch(match);
          store.deleteSchema(name(name), v);
          return Response.noContent().build();
        });
  }

  @POST
  @Path("tables")
  public Response createTable(TableBody request, @Context UriInfo uri) {
    return perform(
        () -> {
          TableConfig t = table(request);
          int version = store.createTable(t);
          return Response.created(
                  uri.getAbsolutePathBuilder().path(t.type().name()).path(t.tableName()).build())
              .tag(tag(version))
              .build();
        });
  }

  @GET
  @Path("tables/{type}/{name}")
  public Response getTable(@PathParam("type") String type, @PathParam("name") String name) {
    return perform(
        () -> {
          var t = store.getTable(name(name), type(type));
          return Response.ok(body(t.value())).tag(tag(t.version())).build();
        });
  }

  @PUT
  @Path("tables/{type}/{name}")
  public Response updateTable(
      @PathParam("type") String type,
      @PathParam("name") String name,
      @HeaderParam("If-Match") String match,
      TableBody request) {
    return perform(
        () -> {
          int v = ifMatch(match);
          TableConfig t = table(request);
          if (!name(name).equals(t.tableName()) || type(type) != t.type())
            throw new IllegalArgumentException("identity mismatch");
          return Response.noContent().tag(tag(store.updateTable(t, v))).build();
        });
  }

  @DELETE
  @Path("tables/{type}/{name}")
  public Response deleteTable(
      @PathParam("type") String type,
      @PathParam("name") String name,
      @HeaderParam("If-Match") String match) {
    return perform(
        () -> {
          int v = ifMatch(match);
          store.deleteTable(name(name), type(type), v);
          return Response.noContent().build();
        });
  }

  @GET
  @Path("tables/{type}/{name}/segments")
  public Response listSegments(
      @PathParam("type") String type,
      @PathParam("name") String table,
      @QueryParam("max") String max) {
    return perform(
        () ->
            Response.ok(store.listSegmentNames(name(table), type(type), segmentLimit(max)))
                .build());
  }

  @POST
  @Path("tables/{type}/{name}/segments")
  public Response createSegment(
      @PathParam("type") String type,
      @PathParam("name") String table,
      SegmentBody request,
      @Context UriInfo uri) {
    return perform(
        () -> {
          var segment = segment(request);
          if (!name(table).equals(segment.tableName()) || type(type) != segment.tableType())
            throw new IllegalArgumentException("identity mismatch");
          int version = store.createSegment(segment);
          return Response.created(uri.getAbsolutePathBuilder().path(segment.segmentName()).build())
              .tag(tag(version))
              .build();
        });
  }

  @GET
  @Path("tables/{type}/{name}/segments/{segment}")
  public Response getSegment(
      @PathParam("type") String type,
      @PathParam("name") String table,
      @PathParam("segment") String segment) {
    return perform(
        () -> {
          var value = store.getSegment(name(table), type(type), name(segment));
          return Response.ok(body(value.value())).tag(tag(value.version())).build();
        });
  }

  @PUT
  @Path("tables/{type}/{name}/segments/{segment}")
  public Response updateSegment(
      @PathParam("type") String type,
      @PathParam("name") String table,
      @PathParam("segment") String segment,
      @HeaderParam("If-Match") String match,
      SegmentBody request) {
    return perform(
        () -> {
          int version = ifMatch(match);
          var value = segment(request);
          if (!name(table).equals(value.tableName())
              || type(type) != value.tableType()
              || !name(segment).equals(value.segmentName()))
            throw new IllegalArgumentException("identity mismatch");
          return Response.noContent().tag(tag(store.updateSegment(value, version))).build();
        });
  }

  @DELETE
  @Path("tables/{type}/{name}/segments/{segment}")
  public Response deleteSegment(
      @PathParam("type") String type,
      @PathParam("name") String table,
      @PathParam("segment") String segment,
      @HeaderParam("If-Match") String match) {
    return perform(
        () -> {
          int version = ifMatch(match);
          store.deleteSegment(name(table), type(type), name(segment), version);
          return Response.noContent().build();
        });
  }
}
