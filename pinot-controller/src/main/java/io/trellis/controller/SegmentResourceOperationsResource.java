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

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.util.List;
import java.util.Objects;

/** Opt-in Helix resource operations; not an authentication or fencing boundary. */
@Path("tables/{type}/{name}/assignment")
@Produces(MediaType.APPLICATION_JSON)
public final class SegmentResourceOperationsResource {
  public record SyncBody(List<String> expectedSegments) {}

  private final SegmentResourceOperations operations;

  @Inject
  public SegmentResourceOperationsResource(SegmentResourceOperations operations) {
    this.operations = Objects.requireNonNull(operations, "operations");
  }

  @GET
  public Response snapshot(@PathParam("type") String type, @PathParam("name") String table) {
    return MetadataResource.perform(
        () ->
            Response.ok(
                    operations.snapshot(MetadataResource.name(table), MetadataResource.type(type)))
                .build());
  }

  @GET
  @Path("observed")
  public Response observed(
      @PathParam("type") String type,
      @PathParam("name") String table,
      @QueryParam("maxInstances") String maxInstances) {
    return MetadataResource.perform(
        () ->
            Response.ok(
                    operations.observed(
                        MetadataResource.name(table),
                        MetadataResource.type(type),
                        MetadataResource.segmentLimit(maxInstances)))
                .build());
  }

  @POST
  public Response bootstrap(
      @PathParam("type") String type, @PathParam("name") String table, @Context UriInfo uri) {
    return MetadataResource.perform(
        () -> {
          var result =
              operations.bootstrap(MetadataResource.name(table), MetadataResource.type(type));
          return Response.created(uri.getAbsolutePath()).entity(result).build();
        });
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public Response sync(
      @PathParam("type") String type, @PathParam("name") String table, SyncBody body) {
    return MetadataResource.perform(
        () -> {
          if (body == null || body.expectedSegments() == null)
            throw new IllegalArgumentException("expected segments required");
          var expected = body.expectedSegments().stream().map(MetadataResource::name).toList();
          return Response.ok(
                  operations.sync(
                      MetadataResource.name(table), MetadataResource.type(type), expected))
              .build();
        });
  }
}
