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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.util.Objects;

/** Opt-in streaming upload; not an authentication or authorization boundary. */
@Path("tables/{type}/{name}/segments/{segment}/content")
@Consumes(MediaType.APPLICATION_OCTET_STREAM)
@Produces(MediaType.APPLICATION_JSON)
public final class SegmentUploadResource {
  private final SegmentArchiveUpload upload;

  @Inject
  public SegmentUploadResource(SegmentArchiveUpload upload) {
    this.upload = Objects.requireNonNull(upload, "upload");
  }

  @POST
  public Response post(
      @PathParam("type") String type,
      @PathParam("name") String table,
      @PathParam("segment") String segment,
      InputStream input,
      @Context UriInfo uri) {
    return MetadataResource.perform(
        () -> {
          var result =
              upload.upload(
                  MetadataResource.name(table),
                  MetadataResource.type(type),
                  MetadataResource.name(segment),
                  input);
          return Response.created(
                  uri.getBaseUriBuilder()
                      .path("tables")
                      .path(type)
                      .path(table)
                      .path("segments")
                      .path(segment)
                      .build())
              .tag(MetadataResource.tag(result.version()))
              .build();
        });
  }
}
