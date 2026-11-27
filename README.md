<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Trellis

A staged, Pinot-inspired OLAP system. This repository contains a build scaffold and initial SPI/common contracts and Lucene-backed segments; it is not yet a working database.

## Build

Install JDK 17 or newer and run `./mvnw verify` (on Windows, `mvnw.cmd verify`). The wrapper downloads Maven 3.9.9 from Maven Central; dependencies and plugins also require Maven Central access. Run `./mvnw -pl pinot-bom -am verify` for this slice. `./mvnw -Pfast-dev verify` skips formatting and license checks, but not tests; use the default profile before merging. Apply Java formatting with `./mvnw -pl pinot-spi,pinot-common,pinot-segment-spi,pinot-segment-local,pinot-core,pinot-controller,pinot-deepstore-local com.diffplug.spotless:spotless-maven-plugin:2.43.0:apply`.

The root is an aggregator and parent for future modules. The standalone `io.trellis:pinot-bom:0.1.0-SNAPSHOT` contains centrally managed dependency versions; future consumers must explicitly import it in `dependencyManagement`. It is not imported by the root, avoiding a reactor model cycle. The BOM lists candidate versions, not runtime dependencies or a promise of adoption; each future slice must review and verify the dependencies it uses. CI checks the reactor on Java 17.

Licensed under Apache-2.0; see [LICENSE](LICENSE).

## Initial contracts

`pinot-spi` defines an immutable schema and table identity, a mutable shallow-value row (not thread-safe), and a record decoder SPI. `pinot-common` adds Calcite query parsing and Helix `ZNRecord` table identity conversion. Parsing is syntactic, not validation or planning. Record conversion does not connect to ZooKeeper or persist metadata. Neither module depends on the query engine or cluster services.

Run `./mvnw -pl pinot-common -am verify` to test these modules and their prerequisites.

## Immutable segments

`pinot-segment-spi` exposes read-only forward and string dictionary access. `pinot-segment-local` writes Lucene doc values and reopens committed segments through `MMapDirectory`. This is a Trellis-specific format, not Pinot-compatible. It currently accepts only non-null, single-valued STRING and LONG fields; dictionary ordering is Lucene UTF-8 byte order. Each write requires a new destination directory. A failed write may leave an uncommitted directory for the caller to remove. Reads may run concurrently; closing must not race with reads.

Run `./mvnw -pl pinot-segment-local -am verify` for the affected modules.

## Rebuildable secondary indexes

`RoaringIndexes` builds in-memory equality and inclusive long-range indexes from immutable segments. `LuceneIndexes` builds in-memory analyzed text and Euclidean HNSW vector indexes. Text queries require all standard-analyzer tokens; vector document IDs are input-list positions. Search results are limited and score-ordered, and vector results are approximate. These indexes are not persisted; callers must close Lucene indexes and rebuild them after restart. Close must not race with queries. Bitmap results are sorted ID snapshots.

## Mutable segments

`InMemorySegments.create(schema)` creates an append-only, volatile Lucene-backed segment using the same field restrictions as the immutable format. Appends and snapshot creation are serialized and assign stable document IDs. A snapshot is point-in-time and remains valid after the mutable segment is closed; callers must close each snapshot. Rows must not be mutated concurrently with an append. This is not durable ingestion, upsert, stream processing or a controller-coordinated commit.

## Local SQL execution

`pinot-core` provides `LocalQueryExecutor` for one logical table backed by one or more compatible immutable segment readers. Calcite validates, plans and executes SQL locally; the caller owns and must keep the readers open. Identifiers are case-sensitive. Results are materialized with a mandatory output-row cap that rejects overflow, but this cap does not bound intermediate memory or CPU. This API is not a network server, security boundary or distributed query engine; use trusted queries only.

Run `./mvnw -pl pinot-core -am verify` for the affected reactor.

## ZooKeeper metadata

`ZooKeeperMetadataStore` in `pinot-common` stores schema and table metadata as versioned Helix records through a caller-owned Curator client. Configure authentication and ACLs on that client; Curator defaults are not a secure deployment configuration. Create, read, update and delete expose ZooKeeper errors and require explicit nonnegative versions for mutations. Versions reset after a znode is deleted and recreated, so they do not prevent stale updates across that ABA transition; clients must coordinate deletion/recreation. Network failures can leave write outcomes uncertain; reconcile state before retrying. Reads have ZooKeeper's normal consistency semantics and can lag other clients; version checks happen at the leader on mutation. Operations on different records are not transactional. The store does not enforce schema/table referential integrity or perform Helix cluster coordination.

Run `./mvnw -pl pinot-common -am verify` for these contracts and embedded ZooKeeper integration tests.

## Controller metadata HTTP API (initial slice)

`MetadataHttpServer` exposes schema CRUD at `/schemas` and `/schemas/{name}`, and table CRUD at `/tables` and `/tables/{OFFLINE|REALTIME}/{name}`. POST creates, GET returns an ETag, and PUT/DELETE require that strong numeric ETag in `If-Match`. Names are 1–128 ASCII letters, digits, underscores, hyphens or dots and must start with a letter, digit or underscore. Body fields and table types use the SPI enum names. There is no schema/table referential-integrity check.

**Security:** this service has no authentication, authorization or TLS. Bind it to a trusted interface and place it behind an authenticated TLS proxy before use; configure ZooKeeper authentication and ACLs separately. ETags are ZooKeeper versions and do **not** prevent stale updates after a delete/recreate (ABA); coordinate name reuse. A failed network write may have an uncertain outcome. This is not yet a Helix cluster controller and does not upload segments.

Run `./mvnw -pl pinot-controller -am verify` for controller and prerequisite tests.

## Helix controller lifecycle

`HelixController.start` connects a fresh caller-configured Helix CONTROLLER manager to a previously provisioned cluster and owns its connection until close. Helix handles leader election and failover. `isLeader()` and `isConnected()` are observations, not fencing tokens; do not use a prior observation to authorize later writes. Cluster provisioning and ZooKeeper authentication/ACL configuration remain the caller’s responsibility. The metadata HTTP API is independent and is **not** restricted to the Helix leader. Resource assignment, segment upload and a runnable integrated controller are not yet implemented.

## Local content-addressed deep store

`BlobStore` and `FileBlobStore` store opaque blobs keyed by SHA-256. Writes stream to a staged file, enforce a caller-provided byte limit, force file contents and publish with a no-overwrite hard link; concurrent identical writes deduplicate. Reads verify the digest before returning, but a destination may already contain corrupt or partial bytes on failure and must be discarded. Callers own the input/output streams.

The local store requires an existing trusted directory and a filesystem that supports hard links and `NOFOLLOW_LINKS`. It does not fsync the directory or promise survival of power loss; crashes can leave staged files, and a failure after publication may have an uncertain outcome. It assumes no out-of-band modification of the store and makes the usual SHA-256 collision-resistance assumption. It is not yet a segment validator, upload endpoint or S3/GCS implementation.

Run `./mvnw -pl pinot-deepstore-local -am verify` for the storage slice.

## Bounded segment-archive extraction

`SegmentArchives.extract` extracts a flat ZIP into a new directory under caller-supplied compressed-size, entry-count, per-file and total expanded-size limits. It rejects paths, directory entries and case-insensitive duplicate names, and checks each entry's declared size and CRC-32. It uses ZIP central-directory entries; the archive and target parent must remain trusted and stable during extraction. The target is visible while extraction runs and must not be consumed until success. Failed extraction attempts cleanup but cleanup can itself fail.

This is only an extraction primitive. It does not validate Lucene checksums or segment metadata, publish a segment, provide an upload endpoint, or guarantee power-loss durability. Do not treat successful extraction as a verified segment.

## Local segment integrity validation

`SegmentVerifier.verify` checks an existing Trellis segment against a caller-supplied schema and maximum document count. It reuses Apache Lucene `org.apache.lucene:lucene-core:9.12.0` `CheckIndex` with slow checks, validates the Trellis metadata, disallows deleted documents and checks forward-value presence and valid UTF-8 across every leaf. It acquires Lucene's writer lock and requires a trusted, stable local directory with no out-of-band changes. It may use substantial CPU and memory: the document cap is not a byte, time or memory limit, and this is not a sandbox for hostile inputs. It does not extract archives, authenticate data, publish files or implement upload.

## Local archive preparation

`SegmentPreparation.prepare` composes bounded archive extraction with local segment integrity validation against the expected schema and document limit. The target must not exist and is visible during preparation; consume it only after success. Validation failure triggers best-effort cleanup, and cleanup failures are suppressed on the original error. The archive and filesystem must remain trusted and stable. This is not atomic publication, upload, authentication or a resource sandbox; the extraction and verification limitations above still apply.

## Local segment archive writing

`SegmentArchiveWriter.write` verifies a trusted, stable local segment against an expected schema and document limit, then streams a flat ZIP under caller-supplied archive, file-count, per-file and total expanded-byte limits. It excludes Lucene's transient `write.lock` and rejects non-regular files and names incompatible with `SegmentArchives`. The caller owns the output stream and must discard all output on failure. Compression and verification CPU and memory are not bounded by these limits. This is not authentication, publication or an upload endpoint.

## Segment blob transfer

`SegmentBlobs.put` stages a bounded, verified segment archive before storing it through `BlobStore`; `SegmentBlobs.prepare` copies a blob into bounded staging, then extracts and verifies it. The existing staging directory, segment, target parent and filesystem must be trusted and stable. The blob store must honor its checksum-verification contract. The target is visible during preparation and must not be consumed before success. Staging files are removed on completion or failure, but cleanup may fail; a cleanup failure after preparation means the target may remain even though the call failed, and a failure after a put may leave a published blob. Crashes may leave staging files. ZIP bytes are not canonical, so retries need not produce the same blob key. This is not controller metadata publication, atomic serving, upload, or an untrusted-input sandbox.

## Segment descriptor records

`SegmentDescriptor` is an immutable claim identifying a table and segment, a complete ordered schema snapshot, a SHA-256 blob key and size, and a document count. `SegmentDescriptorRecord` converts it to and from a versioned Helix `ZNRecord`. A descriptor alone does not prove that the blob exists, matches the claimed size or schema, or has been validated; consumers must perform those checks. This is a metadata contract only: it does not persist records, coordinate assignments, publish segments or enforce table/schema referential integrity.

## ZooKeeper segment descriptors

`ZooKeeperMetadataStore` supports create, read, version-checked update and delete of segment descriptors, scoped by table name, table type and segment name. Reads reject malformed records and mismatched identities. These operations store claims only: they do not check table or schema existence, validate or delete blobs, provide segment assignment or fence leadership. ZooKeeper data versions reset on delete/recreate and are not globally unique generations; clients must coordinate name reuse. Network failures can leave mutation outcomes uncertain, and reads have ZooKeeper's normal consistency semantics. The caller owns the Curator client and must configure authentication and ACLs.


`listSegmentNames` returns a sorted, immutable name snapshot for one table and type; a missing parent returns an empty list, and a caller-supplied maximum rejects larger results. ZooKeeper still materializes the full child response before the limit is checked. The listing is not paginated and is not a consistent snapshot of descriptor contents; names may disappear or change before a subsequent read. Invalid or noncanonical child names cause an error.

## Local segment registration

`SegmentRegistrar.register` verifies a trusted, stable local segment against the expected schema and limits, stores a bounded archive through `BlobStore`, then creates its descriptor through `ZooKeeperMetadataStore`. It verifies the source twice and requires it to remain unchanged throughout. Blob storage and ZooKeeper are not transactional: failures, including duplicate segment names, can leave orphaned blobs or an uncertain metadata outcome. Reconcile before retrying; archives are not canonical, so retries may produce different keys. The caller manages the blob store, Curator client, staging directory, authentication and ACLs. This is not an HTTP upload endpoint, table/schema referential-integrity check, assignment service, resource sandbox or leader-fenced operation.

Run `./mvnw -pl pinot-controller -am verify` for this slice and `./mvnw verify` for the full reactor.

`SegmentBlobs.prepare` also accepts a `SegmentDescriptor`: in addition to the normal checksum and schema validation, this overload rejects archives whose copied byte count or verified document count differs from the descriptor. It does not check table identity or prove that the descriptor is authorized. The same staging, cleanup and trusted-filesystem limitations apply.

## Initial Helix segment resource

`HelixSegmentResource.create` creates a fixed initial full-auto Helix resource for a supplied nonempty segment list and replica count, using the cluster's pre-provisioned `OnlineOffline` state model. Table and segment identities are encoded as canonical URL-safe names; logical names must be nonblank valid Unicode of at most 128 UTF-8 bytes. The caller supplies a segment-count cap, owns the Helix admin, configures ZooKeeper security and coordinates creation. Helix chooses placement when a controller and eligible participants are running. This primitive does not verify metadata or blobs, reconcile subsequent segment changes, implement a participant, ensure replicas are available, or provide leader fencing or cross-store transactions. Creation failures may have uncertain outcomes; inspect Helix before retrying.

## Bootstrap a resource from metadata

`SegmentResourceBootstrap.create` reads a table and its current schema, lists its segment descriptors under a caller-supplied cap, checks that each descriptor's schema snapshot matches, and creates the initial Helix resource. The caller must prevent metadata changes throughout the operation and account for ZooKeeper read consistency: descriptor reads and the Helix write are not transactional or fenced. The operation does not verify blobs or reconcile subsequent changes. On failure, resource creation may have an uncertain outcome; inspect Helix before retrying. The metadata and Helix clients remain caller-owned and must point to the intended secured services.

`HelixSegmentResource.replaceSegments` performs an explicit expected-set replacement on an existing, non-bucketized full-auto `OnlineOffline` resource using Helix's version-retrying updater. Retained partition configuration is preserved and Helix remains responsible for placement. Both expected and desired sets must be nonempty and within the caller's limit. This is not an automatic metadata reconciler, a transaction with the segment catalog, or leadership fencing; expected-set comparisons cannot prevent ABA or resource deletion/recreation. Coordinate writers and reconcile uncertain failures before retrying.

`SegmentResourceSync.apply` performs a one-shot update of an existing resource from registered segment names, checking each descriptor's schema against the table's schema and using an expected current Helix segment set. Both sets must be nonempty. The caller must keep metadata and the expected collection stable and coordinate concurrent writers; the metadata reads and Helix update are not transactional, may observe stale data, and do not fence leadership or prevent ABA. It neither verifies blobs nor continuously reconciles. Inspect metadata and Helix after an uncertain failure before retrying.

`SegmentHelixIdentity` in `pinot-common` exposes the shared resource and partition naming contract, including decoding a resource to its logical table identity. It rejects noncanonical encodings and keeps the controller's existing wire names unchanged. Decoded identities are not authorization or proof that metadata exists.

## Controller lifecycle wrapper

`ControllerRuntime.start` starts the Helix controller and metadata HTTP server together; close stops HTTP and disconnects Helix. Startup failure closes the Helix controller, while the caller retains ownership of the metadata store and Curator client. The HTTP API remains **unauthenticated and not leader-gated**; use a trusted bind interface and authenticated TLS proxy, and configure ZooKeeper authentication and ACLs. The basic overload does not enable uploads. Neither overload provides a standalone executable, a reconciliation loop, or leader fencing.

## Segment descriptor HTTP API

The metadata server exposes descriptor POST and bounded name listing at `/tables/{OFFLINE|REALTIME}/{name}/segments`, and GET/PUT/DELETE at the same path plus `/{segment}`. Listing requires `?max=1` through `?max=10000`; it returns 422 when more names exist. ZooKeeper still materializes the full child list before the cap is checked. PUT/DELETE require the strong numeric `If-Match` ETag; versions have the existing delete/recreate ABA limitation. Names in HTTP requests follow the existing ASCII name policy. These endpoints store unverified metadata claims only: they do not upload, authenticate, validate or delete blobs, check table/schema existence, or require controller leadership. Deploy only behind an authenticated TLS boundary and configure ZooKeeper security.

## Local archive registration

`SegmentArchiveRegistrar.register` extracts an existing ZIP into a new staging directory under caller-supplied limits, validates the segment against the expected schema and document limit, removes staging, stores the original ZIP through `BlobStore`, and creates its ZooKeeper descriptor. The archive, staging directory and filesystem must be trusted and stable throughout. Blob and metadata writes are not transactional: failures can leave an orphaned blob or uncertain metadata outcome; reconcile before retrying. Cleanup can fail and crashes can leave staging files. This does not accept HTTP uploads, authenticate, check table/schema existence, assign segments or fence leadership.

## Opt-in segment archive upload

Construct `SegmentArchiveUpload` with the metadata store, blob store, trusted staging directory, archive limits and document limit; pass it to the upload-enabled `MetadataHttpServer.start` or `ControllerRuntime.start` overload. POST a ZIP as `application/octet-stream` to `/tables/{OFFLINE|REALTIME}/{name}/segments/{segment}/content`. The service looks up the table and schema, stages the request with a compressed-byte cap, validates the ZIP and segment, stores the original blob and creates its descriptor. Success returns 201, a descriptor location and ETag; compressed input above the cap returns 413. Other archive/storage failures may return 500 with an uncertain outcome.

**Security and consistency:** upload is opt-in but unauthenticated, not leader-gated and not a sandbox for hostile archives. Use only behind an authenticated TLS boundary with external request timeouts, concurrency, CPU, memory and disk controls. Keep table/schema metadata and staging files stable during the call; reads and writes are not transactional. Failures can leave orphan blobs, uncertain metadata, or temporary files; reconcile before retrying. No assignment or reconciliation is performed automatically.

## Opt-in Helix resource operations over HTTP

Construct `SegmentResourceOperations` with caller-owned metadata, Helix admin and data accessor for the intended cluster, replicas and segment cap. Pass it to the four-argument `MetadataHttpServer.start` or five-argument `ControllerRuntime.start` overload; the upload option may independently be null. GET `/tables/{OFFLINE|REALTIME}/{name}/assignment` returns Helix **desired** resource state, not observed online replicas. POST creates the initial resource from schema-checked metadata. PUT with `{"expectedSegments":["segmentName"]}` performs one-shot synchronization against that expected set. Missing or unreadable state and expected-set conflicts can both surface as 500; inspect and reconcile before retrying. Request names follow the existing ASCII HTTP policy.

These endpoints are disabled by default and are **unauthenticated and not leader-fenced**. Expose them only through an authenticated, authorized TLS boundary with request-size, time and concurrency controls. The caller must secure and coordinate all clients and keep metadata stable; the operations are not transactional with metadata or blobs, do not check blobs or guarantee placement, and cannot prevent ABA. Helix and metadata resources remain caller-owned, and their closure must not race with active requests.

GET `/tables/{OFFLINE|REALTIME}/{name}/assignment/observed?maxInstances=N` (1–10000) reads the Helix external view and returns observed segments with instance IDs in exact `ONLINE` state. This view can be stale or incomplete: missing partitions are not proof of absence, and `ONLINE` does not prove reachability, healthy data or query readiness. Helix materializes the complete view before the configured segment cap and per-segment instance cap are checked. Oversized or unreadable views can return 500. The opt-in endpoint shares the unauthenticated, not leader-fenced security limitations above and can reveal internal instance names.

`ControllerRuntime.startIntegrated` wires the metadata API, upload and Helix resource-operation endpoints to one fresh controller manager, a caller-owned Helix admin, metadata store and blob store. `IntegratedConfig` supplies bind address, trusted staging path and limits. The admin and manager must address the same pre-provisioned cluster; callers own and secure all other clients, coordinate writes and must not close the runtime or dependencies concurrently with requests. HTTP remains **unauthenticated and not leader-fenced**; use the external security and resource controls above. This does not provide a standalone executable, automatic reconciliation, cross-store transactions or a server participant.
