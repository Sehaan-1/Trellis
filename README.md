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

Install JDK 17 or newer and run `./mvnw verify` (on Windows, `mvnw.cmd verify`). The wrapper downloads Maven 3.9.9 from Maven Central; dependencies and plugins also require Maven Central access. Run `./mvnw -pl pinot-bom -am verify` for this slice. `./mvnw -Pfast-dev verify` skips formatting and license checks, but not tests; use the default profile before merging. Apply Java formatting with `./mvnw -pl pinot-spi,pinot-common,pinot-segment-spi,pinot-segment-local,pinot-core,pinot-controller com.diffplug.spotless:spotless-maven-plugin:2.43.0:apply`.

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
