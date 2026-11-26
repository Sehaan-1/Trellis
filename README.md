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

A staged, Pinot-inspired OLAP system. This repository contains a build scaffold and initial SPI/common contracts; it is not yet a working database.

## Build

Install JDK 17 or newer and run `./mvnw verify` (on Windows, `mvnw.cmd verify`). The wrapper downloads Maven 3.9.9 from Maven Central; dependencies and plugins also require Maven Central access. Run `./mvnw -pl pinot-bom -am verify` for this slice. `./mvnw -Pfast-dev verify` skips formatting and license checks, but not tests; use the default profile before merging. Apply Java formatting with `./mvnw -pl pinot-spi,pinot-common com.diffplug.spotless:spotless-maven-plugin:2.43.0:apply`.

The root is an aggregator and parent for future modules. The standalone `io.trellis:pinot-bom:0.1.0-SNAPSHOT` contains centrally managed dependency versions; future consumers must explicitly import it in `dependencyManagement`. It is not imported by the root, avoiding a reactor model cycle. The BOM lists candidate versions, not runtime dependencies or a promise of adoption; each future slice must review and verify the dependencies it uses. CI checks the reactor on Java 17.

Licensed under Apache-2.0; see [LICENSE](LICENSE).

## Initial contracts

`pinot-spi` defines an immutable schema and table identity, a mutable shallow-value row (not thread-safe), and a record decoder SPI. `pinot-common` adds Calcite query parsing and Helix `ZNRecord` table identity conversion. Parsing is syntactic, not validation or planning. Record conversion does not connect to ZooKeeper or persist metadata. Neither module depends on the query engine or cluster services.

Run `./mvnw -pl pinot-common -am verify` to test these modules and their prerequisites.
