# v1 Issue Map

This is the current v1 blocker map. It tracks the fresh 40-item audit from
2026-05-22 and intentionally does not use a blanket "Done" status for work that
still needs release evidence.

## Status Values

- `Open`: implementation, evidence, or release validation is still missing.
- `In Progress`: code or docs exist, but acceptance evidence is incomplete.
- `Verified`: the current tree has code/docs/tests or release-gate evidence for
  the blocker.
- `Not Applicable`: the blocker was deliberately removed from the v1 contract.

## Execution Order

1. Public claims, package shape, generated docs, and API freeze.
2. CLI, migrations, and destructive-operation safety.
3. LINQ, client-eval, bulk, raw SQL, and transaction semantics.
4. Provider, multi-tenant, temporal, AOT, cache, exception, logging, and
   interceptor contracts.
5. Benchmark evidence, executable performance thresholds, test hygiene, and RC
   release artifacts.

## Blocker Map

| ID | Area | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Remove or prove remaining public marketing claims | Verified | README uses bounded performance and migration language; documentation tests reject unqualified claims. |
| 2 | Fix stale generated API docs | Verified | Removed stale `ConnectionPool` API pages and index entries; documentation tests reject generated docs for missing public types. |
| 3 | Decide whether `ConnectionPool` is removed or restored | Verified | Public docs now direct users to provider-native pooling and `ConnectionManager`; no public `ConnectionPool` examples remain. |
| 4 | Fix v1 issue map and blocker accounting | Verified | This map tracks exactly 40 blockers with evidence-aware statuses. |
| 5 | Make Release build warning-free | Verified | `dotnet build nORM.sln -c Release --nologo` reports 0 warnings on this tree. |
| 6 | Harden public API snapshotting before freeze | In Progress | `tests/PublicApi.Shipped.txt` exists; full supportability classification remains open. |
| 7 | Revisit runtime package dependency architecture | Verified | Decision documented in `docs/provider-packages.md`: single runtime package with SQL Server/SQLite bundled; Postgres/MySQL via reflection to avoid transitive dependencies. Architecture decided and documented. Package-consumer tests validate the dependency contract. |
| 8 | Clean release artifact and version hygiene | Verified | `NormVersion` is centralized in `Directory.Build.props`; package tests and `eng/v1-release-gate.ps1` clean and validate package outputs. |
| 9 | Run package consumer tests cross-platform | In Progress | Package consumer tests exist; cross-platform CI evidence is still required. |
| 10 | Replace or freeze beta CLI dependency | Verified | `dotnet-norm` now references stable `System.CommandLine` 2.0.8; CLI and package consumer smoke tests passed. |
| 11 | Isolate CLI design-time assembly loading | Verified | CLI migration assembly loading uses `AssemblyLoadContext` plus `AssemblyDependencyResolver`; dependency-backed design-time factory test passed. |
| 12 | Harden destructive database drop behavior | In Progress | `--yes` and `--dry-run` exist; live provider safety contracts remain open. |
| 13 | Make migration rename/data-loss handling first-class | In Progress | Destructive-operation warnings exist; first-class rename workflow remains open. |
| 14 | Prove migration recovery and idempotency across providers | Open | Requires fault-injected live-provider evidence. |
| 15 | Complete migration SQL live parity | Open | Requires generated DDL execution across supported live providers. |
| 16 | Generate LINQ support matrix from tests | Verified | `docs/linq-support-coverage.md` maps non-unsupported matrix rows to test files and documentation contracts enforce coverage entries. |
| 17 | Resolve `Any` and `All` semantics across providers | In Progress | Direct SQLite cardinality tests now exercise `Any`, `Any(predicate)`, and `All`; live provider parity still required. |
| 18 | Stabilize Include and lazy-loading contracts | In Progress | Relationship docs exist; unsupported paths and exception taxonomy need cleanup. |
| 19 | Prove terminal operator parity in every execution path | In Progress | Sync/async SQLite cardinality coverage now includes terminal operators; compiled/live-provider parity remains open. |
| 20 | Decide the v1 default for client evaluation | Verified | The v1 default is `ClientEvaluationPolicy.Throw`; `Warn` and `Allow` are explicit opt-ins with tests and docs. |
| 21 | Remove legacy string-based bulk CUD paths | Verified | Bulk CUD now validates `BulkCudQueryShape` only; `ValidateCudPlan(string)` and `ExtractWhereClause(string, ...)` are removed and documentation tests lock that boundary. |
| 22 | Define bulk update value-expression support | Verified | `ExecuteUpdateAsync` v1 assignment values are documented as literal constants or precomputed captured locals only; unsupported computed/server expressions throw actionable `NormUnsupportedFeatureException`s. |
| 23 | Replace raw SQL safety heuristics with provider-aware validation | Verified | Raw query APIs now call `NormValidator.ValidateRawQuerySql`, which combines provider-aware SELECT/CTE gating with shared injection checks; `docs/raw-sql-security.md` documents read-only raw query and privileged-path boundaries. |
| 24 | Tighten stored procedure security and tenant boundaries | Verified | Stored procedure APIs validate provider command text/name shape, SQLite text mode uses the read-only raw query gate, and docs show tenant-parameter patterns plus privileged-path review rules. |
| 25 | Finish transaction and sync/async policy hardening | In Progress | Transaction and sync docs are contract-tested; raw SQL and SQLite text-mode stored procedure paths bind to active transactions. Live provider and interceptor parity remain open. |
| 26 | Prove `ConnectionManager` failover behavior under load | In Progress | Write failover now skips unhealthy primaries; tests cover deterministic secondary failover, read-replica fallback, circuit breaker, and concurrent read/dispose races. Health-check churn stress remains open. |
| 27 | Treat multi-tenancy as a verified security boundary | In Progress | Adversarial tests exist and docs now include a threat-model boundary inventory with bypass-capable APIs. Full live-provider security gate remains open. |
| 28 | Decide temporal/versioning stability | Verified | Temporal versioning is explicitly stable for nORM-managed history tables/triggers; docs define schema ownership, rollback responsibilities, and the RC live-provider evidence requirement. |
| 29 | Enforce provider version support at startup | Verified | Provider initialization validates the actual opened connection against `Capabilities.MinimumServerVersion`; unsupported versions throw `NormConfigurationException`, and provider docs/tests lock the startup contract. |
| 30 | Finish MySQL optimistic concurrency guarantees | Verified | MySQL affected-row OCC is refused by default through `RequireMatchedRowOccSemantics=true`; matched-row mode and explicit weakened opt-in are documented and covered by regression tests. |
| 31 | Prove AOT and trimming claims with real publish tests | Verified | AOT/trimming is explicitly unsupported for v1; annotation tests and a negative `PublishTrimmed=true` smoke test lock the current boundary with real publish diagnostics. |
| 32 | Harden source generator limitations | Verified | Source-generation docs now define the v1 materializer support contract and diagnostics; package-consumer tests verify unsupported mapped properties report `nORMSG005`. |
| 33 | Stress cache and plan memory bounds as release gates | Verified | RC gate now has a dedicated cache memory bounds step; cache policy names the release evidence, and stress tests assert bounded counts/evictions for LRU, bounded FIFO, and compiled materializer caches. |
| 34 | Normalize public exception taxonomy | Verified | Public unsupported query paths for bulk CUD, async streaming Include/GroupJoin, and composite-key includes now throw `NormUnsupportedFeatureException`; docs/tests lock the taxonomy. |
| 35 | Complete logging and redaction coverage | Verified | `LogQuery`, CLI connection-string validation, and the built-in command interceptor now have tests proving SQL literals, parameter values, scalar results, and connection-string secrets are not emitted by default; docs cover benchmark/release artifact boundaries. |
| 36 | Freeze interceptor semantics | Verified | Command interceptor ordering, mutation visibility, suppression notifications, and failure propagation now have contract tests; docs define the suppression and mutation rules. |
| 37 | Rebuild benchmark evidence before any performance launch | Verified | Release gate now generates a benchmark evidence manifest from raw BenchmarkDotNet reports, including commit, SDK/OS, driver packages, redacted provider configuration, report paths, and fastest methods by provider. |
| 38 | Make benchmark thresholds executable | Verified | `eng/benchmark-thresholds.json` defines versioned latency/allocation budgets, `eng/check-benchmark-thresholds.ps1` enforces them from raw BenchmarkDotNet CSV reports, and the release gate runs the checker after benchmark evidence. |
| 39 | Reduce test-suite entropy before v1 | Verified | The stale `CS1998` test-project suppression is removed, ignored local TRX artifacts are cleaned, `.gitignore` covers TRX/coverage outputs, `docs/test-suite-ownership.md` contains the legacy coverage-file policy, and `RepositoryHygieneTests` locks the hygiene rules. |
| 40 | Run and publish a real RC release gate | In Progress | The release gate now writes an auditable RC artifact manifest and CI uploads it with TRX/package/benchmark artifacts; the remaining release action is the full `eng/v1-release-gate.ps1 -Mode rc -MinLiveProviders 3` run from the candidate commit. |

## Closure Rule

A blocker is not considered `Verified` unless at least one of the following is
true:

- an automated test covers the behavior,
- a public doc defines the support contract,
- a release gate or CI workflow enforces it,
- the item is explicitly marked `Not Applicable` with rationale.

Benchmark-related blockers require code-level benchmark categories, public-claim
constraints, and release artifacts before they can be used for launch claims.
