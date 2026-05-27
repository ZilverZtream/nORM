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
| 1 | Remove or prove remaining public marketing claims | Verified | README uses bounded performance and migration language; documentation tests reject unqualified claims; "Production-Ready Features" heading changed to "v1 Features"; "Full support for major database engines" links to provider-capabilities.md; "Smart Relationship Handling" links to linq-support.md constraints; benchmark section references benchmarks/ artifacts only. |
| 2 | Fix stale generated API docs | Verified | Removed stale `ConnectionPool` API pages and index entries; documentation tests reject generated docs for missing public types; `docs/api/README.md` documents the `docfx metadata docfx.json` regeneration command and the RC validation requirement. |
| 3 | Decide whether `ConnectionPool` is removed or restored | Verified | Public docs now direct users to provider-native pooling and `ConnectionManager`; no public `ConnectionPool` examples remain. |
| 4 | Fix v1 issue map and blocker accounting | Verified | This map tracks exactly 40 blockers with evidence-aware statuses. |
| 5 | Make Release build warning-free | Verified | `dotnet build nORM.sln -c Release --nologo` reports 0 warnings on this tree. |
| 6 | Harden public API snapshotting before freeze | Verified | `tests/PublicApi.Shipped.txt` baseline exists and is enforced by `PublicApiSnapshotTests`; `PublicApiClassificationTests` verifies every public namespace is assigned exactly one of StableUser / StableProvider / StableTooling / Unsupported, with no uncategorized entries and no zombie classifications. |
| 7 | Revisit runtime package dependency architecture | Verified | v1 decision documented in `docs/provider-packages.md`: single package, SQL Server and SQLite bundled, PostgreSQL and MySQL reflection-loaded (no transitive driver dependency). `PackageDependencyArchitectureTests` enforces the contract against `nORM.csproj` (SqlClient/Sqlite present, Npgsql/MySqlConnector absent). `PackageConsumerIntegrationTests` validates the same in the produced nuspec. |
| 8 | Clean release artifact and version hygiene | Verified | `NormVersion` is centralized in `Directory.Build.props`; package tests and `eng/v1-release-gate.ps1` clean and validate package outputs. |
| 9 | Run package consumer tests cross-platform | Verified | CI workflow runs on Ubuntu, Windows, and macOS (added macOS leg). Package consumer tests now use forward-slash paths (`src/nORM.csproj`) so `dotnet pack` invocations work on all three platforms without modification. The `test-mysql`, `test-postgres`, and `test-sqlserver` CI jobs provide live-provider cross-platform evidence on Ubuntu via GitHub Actions service containers. |
| 10 | Replace or freeze beta CLI dependency | Verified | `dotnet-norm` now references stable `System.CommandLine` 2.0.8; CLI and package consumer smoke tests passed. |
| 11 | Isolate CLI design-time assembly loading | Verified | CLI migration assembly loading uses `AssemblyLoadContext` plus `AssemblyDependencyResolver`; dependency-backed design-time factory test passed. |
| 12 | Harden destructive database drop behavior | Verified | `--yes` and `--dry-run` exist; `LiveProviderDatabaseDropSafetyTests` proves: (a) `connection.Database` returns the expected protected name on SQL Server/PostgreSQL/MySQL so the drop refusal fires; (b) `GetSchema("Tables")` + `IsSystemSchema` filter correctly excludes system-schema rows on all 3 server providers; (c) SQLite user-table lifecycle (create → enumerate via `sqlite_master` → drop → re-enumerate) works correctly; (d) CLI gate logic (refuse without `--yes`/`--dry-run`) is contract-tested. |
| 13 | Make migration rename/data-loss handling first-class | Verified | `[RenameColumn("OldName")]` workflow generates provider-correct RENAME COLUMN SQL for all 4 providers; `LiveProviderMigrationDdlParityTests` proves UP (old name gone, new name present, data preserved) and DOWN reversal against live SQLite, SQL Server, MySQL, and PostgreSQL. |
| 14 | Prove migration recovery and idempotency across providers | Verified | `LiveProviderMigrationDdlParityTests` fault-injects a failing migration across all four providers and asserts: bad migration history absent, good prior migration history present (per-migration runners) or full rollback (SQLite batch runner). Re-apply after failure also verified. |
| 15 | Complete migration SQL live parity | Verified | `LiveProviderMigrationDdlParityTests` executes ADD COLUMN (nullable + NOT NULL+DEFAULT), DROP COLUMN, CREATE TABLE, DROP TABLE, and DOWN reversal against SQLite, SQL Server, MySQL, and PostgreSQL live connections, introspecting schema after each step. |
| 16 | Generate LINQ support matrix from tests | Verified | `docs/linq-support-coverage.md` maps non-unsupported matrix rows to test files and documentation contracts enforce coverage entries. |
| 17 | Resolve `Any` and `All` semantics across providers | Verified | `LiveProviderTerminalOpParityTests` proves `Any(predicate)` and `All(predicate)` on all 4 live providers: Any returns true when a match exists, false when none does; All returns true only when every row satisfies the predicate. |
| 18 | Stabilize Include and lazy-loading contracts | Verified | Many-to-many Include with keyless entities now throws `NormConfigurationException` with an actionable message at first query time (previously silently ignored). `IncludeProcessorCoverageTests` Group 7 covers left-keyless and right-keyless cases, both async and sync paths. Composite-PK Include throws `NormUnsupportedFeatureException` (covered by Group 3). |
| 19 | Prove terminal operator parity in every execution path | Verified | `LiveProviderTerminalOpParityTests` proves First, FirstOrDefault, Last, LastOrDefault, ElementAt, Count, LongCount, Any(predicate), All, Single, SingleOrDefault on all 4 live providers. `LiveProviderCompiledQueryParityTests` proves `Norm.CompileQuery` with OrderBy, Take, Skip, Where, and string-parameter shapes on all 4 live providers. |
| 20 | Decide the v1 default for client evaluation | Verified | The v1 default is `ClientEvaluationPolicy.Throw`; `Warn` and `Allow` are explicit opt-ins with tests and docs. |
| 21 | Remove legacy string-based bulk CUD paths | Verified | Bulk CUD now validates `BulkCudQueryShape` only; `ValidateCudPlan(string)` and `ExtractWhereClause(string, ...)` are removed and documentation tests lock that boundary. |
| 22 | Define bulk update value-expression support | Verified | `ExecuteUpdateAsync` v1 assignment values are documented as literal constants or precomputed captured locals only; unsupported computed/server expressions throw actionable `NormUnsupportedFeatureException`s. |
| 23 | Replace raw SQL safety heuristics with provider-aware validation | Verified | Raw query APIs now call `NormValidator.ValidateRawQuerySql`, which combines provider-aware SELECT/CTE gating with shared injection checks; `docs/raw-sql-security.md` documents read-only raw query and privileged-path boundaries. |
| 24 | Tighten stored procedure security and tenant boundaries | Verified | Stored procedure APIs validate provider command text/name shape, SQLite text mode uses the read-only raw query gate, and docs show tenant-parameter patterns plus privileged-path review rules. |
| 25 | Finish transaction and sync/async policy hardening | Verified | Transaction and sync docs are contract-tested; raw SQL and SQLite text-mode stored procedure paths bind to active transactions. `LiveProviderTransactionInterceptorParityTests` proves commit/rollback/double-begin/SaveChanges participation and interceptor behavior (Transaction visible on command, suppressed command preserves transaction) across all four providers. |
| 26 | Prove `ConnectionManager` failover behavior under load | Verified | `ConnectionManagerTests` covers deterministic failover, read-replica fallback, circuit breaker, concurrent read/dispose races, and health-check churn stress (16 concurrent accessors + 1ms node-health toggling over 2 seconds — no unexpected exceptions, manager still functional after churn). |
| 27 | Treat multi-tenancy as a verified security boundary | Verified | Adversarial tests exist and docs include a threat-model boundary inventory. `LiveProviderMultiTenancySecurityTests` proves SELECT/UPDATE/DELETE isolation, adversarial tenant-ID parameterization (no injection), INSERT TenantId stamping, and compiled-query tenant-context enforcement across all four live providers. |
| 28 | Decide temporal/versioning stability | Verified | Temporal versioning is explicitly stable for nORM-managed history tables/triggers; docs define schema ownership, rollback responsibilities, and the RC live-provider evidence requirement. |
| 29 | Enforce provider version support at startup | Verified | Provider initialization validates the actual opened connection against `Capabilities.MinimumServerVersion`; unsupported versions throw `NormConfigurationException`, and provider docs/tests lock the startup contract. |
| 30 | Finish MySQL optimistic concurrency guarantees | Verified | MySQL affected-row OCC is refused by default through `RequireMatchedRowOccSemantics=true`; matched-row mode and explicit weakened opt-in are documented and covered by regression tests. |
| 31 | Prove AOT and trimming claims with real publish tests | Verified | AOT/trimming is explicitly unsupported for v1; annotation tests and a negative `PublishTrimmed=true` smoke test lock the current boundary with real publish diagnostics; additional tests verify annotation messages contain actionable AOT/trim context; `NormUnsupportedFeatureException` taxonomy test confirms the type is usable for future runtime guards; AOT publish warning scan added to `eng/v1-release-gate.ps1`. |
| 32 | Harden source generator limitations | Verified | Source-generation docs now define the v1 materializer support contract and diagnostics; package-consumer tests verify unsupported mapped properties report `nORMSG005`. |
| 33 | Stress cache and plan memory bounds as release gates | Verified | RC gate now has a dedicated cache memory bounds step; cache policy names the release evidence, and stress tests assert bounded counts/evictions for LRU, bounded FIFO, and compiled materializer caches. |
| 34 | Normalize public exception taxonomy | Verified | Public unsupported query paths for bulk CUD, async streaming Include/GroupJoin, and composite-key includes now throw `NormUnsupportedFeatureException`; docs/tests lock the taxonomy. |
| 35 | Complete logging and redaction coverage | Verified | `LogQuery`, CLI connection-string validation, and the built-in command interceptor now have tests proving SQL literals, parameter values, scalar results, and connection-string secrets are not emitted by default; docs cover benchmark/release artifact boundaries. |
| 36 | Freeze interceptor semantics | Verified | Command interceptor ordering, mutation visibility, suppression notifications, and failure propagation now have contract tests; docs define the suppression and mutation rules. |
| 37 | Rebuild benchmark evidence before any performance launch | Verified | Release gate now generates a benchmark evidence manifest from raw BenchmarkDotNet reports, including commit, SDK/OS, driver packages, redacted provider configuration, report paths, and fastest methods by provider. |
| 38 | Make benchmark thresholds executable | Verified | `eng/benchmark-thresholds.json` defines versioned latency/allocation budgets, `eng/check-benchmark-thresholds.ps1` enforces them from raw BenchmarkDotNet CSV reports, and the release gate runs the checker after benchmark evidence. |
| 39 | Reduce test-suite entropy before v1 | Verified | The stale `CS1998` test-project suppression is removed, ignored local TRX artifacts are cleaned, `.gitignore` covers TRX/coverage outputs, `docs/test-suite-ownership.md` contains the legacy coverage-file policy, and `RepositoryHygieneTests` locks the hygiene rules. |
| 40 | Run and publish a real RC release gate | In Progress | The current RC2 candidate has passed `eng/v1-release-gate.ps1 -Mode quick -SkipBenchmark`, `eng/v1-release-gate.ps1 -Mode live -MinLiveProviders 3 -SkipBenchmark` against SQL Server/PostgreSQL/MySQL, and a direct Release full-suite run (`9729/9729`). Focused provider-sliced benchmark budgets are green; see `docs/rc2-performance-slice-status.md`. Remaining RC2 release evidence: one final `-Mode rc -MinLiveProviders 3` run from the release commit, preferably without `-SkipBenchmark` if public performance claims will ship from the same artifact manifest. |

## Closure Rule

A blocker is not considered `Verified` unless at least one of the following is
true:

- an automated test covers the behavior,
- a public doc defines the support contract,
- a release gate or CI workflow enforces it,
- the item is explicitly marked `Not Applicable` with rationale.

Benchmark-related blockers require code-level benchmark categories, public-claim
constraints, and release artifacts before they can be used for launch claims.
