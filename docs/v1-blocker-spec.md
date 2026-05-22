# nORM v1.0 Blocker Developer Spec

Date: 2026-05-22

This is a fresh blocker audit of the current repository state, not a status
copy of the earlier spec. I inspected the solution/project files, README, docs,
CLI, migration tooling, LINQ/query pipeline, provider layer, package consumer
tests, benchmark governance, CI workflows, and release gate scripts.

Observed local evidence:

- `dotnet build nORM.sln -c Release --nologo` succeeds with 0 warnings after
  the P0 documentation/warning cleanup.
- Focused package/API/docs contract tests pass:
  `PackageConsumerIntegrationTests`, `PublicApiSnapshotTests`, and
  `DocumentationContractTests` passed 29 tests.
- I did not run the full test suite, live SQL Server/PostgreSQL/MySQL gates, or
  full BenchmarkDotNet provider matrix in this pass.

Several blockers from the earlier spec are no longer accurate: the source
generator is now packed, PostgreSQL/MySQL providers have public constructors,
`global.json` exists, governance files exist, and the CLI now uses
`ConnectionStringValidator.Validate(...)` so it can keep the real connection
string separate from the redacted one. The blockers below are the largest
remaining obstacles before a credible v1.0.

## Release Bar

v1.0 means:

- README examples compile against the shipped NuGet package.
- Generated API docs match the current public assembly.
- Public claims are backed by tests, docs, or release artifacts.
- The full live provider matrix passes in CI or release automation.
- Performance claims map to reproducible BenchmarkDotNet artifacts.
- Unsupported LINQ/provider behavior is explicit, deterministic, and documented.

## P0 Blockers

### 1. Remove or prove remaining public marketing claims

Problem: README still contains v1-risk language such as "drop-in replacement
for EF Core", "Why nORM Outperforms EF Core", "Enterprise Ready", "built-in
pooling", "lazy loading", and Dapper/Raw ADO comparison language. Some of this
is plausible, but v1 should not ship claims broader than the verified contract.

Evidence:

- `README.md` still has "Drop-in replacement for EF Core setup".
- `README.md` still has "nORM is designed as a drop-in replacement for EF Core".
- `README.md` still has "Why nORM Outperforms EF Core".
- Release benchmark artifacts were not produced in this pass.

Scope:

- Rewrite README claims into bounded, testable statements.
- Link every performance claim to a benchmark artifact requirement.
- Replace "drop-in replacement" with a migration guide that lists differences.

Done when:

- Documentation contract tests reject unqualified "drop-in", "outperforms",
  "beats raw ADO", and "Enterprise Ready" language.

### 2. Fix stale generated API docs

Problem: `docs/api` contains generated API pages for types that are not present
in `src`, including `nORM.Core.ConnectionPool` and
`nORM.Core.ConnectionPoolOptions`. Stale generated docs are dangerous for v1
because users will try APIs that no longer exist.

Evidence:

- `rg --files src | rg ConnectionPool` finds no source file.
- `docs/api/nORM.Core.ConnectionPool.yml` documents a public type.
- `docs/api/toc.yml` still lists `ConnectionPool`.
- README shows `new ConnectionPool(...)`.

Scope:

- Regenerate DocFX metadata from the current Release assembly.
- Delete generated pages for removed public types.
- Add a docs contract test that fails when docs reference missing public types.

Done when:

- `docs/api` can be regenerated cleanly from source and contains no removed API.

### 3. Decide whether `ConnectionPool` is removed or restored

Problem: Runtime code says custom connection pooling was removed in favor of
ADO.NET provider pooling, while README and production docs still tell users to
use nORM's `ConnectionPool`.

Evidence:

- `src/nORM/Core/ConnectionManager.cs` remarks say it no longer uses custom
  connection pooling.
- `README.md` shows a `ConnectionPool` example.
- `docs/production-operations.md` says to use nORM's `ConnectionPool` for
  specific reasons.

Scope:

- Either restore a tested public `ConnectionPool` API or remove all docs for it.
- Clarify the difference between `ConnectionManager` and provider-native pooling.
- Add compile-tested README snippets for connection management.

Done when:

- A README consumer can compile every connection-management example.

### 4. Fix the v1 issue map and blocker accounting

Problem: `docs/v1-issue-map.md` marks all 40 blockers done, then also lists a
41st benchmark blocker. That undermines release governance and makes it look
like the checklist is being used to close work prematurely.

Evidence:

- `docs/v1-issue-map.md` contains rows 1-41.
- The current blocker spec is supposed to contain exactly 40 items.

Scope:

- Replace "Done" rows with current status: Open, In Progress, Verified, or
  Not Applicable.
- Remove the 41st row or merge it into the benchmark blocker.
- Require evidence links for every "Verified" row.

Done when:

- The v1 issue map and this spec agree on exactly 40 tracked blockers.

### 5. Make the Release build warning-free

Problem: The Release build succeeds but emits 25 `CS1998` warnings from tests.
The test project explicitly exempts `CS1998` from warnings-as-errors. For v1,
warnings should either be fixed or intentionally documented with a narrow
suppression.

Evidence:

- `tests/nORM.Tests.csproj` contains `WarningsNotAsErrors` for `CS1998`.
- `dotnet build nORM.sln -c Release --nologo` emitted 25 `CS1998` warnings.

Scope:

- Fix async tests that do not await anything.
- Convert truly synchronous tests to non-async facts.
- Keep only targeted suppressions with comments where async signatures are
  intentional.

Done when:

- Release build emits zero warnings, or every remaining warning has a tracked
  documented exception.

### 6. Harden public API snapshotting before freeze

Problem: `PublicApiSnapshotTests` is useful, but it is a custom reflection
formatter. v1 API freeze needs review-quality output and intentional handling
for advanced/provider-facing APIs.

Evidence:

- `tests/PublicApi.Shipped.txt` is generated by custom reflection code.
- The public surface includes many advanced types across Core, Configuration,
  Providers, Migration, Navigation, Enterprise, and SourceGeneration.

Scope:

- Review every public type for v1 supportability.
- Hide or mark preview/provider-facing APIs before v1.
- Consider adding a standard PublicApiGenerator/Roslyn-based snapshot in
  addition to the current reflection test.

Done when:

- Public API changes require explicit review and release-note classification.

### 7. Revisit runtime package dependency architecture

Problem: The runtime package pulls in SQL Server, SQLite, ScriptDom, memory
cache, logging abstractions, object pooling, hashing, and annotations, while
PostgreSQL/MySQL drivers are optional reflection-loaded dependencies. That may
be acceptable, but it is a large dependency footprint for an ORM core package.

Evidence:

- `src/nORM.csproj` references `Microsoft.Data.SqlClient`,
  `Microsoft.Data.Sqlite`, `Microsoft.SqlServer.TransactSql.ScriptDom`, and
  several Microsoft.Extensions packages.
- Package consumer tests assert that Npgsql/MySQL are not dependencies.

Scope:

- Decide whether v1 ships a monolithic `nORM` package or split provider/tooling
  packages.
- Move SQL Server-specific ScriptDom validation out of the hot runtime package if
  possible.
- Document exact package dependencies per provider.

Done when:

- Package dependency graph is intentional and documented.

### 8. Clean release artifact and version hygiene

Problem: The local release output contains both `nORM.0.9.0-preview.1.nupkg` and
`nORM.1.0.0.nupkg`. Stale packages in output folders can confuse tests,
manual installs, and release uploads.

Evidence:

- `src/bin/Release` contains `nORM.0.9.0-preview.1.nupkg`.
- `src/bin/Release` also contains `nORM.1.0.0.nupkg`.
- Project versions are hard-coded in multiple `.csproj` files.

Scope:

- Centralize package versioning.
- Make release scripts clean package output before packing.
- Validate that only expected package versions exist after release gate.

Done when:

- Release artifacts are deterministic and stale package files cannot be uploaded.

### 9. Run package consumer tests cross-platform

Problem: Package consumer tests are strong, but local Windows success is not
enough. Package casing, analyzer paths, tool installation, and generated file
paths can behave differently on Linux.

Evidence:

- Package consumer tests inspect `src/bin/Release/nORM.0.9.0-preview.1.nupkg`.
- CI has Windows and Ubuntu jobs, but the quick release gate runs on Windows.

Scope:

- Run package consumer tests on Windows and Linux.
- Verify analyzer packing, source generation, and dotnet tool installation on
  both OSes.
- Add a case-sensitive package-path assertion on Linux.

Done when:

- Package consumer tests pass in CI on Windows and Linux from produced packages.

### 10. Replace or freeze the beta CLI dependency

Problem: `dotnet-norm` depends on `System.CommandLine` beta bits. A v1 tool
should avoid beta command-line parsing APIs unless the project explicitly owns
that compatibility risk.

Evidence:

- `src/dotnet-norm/dotnet-norm.csproj` references
  `System.CommandLine` version `2.0.0-beta7.25380.108`.

Scope:

- Move to a stable CLI parser or document the beta dependency as an explicit v1
  risk.
- Add tool package compatibility tests for help, invalid options, exit codes,
  and cancellation.

Done when:

- CLI command behavior is stable across supported SDK/runtime versions.

### 11. Isolate CLI design-time assembly loading

Problem: `norm migrations add` loads user assemblies through `Assembly.LoadFrom`.
That is fragile for dependency probing, multiple target frameworks, shadow
copying, and repeated invocations in the same process.

Evidence:

- `src/dotnet-norm/Program.cs` uses `Assembly.LoadFrom(asmPath)`.
- Design-time context discovery is central to migration generation.

Scope:

- Use `AssemblyLoadContext` with dependency resolution.
- Add tests for assemblies with external dependencies.
- Support explicit startup/project/deps paths if needed.

Done when:

- CLI migrations work against realistic app assemblies, not only simple test
  assemblies.

### 12. Harden destructive database drop behavior

Problem: `norm database drop` enumerates tables and drops them. That needs
provider-specific safety around schemas, foreign-key order, views, system
objects, migration history, protected database names, and dry-run output.

Evidence:

- `src/dotnet-norm/Program.cs` gets `connection.GetSchema("Tables")` and issues
  `DROP TABLE`.
- There is a `--yes` and `--dry-run` path, but v1 needs stronger provider
  contracts.

Scope:

- Make drop ordering provider-aware.
- Exclude system schemas and views explicitly.
- Add live-provider destructive-operation tests in disposable databases.

Done when:

- Drop behavior is safe, predictable, and provider-documented.

### 13. Make migration rename/data-loss handling first-class

Problem: The migration diff still treats rename-like changes as drop/add unless
the user manually edits the migration. That is acceptable only if v1 loudly
forces a safe workflow.

Evidence:

- README warns that column renames produce drop/add diffs.
- Forced migrations include TODO warnings.

Scope:

- Add explicit rename operations or a guided rename annotation/command.
- Require destructive diffs to be reviewed in generated migration source.
- Add round-trip tests for rename workflows.

Done when:

- A property/table rename cannot silently become data loss.

### 14. Prove migration recovery and idempotency across providers

Problem: SQL Server/PostgreSQL/SQLite/MySQL have different transactional DDL
semantics. MySQL DDL can auto-commit per step, so v1 needs explicit recovery
behavior for partial failures.

Evidence:

- README notes MySQL DDL auto-commit behavior.
- The release gate has migration filters, but this pass did not run live gates.

Scope:

- Add fault-injected live migration tests for each provider.
- Verify advisory locks, replay behavior, and migration history consistency.
- Document manual recovery procedures.

Done when:

- A failed migration leaves a known recoverable state for every provider.

### 15. Complete migration SQL live parity

Problem: SQL generator unit tests are not enough for v1. Generated DDL must be
executed against the actual provider versions that v1 claims to support.

Evidence:

- Migration SQL generator tests exist.
- Live provider gates are present but were not run in this pass.

Scope:

- Run generated create/alter/drop/index/fk/default migrations live on all four
  providers.
- Store provider version and driver version in release artifacts.
- Add failing SQL text to test output with secrets redacted.

Done when:

- Migration SQL parity is verified by live provider gates.

## P1 Blockers

### 16. Generate the LINQ support matrix from tests

Problem: `docs/linq-support.md` is useful, but it is hand-maintained. v1 needs
the matrix to be anchored to executable coverage so docs cannot drift from the
translator.

Evidence:

- The docs say set operations and `Any` are supported.
- Tests contain comments and workarounds for some operator edge cases.

Scope:

- Create a LINQ matrix test inventory.
- Link each supported/constrained/unsupported row to tests.
- Fail documentation contract tests when matrix rows lose coverage.

Done when:

- Every advertised LINQ shape has a matching test or documented exclusion.

### 17. Resolve `Any` and `All` semantics across providers

Problem: A test comment says `AnyAsync` can fail with SQLite datatype mismatch
and uses `CountAsync > 0` as an equivalent idiom, while the LINQ matrix says
`Any` is supported.

Evidence:

- `tests/LinqOperatorCardinalityTests.cs` comments mention `AnyAsync` datatype
  mismatch on SQLite configurations.
- `docs/linq-support.md` lists `Any` and `All` as supported.

Scope:

- Add direct `AnyAsync` and `AllAsync` cardinality tests.
- Verify predicate overloads across SQLite, SQL Server, PostgreSQL, and MySQL.
- Fix translation or mark constrained/unsupported precisely.

Done when:

- `Any`/`All` behavior is consistent with the matrix.

### 18. Stabilize Include and lazy-loading contracts

Problem: Relationship loading is complex and currently constrained. Composite
key dependent includes throw `NotSupportedException`, async streaming rejects
Include, and lazy loading uses nORM wrapper types rather than EF-style proxy
semantics.

Evidence:

- `docs/linq-support.md` says composite-key dependent includes throw
  `NotSupportedException`.
- `src/nORM/Navigation/NavigationPropertyExtensions.cs` uses
  `LazyNavigationCollection<T>` and `LazyNavigationReference<T>`.

Scope:

- Decide which Include shapes are v1-stable.
- Convert public unsupported Include failures to nORM exception taxonomy.
- Document lazy-loading model and non-EF differences clearly.

Done when:

- Relationship loading is either supported or predictably rejected for each
  documented shape.

### 19. Prove terminal operator parity in every execution path

Problem: Single-result operators (`First`, `Single`, `Last`, `ElementAt`, and
default variants) have multiple execution paths: simple fast path, normal
query executor, sync APIs, async APIs, and compiled queries. These are easy to
diverge.

Evidence:

- `NormQueryProvider.Compiled.cs` has repeated switch blocks for single-result
  handling.
- `Last`/`LastOrDefault` depend on translator ordering reversal and then return
  `list[0]`.

Scope:

- Add a table-driven test suite covering terminal operators across all paths.
- Include empty, one-row, two-row, ordered, unordered, skipped, and filtered
  cases.
- Add compiled-query variants.

Done when:

- Terminal operator behavior matches LINQ semantics or is documented otherwise.

### 20. Decide the v1 default for client evaluation

Problem: `ClientEvaluationPolicy.Warn` is the current default. That is
developer-friendly, but it can hide performance cliffs and execute user code
after materializing rows.

Evidence:

- `DbContextOptions.ClientEvaluationPolicy` defaults to `Warn`.
- `docs/linq-support.md` documents warn/throw/allow modes.

Scope:

- Decide whether v1 default should be `Throw` for safety or `Warn` for
  compatibility.
- Add prominent docs and logging behavior for the chosen default.
- Ensure server filters/paging always execute before allowed client projection.

Done when:

- Client evaluation behavior is a deliberate v1 compatibility decision.

### 21. Remove legacy string-based bulk CUD paths

Problem: Bulk CUD now has structural shape validation, but legacy SQL string
validation and `ExtractWhereClause` still exist. Dead fallback paths invite
future regressions.

Evidence:

- `BulkCudBuilder` contains both `ValidateCudPlan(string sql)` and
  `ValidateCudPlan(BulkCudQueryShape? shape)`.
- `ExtractWhereClause` still parses SQL text.

Scope:

- Remove or quarantine string-based bulk CUD parsing.
- Ensure bulk update/delete use structural metadata only.
- Add tests that comments/literals/quoted identifiers cannot confuse bulk CUD.

Done when:

- Bulk CUD does not depend on ad hoc SQL text parsing.

### 22. Define bulk update value-expression support

Problem: `ExecuteUpdate` set values are limited to constants and captured local
values. That is safer than invoking arbitrary expressions, but it is narrower
than many users expect from EF-style `ExecuteUpdate`.

Evidence:

- `BulkCudBuilder.TryGetSetValue` only accepts constants and captured fields.
- `BatchCudTests` verifies method calls are rejected.

Scope:

- Document supported set expressions.
- Decide whether computed server expressions are v1 or post-v1.
- Add clear exception messages for unsupported computed updates.

Done when:

- Users know exactly what `ExecuteUpdate` can express.

### 23. Replace raw SQL safety heuristics with provider-aware validation

Problem: `FromSqlRawAsync` and stored procedure APIs rely on SQL safety
validation. Blacklist/heuristic validators can be both too strict and too weak,
especially across providers.

Evidence:

- Raw SQL docs say caller owns SQL shape.
- `NormValidator.IsSafeRawSql` has extensive adversarial tests.
- SQL Server ScriptDom is already a runtime dependency.

Scope:

- Define a provider-aware raw SQL policy.
- Use structured parsing where practical.
- Clearly separate read-only raw query APIs from privileged escape hatches.

Done when:

- Raw SQL safety is deterministic and documented per provider.

### 24. Tighten stored procedure security and tenant boundaries

Problem: Stored procedures bypass LINQ tenant filters unless the procedure
itself enforces tenant isolation. That is fine for privileged paths, but v1 must
make the boundary explicit.

Evidence:

- `docs/multi-tenancy-security.md` says stored procedures must enforce tenant
  isolation internally or receive a tenant parameter.
- `DbContext.RawSql.cs` exposes stored procedure APIs.

Scope:

- Add examples for tenant-safe stored procedures.
- Add optional helper patterns that inject tenant parameters.
- Ensure docs label stored procedures as privileged/bypass-capable.

Done when:

- Multi-tenant users cannot mistake stored procedures for automatically filtered
  nORM queries.

### 25. Finish transaction and sync/async policy hardening

Problem: Transaction behavior spans explicit transactions, ambient
`TransactionScope`, savepoints, sync APIs, async APIs, temporal bootstrap, and
provider-specific unsupported operations.

Evidence:

- `DbContext.EnsureConnectionSync` can run temporal bootstrap synchronously.
- `TransactionManager` has provider fallback policies.
- Savepoint support differs by provider.

Scope:

- Verify transaction behavior under cancellation, disposal, retry, interceptors,
  and live providers.
- Document unsupported savepoint/ambient transaction cases per provider.
- Avoid sync-over-async deadlock hazards in docs and implementation.

Done when:

- Transaction behavior is predictable for each supported provider.

### 26. Prove `ConnectionManager` failover behavior under load

Problem: `ConnectionManager` handles health checks, primary selection, read
replicas, circuit breaking, and background disposal. That is production-critical
and separate from simple DbContext connection use.

Evidence:

- `ConnectionManager` starts a background health-check task.
- It creates fresh connections and relies on provider-native pooling.

Scope:

- Add stress tests for health-check failures, dispose races, replica churn, and
  failover under concurrent reads/writes.
- Document operational limits.
- Clarify logger/null behavior and ownership.

Done when:

- Failover/replica behavior is tested as a production feature, not just an API.

### 27. Treat multi-tenancy as a verified security boundary

Problem: Multi-tenancy touches queries, includes, bulk operations, compiled
queries, cache keys, raw SQL, stored procedures, migrations, and direct
connection access. Any one bypass can become a data leak.

Evidence:

- There are extensive adversarial tenant tests.
- Docs still classify raw SQL and stored procedures as caller-owned.

Scope:

- Run tenant security tests in the full live provider matrix.
- Add a threat model table with bypass-capable APIs.
- Require tenant cache-key and compiled-query isolation tests for every new
  query path.

Done when:

- "multi-tenancy" is a documented and tested security contract.

### 28. Decide temporal/versioning stability

Problem: Temporal versioning creates provider-specific history tables and
triggers during connection initialization. That is a big schema side effect for
a v1-stable feature.

Evidence:

- `DbContextOptions.EnableTemporalVersioning()` bootstraps temporal objects.
- `TemporalManager` validates and executes provider DDL.
- Docs describe temporal schema and migration interaction.

Scope:

- Decide whether temporal is stable or preview for v1.
- Verify bootstrap/migration interaction on live providers.
- Document schema ownership and rollback strategy.

Done when:

- Temporal versioning has a clear v1 support level.

### 29. Enforce provider version support at startup

Problem: Providers depend on driver behavior and database version capabilities,
but PostgreSQL/MySQL drivers are loaded by reflection and many failures can show
up late.

Evidence:

- `PostgresProvider` and `MySqlProvider` use reflection parameter factories.
- Provider capability docs exist but startup enforcement is the real gate.

Scope:

- Validate driver presence and minimum versions at startup.
- Validate database version/capability where required.
- Produce actionable errors for missing optional drivers.

Done when:

- Provider setup fails early and clearly.

### 30. Finish MySQL optimistic concurrency guarantees

Problem: MySQL affected-row semantics can weaken optimistic concurrency when a
concurrent writer updates a token to the same value. The repo has an option and
docs, but this remains a serious data-integrity edge.

Evidence:

- `DbContextOptions.RequireMatchedRowOccSemantics` defaults to true.
- Its XML docs describe an unsafe same-value token collision mode.

Scope:

- Verify default behavior with both MySqlConnector affected-row modes.
- Add live tests for same-value token conflicts.
- Keep unsafe mode very explicit in docs and exception messages.

Done when:

- MySQL OCC semantics are either strong by default or loudly refused.

### 31. Prove AOT and trimming claims with real publish tests

Problem: Docs describe JIT-first behavior and AOT/trimming limits, but v1 should
have actual publish-time validation for the supported subset.

Evidence:

- `docs/aot-trimming.md` documents reflection, dynamic code, and source
  generation boundaries.
- Runtime materialization and scaffolding use reflection/dynamic code paths.

Scope:

- Add `PublishTrimmed` and NativeAOT smoke tests for the supported subset.
- Ensure unsupported dynamic features fail at build or startup with clear
  messages.
- Document exact linker warnings that are accepted.

Done when:

- AOT/trimming support is verified by publish artifacts, not only docs.

### 32. Harden source generator limitations

Problem: Source generation now ships in the package, but generator support is
intentionally narrower than runtime materialization. Limitations around
parameterless constructors, fluent-only renames, owned types, converters, and
nullability need sharp diagnostics.

Evidence:

- `MaterializerQueryGenerator` emits diagnostics for unsupported return types
  and missing parameterless constructors.
- Source-generation docs warn about runtime-only mapping cases.

Scope:

- Add analyzer diagnostics for every unsupported source-gen mapping feature.
- Ensure generated code is compatible with trimmed/AOT-supported scenarios.
- Keep package-consumer tests covering generated materializers and compile-time
  queries.

Done when:

- Source generation either works or explains exactly why it cannot.

### 33. Stress cache and plan memory bounds as release gates

Problem: The project has many caches: query plans, compiled parameter sets,
materializers, result cache, tenant-scoped cache keys, and command pools. v1
needs bounded behavior under adversarial dynamic query shapes.

Evidence:

- `NormQueryProvider.Compiled.cs` has a bounded compiled parameter-set cache.
- Cache policy docs exist.
- Many tests cover cache collision and tenant isolation.

Scope:

- Run cache stress tests in RC mode with memory baselines.
- Add diagnostics for cache size/eviction where missing.
- Prove tenant and provider dimensions are always included in cache keys.

Done when:

- Cache growth is bounded and observable.

### 34. Normalize public exception taxonomy

Problem: Docs define nORM exception types, but public paths still throw
`NotSupportedException`, `InvalidOperationException`, and raw provider
exceptions in some unsupported-feature cases.

Evidence:

- `docs/linq-support.md` says some Include shapes throw `NotSupportedException`.
- Query translator paths use `NormUnsupportedFeatureException` in many places.

Scope:

- Decide which exceptions are part of the public contract.
- Wrap unsupported query/provider features consistently.
- Keep provider exceptions where they are intentionally transparent.

Done when:

- Users can catch stable nORM exceptions for expected unsupported features.

### 35. Complete logging and redaction coverage

Problem: Logging touches SQL, parameters, CLI errors, connection strings,
interceptor output, benchmark output, and release artifacts. Redaction must be
global and test-backed.

Evidence:

- `ConnectionStringValidator` has redaction support.
- Logging docs exist.
- Raw benchmark/release artifacts can include connection configuration summaries.

Scope:

- Audit every log/error path for secrets.
- Add tests for redaction in CLI, runtime logging, benchmark summaries, and
  release artifacts.
- Define what is never logged.

Done when:

- Secrets cannot leak through normal diagnostics.

### 36. Freeze interceptor semantics

Problem: Interceptors can observe or alter commands around transactions,
retries, cancellation, raw SQL, compiled queries, generated compile-time queries,
stored procedures, and bulk operations. This is a public extensibility point.

Evidence:

- `DbContextOptions.CommandInterceptors` and `SaveChangesInterceptors` are
  public.
- Docs describe interceptor ordering and transaction behavior.

Scope:

- Verify interceptor ordering under retry and transaction ownership.
- Decide which mutations are supported.
- Document suppression behavior and failure semantics.

Done when:

- Interceptors are a stable v1 contract.

### 37. Rebuild benchmark evidence before any performance launch

Problem: The benchmark harness is real and has improved categories, but v1
performance claims still require full reproducible artifacts and write-path
semantic parity. Beating Raw ADO only matters if the Raw ADO category is named
and comparable.

Evidence:

- `docs/benchmark-governance.md` defines Raw ADO convenience/optimized/prepared
  categories.
- `BenchmarkFairnessLockTests` checks some benchmark labels and reader paths.
- This pass did not run the full provider matrix.

Scope:

- Run full BenchmarkDotNet provider matrix on release hardware.
- Publish raw artifacts, SDK, hardware, driver versions, schema, seed, and
  command line.
- Verify read and write scenarios have equivalent SQL shape and semantics.
- Separate identity-hydrating inserts from throughput-only inserts.

Done when:

- Every public performance claim maps to a benchmark artifact.

### 38. Make benchmark thresholds executable

Problem: Docs describe benchmark governance, but release gates need enforceable
budgets. Otherwise benchmark output becomes advisory and marketing can drift.

Evidence:

- `eng/v1-release-gate.ps1` can run benchmarks.
- CI quick gates skip benchmarks.
- RC workflow has a `skip_benchmark` input.

Scope:

- Add pass/fail thresholds for latency and allocation regressions.
- Make threshold files versioned.
- Require explicit override notes when benchmarks are skipped.

Done when:

- Performance regressions fail the release gate.

### 39. Reduce test-suite entropy before v1

Problem: The test suite is large and valuable, but it includes coverage-boost
files, old TRX artifacts in local search results, async-warning suppressions,
and many overlapping tests. v1 needs maintainable signal, not just volume.

Evidence:

- Build emits many async test warnings.
- `rg` searches hit `tests/TestResults` artifacts unless ignored.
- Many files are named `CoverageBoost*`.

Scope:

- Ensure generated artifacts and test results are ignored and absent from source.
- Rename or consolidate low-signal coverage-only tests where practical.
- Keep adversarial and regression tests, but make ownership clear.

Done when:

- The test suite is maintainable and warning-free.

### 40. Run and publish a real RC release gate

Problem: The repo now has release scripts and workflows, but v1 is not credible
until a full RC gate has actually run and its artifacts are reviewed.

Evidence:

- `eng/v1-release-gate.ps1` supports `quick`, `live`, `full`, and `rc`.
- `.github/workflows/v1-rc.yml` exists.
- This pass did not run the full RC gate or live provider matrix.

Scope:

- Run `eng/v1-release-gate.ps1 -Mode rc -MinLiveProviders 3`.
- Upload full test, live provider, package, benchmark, and environment
  artifacts.
- Review and link artifacts in the release checklist.

Done when:

- v1.0 is backed by an auditable RC run from the release commit.
