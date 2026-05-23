# nORM v1.0 Blocker Developer Spec

Date: 2026-05-23

This is a current v1.0 blocker spec for the repository as it stands on
2026-05-23. It is intentionally written as an engineering backlog, not as a
marketing launch checklist. A blocker is considered closed only when the code,
tests, documentation, release automation, and release artifacts all line up.

## Audit Evidence

Local evidence gathered during this pass:

- `dotnet build nORM.sln -c Release --nologo` succeeds with 0 warnings.
- The Release build produces `nORM.0.9.0-preview.1` and
  `dotnet-norm.0.9.0-preview.1` packages, so the repo is still versioned as a
  preview build.
- `tests/PublicApi.Shipped.txt` exposes a broad API surface and includes
  `nORM.Internal.ConcurrentLruCache<T, TValue>` and
  `nORM.Internal.ParameterOptimizer`.
- `src/nORM.csproj` still ships SQL Server, SQLite, ScriptDom, caching, logging,
  object-pool, annotations, hashing, and SourceLink dependencies in the core
  package.
- Benchmarks use some package versions that differ from tests/runtime, including
  `Npgsql` 8.x in benchmarks and 10.x in tests.
- A focused contract-test command covering public API, docs, repository hygiene,
  package consumer, and CLI tests timed out locally after about 184 seconds
  before a clean result was returned.
- The live provider and RC gates exist, but no successful full RC artifact
  manifest was produced during this audit.

## v1.0 Release Bar

v1.0 should mean:

- Public API is frozen, intentional, documented, and supportable.
- README, DocFX output, package metadata, and examples compile against the
  shipped NuGet package.
- LINQ support is precise, test-backed, and deterministic for unsupported
  shapes.
- SQL Server, PostgreSQL, MySQL, and SQLite behavior is verified against live
  provider gates where provider behavior matters.
- Tenant isolation, raw SQL boundaries, logging redaction, transactions, caches,
  retries, and interceptors are treated as production contracts.
- Performance claims are backed by raw BenchmarkDotNet artifacts from the
  release commit and named baselines.
- A real RC run publishes packages, TRX files, benchmark output, environment
  metadata, and an artifact manifest.

## 40 Biggest Blockers

### 1. Freeze and reduce the public API surface

Problem: The public API is too broad for a v1 freeze, and it exposes implementation
details under `nORM.Internal`.

Evidence:

- `tests/PublicApi.Shipped.txt` contains over 1,000 public API lines.
- `nORM.Internal.ConcurrentLruCache<T, TValue>` and
  `nORM.Internal.ParameterOptimizer` are public in the shipped baseline.
- Mapping, migration, provider, navigation, source-generation, and execution
  implementation types are all exposed.

Work:

- Classify every public type as stable user API, stable provider API, preview
  API, or accidental exposure.
- Make accidental implementation types internal, or move them to a deliberately
  named advanced namespace with documentation.
- Require XML docs and behavioral tests for every type retained as stable.

Acceptance gate:

- Public API diffs are reviewed against a supportability classification file.
- No `nORM.Internal.*` type remains in the stable public API without an explicit
  v1 decision.

### 2. Move from preview packages to deterministic v1 package versioning

Problem: The repository still packages as `0.9.0-preview.1`.

Evidence:

- `Directory.Build.props` sets `NormVersion` to `0.9.0-preview.1`.
- Release builds currently produce preview package filenames.

Work:

- Define the final v1 versioning flow, including RC prereleases, stable v1, and
  post-v1 development versions.
- Make release scripts validate the expected package version and reject stale
  package outputs.
- Ensure docs, package tests, and release manifests all read version from the
  same source.

Acceptance gate:

- The RC gate can produce exactly the expected package set for a chosen v1
  candidate version, with no stale `.nupkg` or `.snupkg` artifacts.

### 3. Decide the core package and provider package architecture

Problem: The runtime package has a large dependency footprint and mixed provider
ownership.

Evidence:

- `src/nORM.csproj` includes SQL Server, SQLite, ScriptDom, caching, logging,
  object pooling, annotations, and hashing.
- PostgreSQL and MySQL drivers are optional and reflection-loaded.
- Provider docs say SQL Server and SQLite drivers are included, while
  PostgreSQL/MySQL require consumer packages.

Work:

- Decide whether v1 ships one monolithic package or splits provider packages.
- Move provider-specific heavy dependencies out of core if that is the selected
  model.
- Document package dependency and driver version policy per provider.

Acceptance gate:

- Package dependency graph is intentional, tested with package-consumer projects,
  and documented in `docs/provider-packages.md`.

### 4. Prove package consumer behavior cross-platform

Problem: Local package consumption is not enough for v1.

Evidence:

- Package consumer tests exist, but the focused local command timed out during
  this audit.
- Analyzer packing, source generation, tool installation, path casing, and CLI
  behavior can differ between Windows and Linux.

Work:

- Run package consumer tests on Windows and Linux from produced packages.
- Verify analyzer inclusion, source-generation diagnostics, `dotnet tool`
  install, README snippets, and optional provider dependencies.
- Ensure package tests use current versioned package paths, not stale preview
  assumptions.

Acceptance gate:

- CI publishes successful package-consumer artifacts for Windows and Linux from
  the same package files intended for release.

### 5. Regenerate and validate generated API documentation

Problem: Generated API docs must match the actual v1 assembly.

Evidence:

- `docs/api` is generated output and can drift from source.
- The old blocker spec previously identified stale API docs, so this needs to
  remain mechanically guarded.

Work:

- Regenerate DocFX metadata from the Release assembly.
- Add or keep a test that fails when `docs/api` references missing public types.
- Ensure docs for preview APIs clearly say preview.

Acceptance gate:

- `docs/api` can be regenerated cleanly from the current build and contains no
  missing, stale, or accidentally stable APIs.

### 6. Tighten README and package metadata claims

Problem: Some user-facing language still reads broader than the verified v1
contract.

Evidence:

- README has headings such as "Production-Ready Features" and "Full support for
  major database engines".
- README advertises "Smart Relationship Handling", preview scaffolding, temporal
  versioning, JSON, window functions, raw SQL, stored procedures, and full
  provider coverage in one document.

Work:

- Replace broad launch language with bounded, test-backed claims.
- Link every performance, provider, LINQ, and production claim to evidence or
  a support matrix.
- Add documentation tests that reject unqualified "full LINQ", "drop-in",
  "beats raw ADO", and similar claims.

Acceptance gate:

- README and package metadata describe the v1 contract without overclaiming.

### 7. Turn the LINQ matrix into a hard compatibility contract

Problem: The docs say nORM supports a broad LINQ subset, but "full LINQ" is not a
credible v1 claim unless every advertised shape is locked down.

Evidence:

- `docs/linq-support.md` lists supported, constrained, preview, and unsupported
  features.
- `docs/linq-support-coverage.md` maps rows to tests, but it is still file-level
  evidence, not provider/result-path proof for every shape.

Work:

- Convert the matrix into table-driven tests for SQLite plus live provider
  parity for SQL Server, PostgreSQL, and MySQL.
- Record which execution paths are covered: normal, fast path, compiled query,
  sync helper, async helper, and source-generated path where applicable.
- Reject unsupported shapes with stable nORM exceptions.

Acceptance gate:

- Every non-unsupported LINQ matrix row has executable result-behavior evidence
  and provider parity evidence or a documented provider limitation.

### 8. Prove terminal operator parity across every execution path

Problem: Terminal operators are easy to make fast but subtly wrong.

Evidence:

- `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last`,
  `LastOrDefault`, `ElementAt`, `Any`, `All`, `Count`, and aggregate paths span
  several query execution paths.
- Compiled-query and fast-path implementations contain separate logic.

Work:

- Add table-driven tests for empty, one-row, two-row, ordered, unordered,
  filtered, skipped, and paged inputs.
- Cover sync, async, compiled, source-generated, and provider-specific paths.
- Document any deviation from LINQ-to-Objects semantics.

Acceptance gate:

- Terminal operators either match LINQ semantics or fail deterministically with a
  documented exception.

### 9. Finish aggregate, null, boolean, enum, and type-conversion parity

Problem: ORM correctness often fails in type edge cases rather than happy paths.

Evidence:

- Tests exist for null semantics, aggregate operators, booleans, enums,
  materialization conversions, and provider parameter binding.
- Provider SQL dialects differ materially for booleans, dates, JSON, decimals,
  and enums.

Work:

- Build a provider matrix for scalar aggregates and type conversions.
- Include nullable columns, nullable concurrency tokens, decimal precision,
  DateOnly/TimeOnly, enums, booleans, GUIDs, and provider-native JSON values.
- Ensure parameter metadata does not contaminate cached plans.

Acceptance gate:

- Scalar query semantics are equivalent across supported providers or documented
  as provider-specific.

### 10. Stabilize Include, ThenInclude, and lazy loading

Problem: Relationship loading is a major v1 support surface and still has
documented constraints.

Evidence:

- `docs/linq-support.md` says Include is constrained.
- Composite-key dependent includes and async streaming with Include are
  constrained or rejected.
- Public lazy-loading wrapper types are in the API surface.

Work:

- Define exact v1 support for reference, collection, many-to-many, owned,
  split-query, composite-key, and nested include shapes.
- Ensure every unsupported shape throws `NormUnsupportedFeatureException`.
- Document that nORM lazy loading uses wrapper types, not EF proxy semantics.

Acceptance gate:

- Relationship-loading docs, tests, and public API behavior agree for every
  supported and unsupported shape.

### 11. Harden GroupBy, GroupJoin, SelectMany, and set operations

Problem: These operators are common sources of semantic drift between LINQ and
SQL.

Evidence:

- The LINQ matrix marks `GroupBy`, `GroupJoin`, and `SelectMany` as constrained.
- Group joins are bounded by `DbContextOptions.MaxGroupJoinSize`.

Work:

- Add cross-provider result tests for simple and nested joins, grouped aggregate
  projections, correlated collections, set operation ordering, and duplicate
  behavior.
- Keep unsupported `IGrouping` and streaming scenarios deterministic.
- Document complexity limits and memory behavior.

Acceptance gate:

- Complex LINQ shapes are either stable across providers or explicitly outside
  the v1 contract.

### 12. Make client evaluation impossible to misunderstand

Problem: Client evaluation can hide performance cliffs and security bugs if it
applies before filtering or paging.

Evidence:

- `docs/linq-support.md` says v1 default is `ClientEvaluationPolicy.Throw`.
- `DbContextOptions` exposes `Throw`, `Warn`, and `Allow`.

Work:

- Verify the runtime default is `Throw`.
- Test that `Warn` and `Allow` only permit projection-tail evaluation after
  server filtering, ordering, and paging.
- Ensure logs make client evaluation visible without leaking values.

Acceptance gate:

- Client evaluation cannot pull unbounded rows before server-side restrictions.

### 13. Prove compiled query isolation and shape parity

Problem: Compiled queries are central to the performance story and can leak
state through caches.

Evidence:

- Tests exist for compiled query tenant isolation, provider matrix, parameter
  binding, fast path, lifecycle, and SQL shape.
- Compiled paths still need release-gate evidence across live providers.

Work:

- Compare compiled and non-compiled SQL shape, parameters, tenant filters,
  materialization, cancellation, and exceptions.
- Stress compiled caches under dynamic query and tenant churn.
- Include compiled queries in benchmark and RC gates.

Acceptance gate:

- Compiled queries are semantically identical to normal nORM queries for the
  advertised shapes.

### 14. Finish source generator and compile-time query contracts

Problem: Source generation is shipped in the runtime package but supports a
narrower mapping subset than runtime materialization.

Evidence:

- The source generator targets `netstandard2.0`.
- Docs describe limitations and diagnostics.
- Source-generation paths interact with package-consumer tests and AOT docs.

Work:

- Keep analyzer diagnostics precise for unsupported shapes.
- Cover fluent renames, owned types, converters, nullability, constructors, and
  generated materializer cache behavior.
- Verify generated code compiles and behaves in package-consumer projects.

Acceptance gate:

- Source generation either works or fails with a stable diagnostic for every
  documented v1 scenario.

### 15. Treat raw SQL and stored procedures as privileged security boundaries

Problem: Raw SQL and stored procedures bypass parts of ORM-generated query
protection.

Evidence:

- README and docs say raw SQL is read-only gated and stored procedures are
  privileged.
- Multi-tenancy docs say stored procedures must enforce tenant isolation
  themselves.

Work:

- Prove provider-aware raw SQL validation for SELECT/CTE only.
- Keep stored procedure command-name validation provider-specific.
- Add examples and tests for tenant-safe stored procedure parameters.

Acceptance gate:

- Users cannot mistake raw SQL or stored procedures for automatically tenant-
  filtered ORM queries.

### 16. Verify multi-tenancy as a security contract

Problem: Tenant isolation crosses queries, includes, compiled queries, caches,
bulk operations, raw SQL, stored procedures, migrations, and direct connection
access.

Evidence:

- The test suite contains many adversarial tenant and cache-poisoning tests.
- Docs classify raw SQL, stored procedures, migrations, scaffolding, and direct
  connection access as caller-controlled privileged paths.

Work:

- Build a single threat-model table listing protected paths and bypass-capable
  paths.
- Run tenant isolation tests in the live provider matrix.
- Require tenant cache-key tests for every new query execution path.

Acceptance gate:

- Multi-tenancy is verified as a boundary, not just a convenience filter.

### 17. Prove cache memory bounds, cache keys, and invalidation

Problem: nORM uses many caches, and unbounded or cross-tenant cache behavior is a
v1 production risk.

Evidence:

- `docs/cache-policy.md` documents cache bounds.
- Tests cover cache collisions, tenant cache poisoning, cache eviction, and
  compiled materializer store bounds.

Work:

- Keep stress tests in the RC gate.
- Expose or document diagnostics for critical shared caches.
- Validate result-cache keys include provider, database identity, SQL shape,
  parameters, tracking mode, tenant, and model fingerprint where needed.

Acceptance gate:

- Dynamic query churn and tenant churn cannot grow caches without bound or reuse
  the wrong result.

### 18. Harden change tracking, identity maps, and snapshots

Problem: Change tracking correctness is as important as query speed for a v1 ORM.

Evidence:

- Tests exist for precise change tracking, constructor-bound entities, primary
  key mutation, composite keys, snapshot aliasing, notifications, and cleanup.
- Public APIs expose `ChangeTracker`, `EntityEntry`, attach/add/delete/update,
  and tracking modes.

Work:

- Define stable semantics for attaching, detaching, PK mutation, owned entities,
  relationship fixup, shadow properties, immutable/constructor-bound entities,
  and notification-based tracking.
- Stress identity maps under repeated load/save/clear cycles.
- Ensure failures are stable nORM exceptions where callers can recover.

Acceptance gate:

- Change tracking semantics are documented and covered for the supported entity
  model shapes.

### 19. Finalize SaveChanges, graph ordering, and cascade behavior

Problem: Write ordering bugs cause data corruption and foreign-key failures.

Evidence:

- Tests cover FK ordering, cascade delete, owned collections, composite keys,
  many-to-many, concurrency tokens, and batching.
- `SaveChangesAsync` is the only save API, while sync query APIs still exist.

Work:

- Prove insert/update/delete ordering across aggregate graphs.
- Define cascade behavior and ownership semantics for owned and many-to-many
  relationships.
- Keep batching behavior observable and provider-safe.

Acceptance gate:

- Complex graph writes are atomic, ordered, and correctly accepted or rejected
  across providers.

### 20. Finish optimistic concurrency across all write paths

Problem: Concurrency semantics must be consistent across normal writes, bulk
writes, deletes, and provider-specific affected-row behavior.

Evidence:

- README documents a residual MySQL same-value token conflict gap.
- Tests cover optimistic concurrency and provider-specific MySQL behavior.

Work:

- Align normal write, bulk update, bulk delete, timestamp, nullable token, and
  composite-key concurrency behavior.
- Keep MySQL affected-row semantics fail-fast by default or explicitly weakened
  by opt-in.
- Document the residual same-value token gap and recommended token strategy.

Acceptance gate:

- Concurrency conflicts are detected consistently or the provider-specific gap
  is explicit and opt-in.

### 21. Finish transaction, savepoint, ambient, retry, and interceptor semantics

Problem: Transactions touch almost every subsystem and vary by provider.

Evidence:

- Docs cover transactions and sync policy.
- Providers implement savepoint methods with provider-specific behavior, while
  base provider paths throw unsupported exceptions.

Work:

- Verify explicit transactions, ambient `TransactionScope`, savepoints,
  retries, cancellation, raw SQL, stored procedures, bulk operations, temporal
  bootstrap, migrations, and interceptors together.
- Normalize unsupported savepoint failures.
- Document ownership rules for externally supplied connections and
  transactions.

Acceptance gate:

- Transaction behavior is predictable for each provider and execution path.

### 22. Complete cancellation and timeout audits

Problem: Cancellation bugs can leave commands, transactions, temp tables, and
connection state inconsistent.

Evidence:

- Tests exist for cancellation across queries, materialization, migrations, bulk
  operations, commits, savepoints, temporal bootstrap, and fault-injected races.
- `AdaptiveTimeoutManager` is public.

Work:

- Audit every async public API for cancellation-token propagation.
- Verify cleanup after cancellation for transactions, temp tables, connection
  ownership, readers, command pools, and interceptors.
- Define timeout exception taxonomy and retry interaction.

Acceptance gate:

- Cancellation either completes cleanup or reports a documented recoverable
  state.

### 23. Prove provider dialect parity and version gates

Problem: Provider-specific SQL is a large part of nORM's surface area.

Evidence:

- Provider capabilities docs define minimum versions and supported features.
- Providers differ on paging, booleans, JSON, identifiers, savepoints, bulk
  operations, and parameter prefixes.

Work:

- Run provider parity for SQL generation and result behavior across SQLite, SQL
  Server, PostgreSQL, and MySQL.
- Validate actual server versions at connection initialization.
- Keep missing optional-driver errors actionable.

Acceptance gate:

- Provider differences are either hidden by nORM or documented as provider
  limitations with tests.

### 24. Harden bulk operations as provider-specific contracts

Problem: Bulk operations are a major performance feature and a major correctness
risk.

Evidence:

- README advertises provider-specific bulk insert, update, and delete.
- Base `DatabaseProvider` native bulk update/delete methods throw
  `NotImplementedException`.
- Tests cover bulk provider parity, tenant isolation, temp-table leaks, OCC, and
  transactions.

Work:

- Define native and fallback behavior per provider.
- Prove atomicity, rollback, cancellation cleanup, temp-table cleanup, tenant
  filters, concurrency checks, generated keys, and cache invalidation.
- Normalize unsupported native bulk paths to the v1 exception taxonomy.

Acceptance gate:

- Bulk operations are both fast and semantically equivalent to row-wise writes
  for supported scenarios.

### 25. Make migration rename and data-loss handling first-class

Problem: Rename-like changes still require manual correction to avoid data loss.

Evidence:

- README warns that property/column renames become drop/add diffs.
- Forced generated migrations include TODO warnings.

Work:

- Add explicit rename operations or a guided rename annotation/command.
- Require destructive diffs to be visibly reviewed in generated migration
  source.
- Add tests that prove generated migrations cannot silently drop data.

Acceptance gate:

- A table or column rename cannot become data loss without an explicit developer
  decision.

### 26. Prove migration recovery, idempotency, and live DDL parity

Problem: DDL behavior differs sharply across providers, especially MySQL
auto-commit behavior.

Evidence:

- Migration runners and SQL generators exist for SQLite, SQL Server,
  PostgreSQL, and MySQL.
- Release gates include migration filters, but a full RC run was not produced in
  this audit.

Work:

- Execute generated create/alter/drop/index/default/fk migrations live on all
  supported providers.
- Fault-inject failures and verify history table consistency.
- Document manual recovery for partially applied provider-specific DDL.

Acceptance gate:

- Failed migrations leave a known recoverable state for every supported
  provider.

### 27. Harden destructive CLI database drop

Problem: `norm database drop` is intentionally destructive.

Evidence:

- README says `--yes` is required and `--dry-run` previews objects.
- CLI code enumerates database tables and drops them.

Work:

- Make object discovery schema-aware and provider-aware.
- Exclude system schemas, views, migration history where appropriate, and
  protected database names.
- Add live disposable-database tests for every provider.

Acceptance gate:

- The drop command cannot destroy an unintended system database or unsupported
  object type.

### 28. Finish CLI design-time loading and project discovery

Problem: Migration generation depends on loading user assemblies correctly.

Evidence:

- CLI uses a custom `AssemblyLoadContext` and `AssemblyDependencyResolver`.
- Real projects have multiple target frameworks, external dependencies,
  startup projects, appsettings, and environment-specific configuration.

Work:

- Support explicit project, startup project, assembly, deps, runtimeconfig, and
  target framework selection where needed.
- Test design-time factory discovery with external dependencies.
- Make errors actionable and redact connection strings.

Acceptance gate:

- `norm migrations add` works against realistic application assemblies, not just
  simple test assemblies.

### 29. Decide whether scaffolding is preview or v1-stable

Problem: Scaffolding is advertised but marked preview.

Evidence:

- README says scaffolding is preview in v1.
- `docs/scaffolding.md` says relationship and index generation remain explicit
  post-processing.
- Public scaffolding APIs exist.

Work:

- Either keep scaffolding clearly preview and exclude it from compatibility
  guarantees, or stabilize reverse engineering for tables, columns, indexes,
  FKs, nullability, keys, and relationships.
- Ensure generated C# compiles and follows naming/escaping rules.
- Define provider-specific type mapping.

Acceptance gate:

- Scaffolding status is unambiguous in API docs, CLI help, README, and package
  metadata.

### 30. Stabilize temporal versioning and schema side effects

Problem: Temporal versioning creates schema objects during context
initialization.

Evidence:

- `DbContextOptions.EnableTemporalVersioning()` exists.
- Docs describe nORM-managed history tables and triggers rather than native
  provider temporal tables.

Work:

- Prove bootstrap idempotency, cancellation cleanup, migration interaction,
  provider DDL validity, rollback behavior, and permission requirements.
- Document how applications own generated history schema.
- Ensure temporal initialization does not surprise ordinary read-only contexts.

Acceptance gate:

- Temporal versioning is safe to enable deliberately and impossible to trigger
  accidentally.

### 31. Keep AOT and trimming boundaries honest

Problem: nORM is JIT-first but ships source-generation support, which can create
confusion about NativeAOT support.

Evidence:

- Docs explicitly say AOT/trimming are unsupported for v1.
- Runtime paths use reflection, dynamic materialization, and dynamic scaffolding.

Work:

- Maintain publish tests that prove unsupported dynamic paths fail with clear
  diagnostics.
- Annotate public APIs that require dynamic code or unreferenced code.
- Ensure package docs and README do not imply broad AOT support.

Acceptance gate:

- AOT/trimming behavior is verified by publish diagnostics, not just prose.

### 32. Freeze interceptor contracts

Problem: Interceptors are public extensibility points that can alter commands and
observe save behavior.

Evidence:

- Public command and save interceptors exist.
- Docs define ordering, suppression, mutation, and failure behavior.

Work:

- Cover raw SQL, stored procedures, compiled queries, source-generated queries,
  retries, transactions, cancellation, bulk operations, and suppression.
- Define which command mutations are supported.
- Keep sensitive data redacted in interceptor logging paths.

Acceptance gate:

- Interceptor behavior is stable enough for third-party integrations.

### 33. Normalize public exception taxonomy

Problem: Public paths still risk leaking `InvalidOperationException`,
`NotSupportedException`, `NotImplementedException`, or raw provider exceptions
where stable nORM exceptions should apply.

Evidence:

- Source still contains public or provider-facing unsupported paths using
  `NotSupportedException` and `NotImplementedException`.
- Docs define `NormUnsupportedFeatureException`, `NormUsageException`,
  `NormConfigurationException`, `NormDatabaseException`, and related types.

Work:

- Audit every public API for expected failure modes.
- Map unsupported features, usage errors, configuration errors, timeouts,
  provider failures, and concurrency conflicts to stable exception types.
- Preserve raw provider exceptions only where explicitly documented.

Acceptance gate:

- Users can catch stable nORM exception categories for expected failures.

### 34. Complete logging and redaction coverage

Problem: Diagnostics touch SQL, parameters, connection strings, CLI output,
interceptors, benchmark artifacts, and release manifests.

Evidence:

- Docs and tests exist for redaction.
- Release scripts and CLI commands handle connection strings and provider
  configuration.

Work:

- Audit every log path for SQL literals, parameter values, connection strings,
  environment variables, exception messages, benchmark output, and artifacts.
- Keep sensitive-data logging as an explicit application-owned opt-in.
- Add redaction tests for new CLI and release-manifest paths.

Acceptance gate:

- Normal diagnostics cannot leak secrets.

### 35. Prove ConnectionManager high-availability behavior under load

Problem: `ConnectionManager` is public production infrastructure, not a simple
helper.

Evidence:

- `ConnectionManager` handles topology, primary/write selection, read replicas,
  health checks, and background disposal.
- README separately explains provider-native pooling.

Work:

- Stress read/write failover, health-check failures, background disposal,
  replica churn, concurrent callers, logger behavior, and cancellation.
- Document that it creates provider connections and relies on provider-native
  pooling.
- Define operational limits.

Acceptance gate:

- ConnectionManager behavior remains correct under concurrent failover and
  disposal stress.

### 36. Define retry and adaptive timeout production policy

Problem: Retries and adaptive timeouts can change transactional semantics if
misapplied.

Evidence:

- Public `RetryPolicy`, execution strategies, and `AdaptiveTimeoutManager`
  exist.
- Retry and timeout behavior interacts with transactions, interceptors, raw SQL,
  stored procedures, and cancellation.

Work:

- Define which operations are retryable and which are never retried.
- Ensure retries do not repeat non-idempotent operations inside external
  transactions.
- Document timeout budgets and adaptive behavior.

Acceptance gate:

- Retry and timeout policies are safe by default and explicit when risky.

### 37. Rebuild benchmark fairness and dependency parity

Problem: Performance claims are central to nORM, but benchmark evidence must be
release-grade.

Evidence:

- Benchmark governance docs exist.
- Benchmarks compare EF Core, Dapper, Raw ADO.NET, and nORM.
- Benchmark package versions differ from tests/runtime for some dependencies.

Work:

- Align benchmark dependency versions with the release support matrix or
  document why they differ.
- Verify equivalent SQL, row counts, projections, compiled/prepared modes, and
  materialization semantics.
- Separate Raw ADO convenience, optimized, and prepared-optimized baselines.

Acceptance gate:

- Every public performance claim maps to raw BenchmarkDotNet output from the
  release commit.

### 38. Make benchmark thresholds credible and enforced

Problem: Current benchmark budgets are useful guardrails but not yet release
evidence.

Evidence:

- `eng/benchmark-thresholds.json` defines ratio budgets.
- RC gate can run benchmark evidence and threshold scripts.

Work:

- Run the full provider matrix on controlled release hardware.
- Tighten thresholds from provisional budgets to release-candidate baselines.
- Require explicit override notes for skipped benchmark jobs.

Acceptance gate:

- Performance regressions fail release automation, and skipped benchmarks block
  performance claims.

### 39. Reduce test-suite entropy and prove reliability

Problem: The test suite is large and valuable, but size alone is not release
confidence.

Evidence:

- The repo has thousands of test declarations and many specialized regression
  files.
- No skipped xUnit tests were found in this audit.
- A focused contract-test command timed out locally.

Work:

- Split slow tests from fast gates deliberately.
- Track flaky, stress, live-provider, benchmark, and package-consumer tests with
  clear ownership.
- Keep coverage-boost style tests from becoming the default pattern for new
  work.

Acceptance gate:

- CI gates are reliable, understandable, and fast enough to run consistently.

### 40. Run and publish a real RC gate from the release commit

Problem: v1 is not credible until release evidence exists.

Evidence:

- `eng/v1-release-gate.ps1` supports `quick`, `live`, `full`, and `rc`.
- `.github/workflows/v1-rc.yml` exists and can run the RC gate.
- No successful full RC artifact manifest was produced during this audit.

Work:

- Run `eng/v1-release-gate.ps1 -Mode rc -MinLiveProviders 3` from the candidate
  commit with benchmarks enabled.
- Upload TRX, packages, benchmark artifacts, provider configuration summary,
  SDK/OS metadata, and RC manifest.
- Review the manifest before tagging.

Acceptance gate:

- The v1.0 tag points at the exact commit validated by the RC manifest.

## Execution Order

1. API/package/docs freeze: blockers 1-6.
2. Query semantics: blockers 7-14.
3. Security and correctness: blockers 15-24.
4. Tooling and schema features: blockers 25-31.
5. Extensibility and operations: blockers 32-36.
6. Benchmarks, tests, and release evidence: blockers 37-40.

## Closure Rule

Do not mark any item complete from prose alone. Closure requires at least one
hard artifact:

- code change with tests,
- generated docs or package output,
- CI or release-gate run,
- live-provider evidence,
- raw benchmark artifacts,
- or a deliberate "not in v1" decision reflected in public docs and API shape.
