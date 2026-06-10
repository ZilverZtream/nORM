# nORM v1.0 Blocker Developer Spec

Date: 2026-06-10

This spec is a fresh v1.0 readiness audit of the current working tree. It is
deliberately strict: nORM can be fast and promising without being ready for a
stable v1 API. v1.0 should mean the public contract, package shape, provider
behavior, LINQ support, release gates, and performance claims are all backed by
repeatable evidence from the release commit.

## Audit Baseline

Local commands and findings used for this revision:

- `dotnet build nORM.sln -c Release --nologo`
  - Passed with 0 warnings and 0 errors.
  - Produced `nORM.1.0.0-rc.3` and `dotnet-norm.1.0.0-rc.3` packages.
- `.\eng\v1-release-gate.ps1 -Mode quick -SkipBenchmark`
  - Passed after refreshing five stale `DatabaseScaffolder.cs` AOT baseline
    line entries.
  - AOT scan observed 298 IL diagnostics, all accepted by
    `eng/aot-baseline.txt` (`new=0`, `cleaned=0`).
  - Public API snapshot passed 2/2.
  - Package consumer smoke tests passed 6/6.
  - CLI smoke tests passed 70/70.
- `dotnet test tests\nORM.Tests.csproj -c Release --no-build --logger "console;verbosity=minimal"`
  - Passed: 11,450.
  - Failed: 0.
  - Skipped: 0.
- `.\eng\v1-release-gate.ps1 -Mode live -MinLiveProviders 3 -SkipBenchmark`
  - Passed with SQL Server, PostgreSQL, MySQL, and SQLite configured.
  - AOT scan observed 298 IL diagnostics, all accepted by
    `eng/aot-baseline.txt` (`new=0`, `cleaned=0`).
  - Public API snapshot passed 2/2.
  - Package consumer smoke tests passed 6/6.
  - CLI smoke tests passed 70/70.
  - Live provider gate passed 1,925/1,925.
  - Wrote `artifacts/v1-rc/rc-artifacts.json` and
    `artifacts/v1-rc/rc-artifacts.md` in `live` mode with
    `BenchmarkSkipped=true` and `WorkingTreeClean=false`.
- Test categorization scan:
  - 969 test source files after removing four stale merge-placeholder files.
  - 962 files contain `[Trait(...)]` or `[Xunit.Trait(...)]`.
  - The remaining 7 files are shared helper or xUnit collection-definition
    infrastructure with no `[Fact]` or `[Theory]` methods.
  - `TestCategoryHygieneTests` enforces that every public test class declaring
    `[Fact]` or `[Theory]` has an allowed v1 category.
  - `v1-release-gate.ps1` package-consumer smoke tests now use
    `Category=PackageConsumer`.
  - The live provider gate now starts from `Category=LiveProvider` and appends
    `Category=ProviderParity`; discovered test coverage is
    1,929 tests versus 1,925 in the previous name-only filter, with no previous
    tests dropped.
- Public API snapshot:
  - `tests/PublicApi.Shipped.txt` has 1,648 lines.
  - Public surface includes `nORM.Internal.ConcurrentLruCache<T, TValue>` and
    `nORM.Internal.ParameterOptimizer`.
  - Public additions include `RenameColumnAttribute`,
    `ColumnSchema.PreviousName`, `SchemaDiff.RenamedColumns`,
    provider constructors accepting `IDbParameterFactory`, and
    `CompiledMaterializerStore.AddPermanent<T>`.
- Live provider evidence:
  - SQL Server, PostgreSQL, MySQL, and SQLite were configured for the live v1
    gate.
  - The `live` artifact manifest records all four providers as configured.
  - This is local dirty-working-tree correctness evidence, not final release
    evidence from a clean tag.
- Benchmark evidence:
  - No fresh BenchmarkDotNet provider matrix was run in this audit.
  - The current `live` artifact manifest was produced with `-SkipBenchmark`,
    so it must not be used to support public performance claims.
- Documentation evidence:
  - `docs/scaffolding.md`, `README.md`, and `src/dotnet-norm/README.md` now
    present scaffolding as a bounded stable v1 tooling surface.
  - `ScaffoldingContractDocTests` pins the scaffold contract, CLI option
    inventory, README wording, and release-gate coverage.

## v1.0 Release Bar

v1.0 requires:

- A clean release branch with intentional public API.
- Real provider evidence for SQL Server, PostgreSQL, MySQL, and SQLite.
- A bounded, executable LINQ contract rather than a broad "full LINQ" claim.
- Stable source-generation behavior from the packed NuGet package.
- Production-grade security boundaries for tenants, raw SQL, stored
  procedures, logging, and caches.
- Stable write behavior for tracking, graph saves, transactions, migrations,
  bulk operations, and optimistic concurrency.
- Fresh benchmark artifacts from the release commit for every public
  performance claim.

## The 40 Biggest Blockers

### 1. Clean and Freeze the Release Branch

Problem: The repository is heavily dirty, including source, tests, generated
API docs, release scripts, and the blocker spec itself. A stable release cannot
be cut from an ambiguous working tree.

Work:

- Split product fixes, generated docs, tests, and audit/spec changes into
  separate reviewed commits.
- Ensure generated outputs are either reproducible tracked files or ignored
  artifacts.
- Require `git status --short` to be clean before tagging.

Acceptance gate:

- The final v1 tag points at a clean commit whose release artifacts were
  produced from that exact commit.

### 2. Make the Quick Gate Reliable and Bounded

Problem: Quick mode now passes, but v1 still needs proof that it is
consistently bounded and self-cleaning across repeated runs instead of passing
only after manual cleanup.

Work:

- Add process cleanup and file-lock diagnostics around test and package steps.
- Make package-producing tests use isolated output paths or clear package
  outputs safely before packing.
- Ensure interrupted gate runs do not poison the next run.

Acceptance gate:

- Repeated `.\eng\v1-release-gate.ps1 -Mode quick -SkipBenchmark` runs pass
  back-to-back without manual process cleanup.

Current status:

- Closed for the current working tree. Quick mode cleans package outputs and
  orphaned test hosts before running.
- `eng/v1-release-gate.ps1` has `-TestStepTimeoutMinutes` /
  `NORM_TEST_STEP_TIMEOUT_MINUTES`, defaults to 45 minutes per `dotnet test`
  invocation, kills timed-out test process trees, and reports stdout/stderr log
  paths plus recent log tails.
- A controlled `-TestStepTimeoutMinutes 1` quick-gate run proved successful
  test-step exit-code handling on public API/package smoke tests and timed out
  `CLI smoke tests` with no repo-scoped `dotnet` processes left behind.
- A native back-to-back quick run passed without manual cleanup:
  `quick-repeat-native-20260610-025156-run1.log` passed at
  `2026-06-10T02:53:48+02:00`, `quick-repeat-native-20260610-025156-run2.log`
  passed at `2026-06-10T02:55:26+02:00`, and
  `quick-repeat-native-20260610-025156.exitcode` recorded `0`.

### 3. Fix the Full Local Test Contract

Problem: The full local test command must stay usable on machines without SQL
Server, PostgreSQL, or MySQL. Live-provider tests need a consistent no-provider
policy, and live/RC gates still need to fail before tests when the configured
provider minimum is not met.

Work:

- Keep local non-live runs trait-filtered with `Category!=LiveProvider`.
- Keep unconfigured live-provider test bodies on the shared `Skip.If`
  early-return policy instead of runtime skip exceptions.
- Keep RC mode strict: missing required live providers must fail before tests
  run.

Acceptance gate:

- Local full non-live tests pass without SQL Server, PostgreSQL, or MySQL.
- `live` and `rc` modes fail early when their configured live-provider minimum
  is not met.

Current status:

- This blocker is closed for the current working tree.
- `dotnet test tests/nORM.Tests.csproj -c Release --no-build --filter
  "Category!=LiveProvider"` passed 10,107/10,107 with
  `NORM_TEST_SQLSERVER`, `NORM_TEST_POSTGRES`, `NORM_TEST_MYSQL`, their `_CS`
  aliases, `NORM_REQUIRE_LIVE_PARITY`, and `NORM_MIN_LIVE_PROVIDERS` cleared.
- Focused no-provider live-shape/hygiene coverage passed 27/27 after moving the
  shared `Skip.If` helper to `tests/LiveProviderSkip.cs`.
- `v1-release-gate.ps1 -Mode live -MinLiveProviders 3 -SkipBenchmark` and
  `v1-release-gate.ps1 -Mode rc -SkipBenchmark` both failed before tests with
  `v1 gate requires 3 live provider(s), but only 0 are configured`.
- `TestCategoryHygieneTests` now rejects runtime `SkipException` usage in
  `src/` and `tests/` so the no-provider contract cannot regress silently.
- The full local test suite also previously passed 11,450/11,450 with the
  configured live providers available, and the live gate previously passed with
  SQL Server, PostgreSQL, MySQL, and SQLite coverage.

### 4. Require Real Live Provider Release Evidence

Problem: SQLite and dialect-shape tests cannot prove SQL Server, PostgreSQL,
or MySQL behavior for row counts, DDL, transactions, savepoints, JSON, identity
retrieval, bulk operations, and type conversion.

Work:

- Run and store live provider results for SQL Server, PostgreSQL, MySQL, and
  SQLite from the RC commit.
- Separate dialect-only tests from real-server tests.
- Make RC mode require the configured minimum live providers, normally all
  non-SQLite providers.

Acceptance gate:

- RC evidence includes real-server pass results for every supported provider
  and every provider-specific contract.

Current status:

- Local `live` mode now has real SQL Server, PostgreSQL, MySQL, and SQLite
  correctness evidence: 1,925/1,925 live provider tests passed with
  `-MinLiveProviders 3`.
- This is not final RC evidence because the working tree is dirty and
  benchmarks were skipped.

### 5. Validate Provider Capability Floors

Problem: Provider minimum versions are now documented and exposed at runtime,
but v1 still needs proof that every advertised feature works at those floors.

Work:

- Verify SQL Server 2016, PostgreSQL 12, MySQL 8.0, and SQLite 3.25 or raise
  the floor.
- Test JSON, window functions, generated identity, rename column, savepoints,
  UPSERT/ON CONFLICT, RETURNING/OUTPUT, and temporal/versioning features.
- Keep docs and `ProviderCapabilities.MinimumServerVersion` mechanically in
  sync.

Acceptance gate:

- Every documented provider floor is proven by live tests or explicitly raised.

Current status:

- This blocker is closed for the current working tree.
- `ProviderCapabilityContractTests` already keeps
  `docs/provider-capabilities.md` minimum-version rows in sync with
  `ProviderCapabilities.MinimumServerVersion`.
- `docs/provider-capabilities.md` now includes an explicit floor-feature
  evidence table for SQL Server 2016, PostgreSQL 12, MySQL 8.0, and SQLite
  3.25, covering JSON, window functions, generated-value retrieval,
  rename-column DDL, savepoints, idempotent insert/ignore semantics,
  temporal/versioning, native bulk, and native tenant session support where
  applicable.
- `eng/rc-artifact-manifest.ps1` records the same declared floor-feature ledger
  in `rc-artifacts.json`/`rc-artifacts.md` without pretending to capture actual
  server versions.
- `dotnet-norm portability certify` live target probing now exercises
  representative floor-gated features instead of only checking JSON:
  JSON translation, `ROW_NUMBER` window translation, generated-value retrieval,
  rename-column DDL, savepoints, and idempotent insert/ignore semantics.
- `artifacts/v1-rc/provider-target-capabilities.json` and
  `artifacts/v1-rc/provider-target-capabilities.html` passed on `2026-06-10`
  with actual target versions SQLite `3.41.2`, SQL Server `16.0.1000`,
  PostgreSQL `17.5`, and MySQL `8.0.46`, all at or above the documented
  provider floors. The report has no `provider-target-capability` errors.

### 6. Freeze the Public API Surface

Problem: The public API is large and includes types that look internal,
provider-internal, or preview-like. Once v1 ships, all public members become a
compatibility promise.

Work:

- Classify every public type as stable user API, stable provider API, stable
  tooling API, or explicitly out of v1.
- Move or rename `nORM.Internal.*` public types, or intentionally document and
  support them as a compatibility exception with a future relocation path.
- Add support-tier documentation and tests for every exported namespace.

Acceptance gate:

- Every entry in `tests/PublicApi.Shipped.txt` has an intentional support tier
  and matching docs.

Current status:

- `docs/namespace-policy.md` defines the approved public namespaces and closes
  `nORM.Internal` to new public additions.
- `NamespacePolicyContractTests` enforces both the approved namespace list and
  the grandfathered internal type list.
- `docs/public-api-policy.md` now classifies
  `nORM.Internal.ConcurrentLruCache<TKey, TValue>` and
  `nORM.Internal.ParameterOptimizer` as v1.0 compatibility surface in a
  deprecated namespace, with v1.x relocation targets documented in
  `docs/namespace-policy.md`.
- `PublicApiClassificationTests` now derives its support-tier map from
  `docs/namespace-policy.md`, verifies every non-comment entry in
  `tests/PublicApi.Shipped.txt` maps to a documented namespace tier, and fails on
  missing or stale policy rows. The current shipped baseline has 1,648 lines.
- This blocker is closed for the current working tree. The final release branch
  still reruns the public API gate, but the current contract contradiction is
  enforced mechanically instead of depending on a stale in-test registry.

### 7. Review Recent Public API Additions

Problem: New public APIs such as rename metadata, parameter factory
constructors, and permanent source-generated materializer registration are
v1-significant decisions.

Work:

- Review naming, mutability, exceptions, nullability, XML docs, and versioning
  impact.
- Add contract tests for every new public member.
- Document why each addition is stable and who should use it.

Acceptance gate:

- Public API snapshot changes are approved with docs and tests before v1.

Current status:

- `docs/public-api-policy.md` lists the recent v1 additions with test and
  documentation evidence.
- The `nORM.Internal` compatibility entries are now included in that table and
  pinned by `NamespacePolicyContractTests` plus `InternalPublicApiTests`.
- This blocker remains open for final release review of naming, XML docs,
  mutability, nullability, and exception contracts across the full public API
  baseline.

### 8. Regenerate and Verify API Documentation

Problem: Generated DocFX files are modified, and API docs must exactly match
the release assembly.

Work:

- Regenerate DocFX metadata from the Release build.
- Add a test that every public type has a generated page or explicit
  exclusion.
- Mark preview or advanced APIs consistently in XML docs and generated docs.

Acceptance gate:

- Generated API docs are reproducible and mechanically checked against the
  shipped assembly.

### 9. Finalize Package Architecture

Problem: `nORM` ships core ORM code, provider dialects, SQL Server and SQLite
drivers, ScriptDom, source generators, and reflection-loaded PostgreSQL/MySQL
support in one package. That may be acceptable, but it must be intentional.

Work:

- Decide monolithic package versus `nORM.Core` plus provider packages.
- If monolithic, document dependency cost and prove every provider consumer
  story from the package.
- If split, create provider packages and prove independent consumption.

Acceptance gate:

- Package layout is stable, documented, and verified by package-consumer tests.

### 10. Stabilize Package Artifact Hygiene

Problem: Package tests can fail on locked `.nupkg` files, and stable v1 package
version transitions are not yet proven.

Work:

- Isolate package test outputs per run.
- Reject stale packages before packing.
- Define transitions from `1.0.0-rc.*` to `1.0.0` and post-v1 development.

Acceptance gate:

- Final package outputs contain only the expected `nORM.1.0.0.*` and
  `dotnet-norm.1.0.0.*` artifacts.

### 11. Make Test Categories Real

Problem: Test-class categorization is now enforced and the v1 gate uses
category-first package, live, provider-parity, and RC-loop filters. This closes
the previous name-pattern routing risk for the release gate.

Work:

- Keep every test class categorized as Fast, LiveProvider, PackageConsumer,
  Stress, ProviderParity, or another documented release-gate category.
- Keep release-gate filters category-first.
- Keep hygiene tests failing on uncategorized or undocumented test categories.

Acceptance gate:

- Quick, full, live, stress, and RC gates run predictable, documented test
  sets.

Current status:

- `TestCategoryHygieneTests` enforces explicit categories on every public test
  class that declares xUnit `[Fact]` or `[Theory]` methods.
- Four stale merge-placeholder `.cs` files were removed; the remaining
  category-less files are helper/collection infrastructure.
- Package-consumer smoke tests now run by `Category=PackageConsumer`.
- The live provider gate now starts with `Category=LiveProvider` and keeps
  supplemental provider-parity evidence under `Category=ProviderParity`. The
  category filter preserves the previous 1,925 discovered tests and adds four
  categorized `TenantTemporalProviderSwapTests`.
- RC-loop navigation, transaction, compiled-query, provider/source-generation
  parity, bulk/provider parity, migration parity, cache-memory, and
  concurrency/adversarial filters now use documented categories. Discovery
  comparison matched the previous name-based filters exactly: 26 navigation
  tests, 55 transaction tests, 26 compiled-query tests, 97 provider/source-gen
  parity tests, 424 bulk/provider parity tests, 219 migration tests, 16
  cache-memory tests, and 768 concurrency/adversarial tests.
- This blocker is closed for the current working tree. It still needs the
  normal clean-branch release evidence required by blocker 1 before v1 tagging.

### 12. Turn the LINQ Matrix Into Executable Evidence

Problem: `docs/linq-support-coverage.md` maps features to test files, not to
provider, query path, SQL shape, result shape, and expected failure behavior.

Work:

- Convert the LINQ matrix into table-driven tests.
- Track provider, execution path, sync/async, compiled/non-compiled,
  source-generated/runtime, expected SQL shape, expected result, and expected
  exception.
- Generate docs from that data or test docs against it.

Acceptance gate:

- Every supported or constrained LINQ row is backed by executable provider
  evidence.

### 13. Close Common LINQ Gaps or Document Them Explicitly

Problem: Users hear "full LINQ" and expect common operators beyond the current
matrix. Missing or partial operators must be implemented or explicitly rejected
with stable exceptions.

Work:

- Audit common `Queryable` operators: `Last`, `LastOrDefault`, `Reverse`,
  `DefaultIfEmpty`, left join patterns, `ElementAt`, `ElementAtOrDefault`,
  `Contains` over local collections, `SequenceEqual`, `Concat`, `ToArray`,
  `ToDictionary`, `TakeWhile`, `SkipWhile`, `OfType`, and cast/type filters.
- Implement high-value operators where provider SQL is straightforward.
- Document unsupported operators with deterministic `NormUnsupportedFeatureException`.

Acceptance gate:

- The LINQ support page contains no vague "full LINQ" implication and every
  common operator has an implemented or explicitly unsupported v1 decision.

### 14. Harden Terminal Operator Semantics

Problem: Terminal operators are duplicated across normal, fast-path, compiled,
sync helper, async, and generated paths.

Work:

- Test `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last`,
  `LastOrDefault`, `Any`, `All`, `Count`, and `LongCount` for empty, one-row,
  two-row, ordered, unordered, filtered, skipped, and paged inputs.
- Compare behavior to LINQ-to-Objects where applicable.
- Fix path divergence instead of documenting accidental differences.

Acceptance gate:

- Terminal operators match LINQ semantics on every advertised execution path.

### 15. Prove Scalar, Null, Boolean, Enum, and Conversion Parity

Problem: Provider differences in scalar semantics are a common ORM failure
source.

Work:

- Matrix-test nullable columns, null-safe equality, booleans, enums, decimals,
  GUIDs, byte arrays, DateOnly/TimeOnly, DateTime precision, aggregates, and
  JSON value conversion.
- Include both result materialization and parameter binding.
- Run the matrix live against all providers.

Acceptance gate:

- Scalar behavior is consistent at the nORM API boundary across providers.

### 16. Stabilize Include, ThenInclude, Split Query, and Lazy Loading

Problem: Relationship loading touches query translation, materialization,
identity maps, tenant filters, composite keys, and tracking.

Work:

- Prove one-to-one, one-to-many, many-to-many, owned references, owned
  collections, composite keys, and filtered parents.
- Verify tenant isolation and identity-map behavior for every include path.
- Define unsupported relationship shapes with stable exceptions.

Acceptance gate:

- Relationship loading is predictable and documented for every supported shape.

### 17. Harden GroupBy, GroupJoin, SelectMany, and Set Operations

Problem: These operators are high-risk because SQL grouping and correlated
collection semantics do not map cleanly to arbitrary LINQ.

Work:

- Define exactly which grouped projections are server-side.
- Bound `GroupJoin` memory and cardinality behavior.
- Test `SelectMany`, `Union`, `Intersect`, `Except`, and `Concat` where
  supported across providers.

Acceptance gate:

- Complex LINQ either translates correctly or fails before accidental
  client-side semantic drift.

### 18. Make Client Evaluation Impossible to Misuse

Problem: Client evaluation can accidentally pull unbounded data or bypass
tenant filters.

Work:

- Keep default policy as Throw.
- Ensure Warn/Allow only apply after server-side filters, tenant filters,
  paging, and safe projection boundaries.
- Log shape and risk without leaking values.

Acceptance gate:

- Client evaluation cannot run before required server restrictions.

### 19. Prove Compiled Query Shape Parity

Problem: Compiled queries are central to performance claims but use separate
translation, cache, parameter binding, terminal, and materialization paths.

Work:

- Compare compiled and non-compiled SQL, parameters, cache keys, tenant
  filters, cancellation, exceptions, and result cardinality.
- Cover provider-specific syntax and real-server execution.
- Include stress tests for cache poisoning and context isolation.

Acceptance gate:

- Compiled queries are semantically identical to normal queries for every
  advertised shape.

### 20. Finish Source Generator v1 Coverage

Problem: Source generation is packed, but its supported surface is still too
small and fallback-heavy for a stable headline feature.

Work:

- Test generation from produced packages on Windows and Linux.
- Cover `[Column]`, `[RenameColumn]`, fluent rename fallbacks, owned types,
  value converters, nullability, constructors, inaccessible setters, global
  namespace types, and compile-time query diagnostics.
- Make generated output deterministic and debuggable.

Acceptance gate:

- Package consumers get correct generated code or stable diagnostics for every
  documented source-generation scenario.

### 21. Shrink the AOT and Trimming Baseline

Problem: The gate currently accepts 298 IL diagnostics. That is useful as a
regression fence, but it is not v1-ready AOT/trimming support.

Work:

- Triage every IL2026, IL2057, IL2060, IL2067, IL2070, IL2072, IL2075,
  IL2077, IL2093, IL2111, and IL3050 diagnostic.
- Annotate truly dynamic APIs with correct attributes or refactor hot paths to
  be trim/AOT safe.
- Split supported AOT surfaces from unsupported dynamic surfaces in docs.

Acceptance gate:

- The AOT baseline is either empty for supported surfaces or every remaining
  diagnostic is intentionally documented and tested.

### 22. Treat Raw SQL and Stored Procedures as Security Boundaries

Problem: Raw SQL and stored procedures bypass normal translation and tenant
injection. Users must not confuse them with automatically safe ORM queries.

Work:

- Prove read-only raw SQL validation with provider-specific syntax.
- Prove stored procedure name validation, parameter handling, cancellation,
  transaction, and logging behavior.
- Document tenant responsibility for every privileged path.

Acceptance gate:

- Privileged APIs are safe-by-default where possible and unmistakably labeled
  where caller responsibility begins.

### 23. Verify Multi-Tenancy as a Boundary

Problem: Tenant isolation spans query translation, includes, compiled queries,
caches, result caching, writes, bulk operations, raw SQL, stored procedures,
migrations, temporal features, and direct connection access.

Work:

- Build a threat-model matrix from `docs/multi-tenancy-security.md`.
- Test every execution path for tenant-aware cache keys and write coercion.
- Run tenant tests on live providers.

Acceptance gate:

- No supported ORM-generated path can leak or mutate another tenant's data.

### 24. Prove Cache Bounds and Invalidation

Problem: Performance depends on shared caches, and shared caches can leak
memory or cross-contaminate tenants, models, providers, or queries.

Work:

- Stress plan caches, result caches, materializer caches, command caches,
  dynamic type caches, and source-generated stores.
- Expose diagnostics for critical cache sizes.
- Verify invalidation after writes, bulk operations, migrations, tenant churn,
  and model churn.

Acceptance gate:

- Dynamic query and tenant churn cannot grow caches without bound or reuse
  wrong results.

### 25. Finalize Change Tracking Semantics

Problem: Change tracking is a core ORM contract, not a convenience layer.

Work:

- Document attach, detach, add, update, remove, identity maps, snapshots,
  immutable entities, constructor-bound entities, shadow properties, owned
  types, relationship fixup, notification tracking, and PK mutation.
- Add table-driven contract tests for each behavior.
- Ensure projection tracking rules are explicit.

Acceptance gate:

- Supported tracking behavior is documented and mechanically covered.

### 26. Prove SaveChanges Graph Ordering and Cascade Behavior

Problem: Graph save bugs cause foreign-key failures or data corruption.

Work:

- Live-test inserts, updates, deletes, cascades, owned collections,
  many-to-many, composite keys, generated keys, batching, and rollback.
- Define cascade and ownership semantics clearly.
- Keep batching observable and provider-safe.

Acceptance gate:

- Complex graph writes are atomic, ordered, and consistent across providers.

### 27. Lock Optimistic Concurrency Across Write Paths

Problem: Normal writes, direct writes, bulk updates, bulk deletes, nullable
tokens, composite keys, and provider affected-row semantics can diverge.

Work:

- Funnel conflict detection through shared row-count/token logic where
  possible.
- Keep MySQL matched-row semantics strict by default or document explicit
  opt-in weakening.
- Verify live provider behavior for same-value updates and nullable tokens.

Acceptance gate:

- Every write path detects concurrency conflicts consistently under default
  configuration.

### 28. Finish Transaction, Savepoint, Ambient, Retry, and Interceptor Parity

Problem: Transactions cross-cut queries, writes, bulk operations, migrations,
raw SQL, stored procedures, retries, and interceptors.

Work:

- Live-test explicit transactions, ambient `TransactionScope`, savepoints,
  rollback, retry policies, cancellation, and interceptor mutation/suppression.
- Surface unsupported savepoint cases as stable nORM exceptions with precise
  reasons.
- Document provider-specific transaction limits.

Acceptance gate:

- Transaction behavior is predictable for every provider and execution path.

### 29. Complete Cancellation and Timeout Audits

Problem: Cancellation can leave commands, readers, temp tables, transactions,
or connection state inconsistent.

Work:

- Audit every async public API for cancellation propagation.
- Verify cleanup after cancellation in migrations, temporal bootstrap, bulk
  operations, interceptors, command pools, and readers.
- Normalize timeout behavior with retry policy.

Acceptance gate:

- Cancellation either cleans up or reports a documented recoverable state.

### 30. Normalize Public Exception Taxonomy

Problem: Public paths still expose raw `ArgumentException`,
`InvalidOperationException`, `NotSupportedException`, provider exceptions, and
nORM exceptions inconsistently.

Work:

- Audit every expected public failure mode.
- Map usage, configuration, query, timeout, provider, concurrency, validation,
  and unsupported-feature failures to stable `Norm*Exception` categories where
  appropriate.
- Keep raw provider exceptions only on documented escape hatches.

Acceptance gate:

- Users can catch stable nORM exception categories for every expected public
  failure.

### 31. Harden Migrations, Rename, Recovery, and Idempotency

Problem: Migration correctness is provider-specific and failure-prone.

Work:

- Live-test create, alter, drop, index, default, FK, rename, and history-table
  operations.
- Fault-inject failures and verify recovery state.
- Align `[RenameColumn]`, `PreviousName`, generated SQL, docs, and CLI
  behavior.

Acceptance gate:

- Failed migrations leave a known, documented, recoverable state for every
  provider.

### 32. Harden Destructive CLI Database Drop

Problem: `norm database drop` is intentionally destructive and must be harder
to misuse than ordinary commands.

Work:

- Prove schema-aware object discovery on live providers.
- Exclude system schemas and protected databases.
- Require dry-run output precise enough for review before destructive use.

Acceptance gate:

- The command cannot destroy unintended system objects under supported
  configurations.

### 33. Finish CLI Design-Time Loading

Problem: Migration generation depends on loading real application assemblies,
not just simple smoke-test projects.

Work:

- Support project, startup project, assembly, deps, runtimeconfig, target
  framework, configuration, and environment selection.
- Test external dependencies and multi-targeted projects.
- Redact connection strings and config values in every error path.

Acceptance gate:

- `dotnet-norm migrations add` works against realistic application layouts.

Current status:

- This blocker is closed for the current working tree. `migrations add` now resolves `--project` /
  `--startup-project` outputs with `--configuration`, `--runtime`, and
  `--target-framework`/`--framework` passed through MSBuild evaluation instead
  of defaulting silently to Debug/no-RID output.
- Explicit `--deps` and `--runtimeconfig` paths now fail fast when missing;
  explicit `.deps.json` managed and native runtime assets are dependency
  candidates instead of ignored command-line values, including nested
  deps-file-directory assets, runtimeconfig `additionalProbingPaths`, and
  matching assets under the configured/global NuGet package cache.
- `--environment` is passed to `INormDesignTimeDbContextFactory<TContext>` and
  temporarily exposed through `ASPNETCORE_ENVIRONMENT` and
  `DOTNET_ENVIRONMENT` while the snapshot is built.
- Focused CLI integration coverage now proves `--framework` on a multi-targeted
  project and `--startup-project` winning over an unbuilt target project for the
  design-time host.
- Design-time factory constructor and `CreateDbContext` failures are normalized
  with the failing factory type, keep the underlying failure reason, and redact
  connection-string secrets in CLI stderr.
- The broader CLI design-time/integration gate passed 91/91:
  `dotnet test tests/nORM.Tests.csproj -c Release --filter
  "FullyQualifiedName~CliDesignTimeTests|FullyQualifiedName~CliIntegrationTests"`.

### 34. Resolve the Scaffolding Contract Contradiction

Problem: This blocker existed because `docs/scaffolding.md` called scaffolding
stable v1 while later saying the `dotnet-norm scaffold` command was preview.
The current docs now resolve that contradiction as a bounded stable v1 tooling
surface.

Work:

- Decide whether scaffolding is stable v1 or preview.
- If stable, finish relationship/index/FK introspection, deterministic output,
  reserved identifiers, schemas, unsigned types, JSON, computed columns, and
  live provider round-trips.
- If preview, remove or clearly mark unsupported API/tooling expectations.

Acceptance gate:

- Runtime API, CLI help, README, generated API docs, and tests all state one
  coherent scaffolding contract.

Current status:

- This blocker is closed for the current working tree.

Resolution note:

- Current v1 scaffolding is a bounded stable tooling surface, not preview.
  `docs/scaffolding.md`, `README.md`, and `src/dotnet-norm/README.md` define
  the same contract: base-table entity scaffolding, schema preservation for SQL
  Server/PostgreSQL/SQLite attached databases, MySQL catalog-portable metadata,
  nullable-safe generated code, provider metadata-backed identity columns,
  computed/generated and rowversion metadata, deterministic generated output,
  explicit SQL Server/PostgreSQL primary-key constraint names without
  system/default-name noise, provider-native table/column comments,
  SQL Server/PostgreSQL/MySQL routine
  comments, SQL Server/PostgreSQL sequence comments, SQL Server local-synonym
  comments, and SQL Server/PostgreSQL view/materialized-view query-artifact
  comments as generated XML documentation, SQLite rowid key normalization,
  static/dynamic required metadata parity,
  single-column and composite FK navigations when the FK targets the generated
  principal primary key or an exact ordered unfiltered unique index, one-to-one
  reference navigations for exact unique dependent FKs, cascade/non-cascade
  delete preservation, pure many-to-many join mappings including
  schema-qualified and self-referencing role-named join tables, index
  metadata, warning reports, and `--fail-on-warnings`.
- Warning reports are structured evidence, not prose dumps: JSON rows include
  stable diagnostic codes, severity, category, suggested actions, section
  counts, and code/category summaries; stale warning reports are removed or
  rejected deterministically when a later scaffold has no diagnostics.
- Unsupported or non-entity database shapes are reported instead of silently
  modeled: non-scaffoldable composite FKs, payload join tables,
  alternate-key/keyless-principal relationships, provider defaults/computed expressions/check constraints/
  collations/provider column types/precision, non-default identity settings,
  unrecognized/provider-specific FK referential action tokens, triggers, SQL Server provider-native
  temporal tables, keyless tables, SQLite virtual tables and shadow tables,
  views, routines, sequences, synonyms, materialized views, and events.
- Evidence lives in `ScaffoldingAndNavigationCoverageTests`,
  `ScaffoldingContractDocTests`, `LiveProviderScaffoldingParityTests`,
  CLI integration scaffolding tests, and package consumer compile tests.

### 35. Stabilize Temporal Versioning Side Effects

Problem: Temporal versioning creates provider-specific schema objects during
application use.

Work:

- Prove bootstrap idempotency, permissions, rollback, cancellation cleanup,
  migration interaction, and live DDL validity.
- Document ownership of history tables, triggers, functions, and tag tables.
- Ensure temporal enablement is deliberate and observable.

Acceptance gate:

- Temporal versioning is safe to enable deliberately and cannot trigger
  surprising schema writes.

### 36. Freeze Interceptor Contracts

Problem: Interceptors are public extensibility points that can observe, mutate,
or suppress command execution.

Work:

- Cover normal queries, compiled queries, source-generated queries, raw SQL,
  stored procedures, writes, bulk operations, migrations, retries,
  transactions, failures, and cancellation.
- Define allowed command mutations and ordering.
- Verify redaction boundaries for intercepted commands.

Acceptance gate:

- Interceptor behavior is stable enough for third-party integrations.

### 37. Prove Logging and Redaction End to End

Problem: Diagnostics can leak SQL literals, parameter values, connection
strings, environment variables, scalar results, benchmark metadata, and release
artifacts.

Work:

- Audit every log and artifact path.
- Keep sensitive-data logging as explicit application opt-in.
- Add redaction tests for CLI errors, provider failures, RC manifests,
  benchmark evidence, and command logs.

Acceptance gate:

- Normal diagnostics and release artifacts do not leak secrets.

### 38. Prove ConnectionManager HA Behavior

Problem: `ConnectionManager` is public production infrastructure for topology,
read/write selection, health checks, and disposal.

Work:

- Stress read/write failover, health-check failures, replica churn, concurrent
  callers, disposal races, cancellation, and logging.
- Document interaction with provider-native pooling.
- Verify behavior under transient and permanent failures.

Acceptance gate:

- ConnectionManager remains correct under concurrent failover and disposal.

### 39. Rebuild Benchmark Evidence From the Release Commit

Problem: Performance is central to the project, but no fresh benchmark matrix
was produced in this audit.

Work:

- Run the full BenchmarkDotNet provider matrix from the RC commit.
- Verify equivalent SQL shape, row counts, projections, materialization,
  prepared/compiled modes, and driver versions.
- Separate Raw ADO.NET convenience, optimized, and prepared-optimized claims.

Acceptance gate:

- Every public performance claim maps to raw BenchmarkDotNet output from the
  release commit.

### 40. Make Benchmark Thresholds and Claims Release-Grade

Problem: Threshold scripts exist, but skipped benchmarks still allow release
gates to pass and public claims can drift from evidence.

Work:

- Calibrate `eng/benchmark-thresholds.json` on controlled release hardware.
- Fail release automation on benchmark regressions.
- Block public "beats EF/Dapper/raw ADO" claims unless the exact benchmark
  artifact is present for that claim.

Acceptance gate:

- Performance regressions block release automation, and skipped benchmarks
  block performance claims.

## Suggested Execution Order

1. Stabilize the branch and gates: blockers 1-5.
2. Freeze API, docs, packages, and tests: blockers 6-12.
3. Close query, LINQ, source-generation, and AOT gaps: blockers 13-21.
4. Close security, cache, write, migration, and tooling gaps: blockers 22-35.
5. Close operations, diagnostics, HA, and performance evidence: blockers 36-40.

## Closure Rule

No blocker closes from prose alone. Closure requires at least one hard artifact:

- code change with targeted tests,
- generated docs or package output,
- passing gate output,
- live-provider evidence,
- raw benchmark artifact,
- public API review record,
- or a deliberate out-of-v1 decision reflected in docs, API shape, and tests.
