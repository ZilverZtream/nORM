# nORM v1.0 Blocker Developer Spec

Date: 2026-05-22

This spec lists the 41 largest blockers to resolve before nORM should be
treated as a stable v1.0 ORM. The current repository is already strong in local
coverage and performance intent: `dotnet build nORM.sln -c Release` passed, the
public API snapshot passed, and `dotnet test tests\nORM.Tests.csproj -c Release
--no-build --no-restore` passed 7111 tests on this machine. These items are
therefore not a claim that the project is broken; they are the remaining product,
API, packaging, correctness, operability, and evidence gates needed for v1.

## Release Bar

v1.0 means:

- A new user can install the NuGet package, follow README examples, and use every
  advertised provider without custom hidden helper types.
- The public API is intentionally frozen, documented, and supportable.
- Source generators, migrations, scaffolding, providers, CLI, and benchmarks are
  shipped as tested artifacts, not only as repository features.
- Unsupported LINQ/provider behavior is explicit, deterministic, and documented.
- Live SQL Server, PostgreSQL, MySQL, and SQLite gates pass in CI or release
  automation, including stress and benchmark gates.

## P0 Blockers

### 1. Ship the source generator in the nORM NuGet package

Problem: `src/bin/Release/nORM.0.9.0-preview.1.nupkg` contains only
`lib/net8.0/nORM.dll`, XML docs, README, and nuspec metadata. It does not contain
`analyzers/dotnet/cs/nORM.SourceGenerators.dll`, so consumers who install the
package will not get `[GenerateMaterializer]` or `[CompileTimeQuery]` generation.

Evidence:

- `src/nORM.csproj` references `nORM.SourceGenerators` as an analyzer for the
  local build.
- The packed `.nupkg` does not include the analyzer assembly.
- Source generator attributes are public under `src/SourceGeneration`.

Scope:

- Add proper analyzer packing for `nORM.SourceGenerators`.
- Add a package-consumer integration test that installs the produced package in a
  temporary project and verifies generated code compiles and runs.
- Decide whether generator attributes live in the runtime package, a separate
  abstractions package, or a build-transitive asset.

Done when:

- The package has `analyzers/dotnet/cs/nORM.SourceGenerators.dll`.
- A consumer project using only `PackageReference Include="nORM"` can use
  `[GenerateMaterializer]` and `[CompileTimeQuery]`.
- The v1 release gate validates this from the `.nupkg`, not from project
  references.

### 2. Make PostgreSQL and MySQL providers usable from the public package

Problem: README shows `new PostgresProvider()` and `new MySqlProvider()`, but the
actual public constructors require an `IDbParameterFactory`. The only concrete
Npgsql/MySQL factories are internal to `src/dotnet-norm`, while tests use a
SQLite test factory. A normal package consumer cannot follow the docs.

Evidence:

- `README.md` provider examples use parameterless constructors.
- `PostgresProvider` and `MySqlProvider` only expose constructors with
  `IDbParameterFactory`.
- `src/dotnet-norm/ParameterFactories.cs` contains internal provider factories
  that are not part of the runtime package.

Scope:

- Add public default constructors that use reflection-based provider parameter
  factories, or ship provider-specific packages such as `nORM.PostgreSQL` and
  `nORM.MySql`.
- Make the dependency story explicit: required packages, supported drivers, and
  error behavior when drivers are missing.
- Fix README and samples to match the final API.

Done when:

- PostgreSQL and MySQL quick-start code compiles in a fresh consumer app.
- Missing-driver errors are actionable and tested.
- Live provider tests instantiate providers using the same public path as users.

### 3. Fix CLI connection string handling before shipping `dotnet-norm`

Problem: `ConnectionStringValidator.ValidateAndSanitize` masks secrets and
returns the sanitized string, and `Program.cs` then uses that returned value to
connect. Any password-bearing connection string becomes invalid because the
password is replaced with `***`.

Evidence:

- `src/dotnet-norm/ConnectionStringValidator.cs` replaces password/user/token
  values with `***`.
- `src/dotnet-norm/Program.cs` uses the returned value for `CreateConnection`.

Scope:

- Split validation from redaction. Return the original validated string for
  execution, and expose a separate redacted string only for logs/errors.
- Add tests for SQL Server, PostgreSQL, MySQL, and SQLite connection strings with
  secrets.
- Ensure CLI errors never print the unredacted connection string.

Done when:

- CLI commands can connect with real password-bearing strings.
- Logs and exceptions use redacted strings.
- Tests cover both validation and redaction behavior.

### 4. Make CLI failures return non-zero exit codes

Problem: Most CLI actions catch `Exception`, print `Error: ...`, and return from
the action without setting an exit code. CI scripts and deployment systems can
therefore treat failed migrations or scaffolding as success.

Evidence:

- `src/dotnet-norm/Program.cs` catch blocks print errors in `scaffold`,
  `database update`, `database drop`, and `migrations add`.
- The root invocation returns the command invocation result, but caught errors do
  not propagate.

Scope:

- Standardize CLI error handling through a shared helper.
- Return deterministic exit codes for validation errors, database errors, no-op
  outcomes, and unexpected failures.
- Add integration tests that invoke the tool and assert exit codes.

Done when:

- Every failed CLI operation exits non-zero.
- Expected no-op states, such as "no pending migrations", have documented exit
  behavior.
- Release CI runs at least one packaged-tool smoke test.

### 5. Harden migration code generation

Problem: `migrations add` writes generated C# string literals by only replacing
double quotes. SQL containing backslashes, newlines, raw string delimiters, or
provider-specific bodies can produce uncompilable or semantically changed
migration code.

Evidence:

- `src/dotnet-norm/Program.cs` `AppendMethod` interpolates SQL into ordinary
  C# string literals.
- Temporal and provider migration SQL can contain multi-line DDL.

Scope:

- Generate raw string literals or a robust escaped representation.
- Preserve multi-statement SQL exactly.
- Add compile tests for generated migrations containing quotes, backslashes,
  newlines, PostgreSQL dollar-quoted bodies, and trigger/function DDL.

Done when:

- Generated migrations compile for all supported providers.
- Round-trip tests prove generated SQL is byte-for-byte equivalent to generator
  output.

### 6. Define and enforce the v1 public API surface

Problem: The public API baseline has 955 lines and includes low-level mapping,
query, cache, source-generation, and provider internals such as public fields on
`Column`, `TableMapping`, `JoinTableMapping`, and public cache/query helper
types. v1 will lock this surface unless it is intentionally reduced.

Evidence:

- `tests/PublicApi.Shipped.txt` has 955 entries.
- A source scan shows more than 500 public declarations in `src/nORM`.
- `docs/public-api-policy.md` says experimental surface should be internal or
  documented before v1.

Scope:

- Classify every public type/member as user API, provider API, advanced API, or
  accidental exposure.
- Make accidental exposure internal before v1.
- Replace public mutable fields with stable properties or immutable descriptors
  where user-facing access is required.
- Add API docs for all remaining public members.

Done when:

- The public API baseline is intentionally reviewed.
- No accidental internals are public.
- Public API diff review is mandatory in CI.

### 7. Stop suppressing missing XML docs for v1

Problem: `Directory.Build.props` excludes `CS1591` from warnings-as-errors. That
is reasonable pre-v1, but not for a stable public ORM with a broad API surface.

Evidence:

- `TreatWarningsAsErrors` is enabled.
- `WarningsNotAsErrors` includes `CS1591`.

Scope:

- Enable CS1591 for shipping projects or at least for public API builds.
- Add XML docs to every stable public type/member.
- Hide non-user-facing members instead of documenting accidental surface.

Done when:

- Shipping projects build with public XML documentation warnings enforced.
- DocFX output has no missing public API summaries for stable surface.

### 8. Resolve obsolete and pre-v1 compatibility debt

Problem: `DbContextOptions.CommandTimeout` is already obsolete before v1. Keeping
obsolete members in v1 creates long-lived compatibility debt; removing them after
v1 is a breaking change.

Evidence:

- `CommandTimeout` is marked `[Obsolete("Use TimeoutConfiguration.BaseTimeout
  instead")]`.

Scope:

- Decide whether obsolete members stay for v1 or are removed before freeze.
- If kept, document lifecycle and removal policy.
- Add tests around compatibility aliases if retained.

Done when:

- No pre-v1 obsolete member exists without an explicit v1 compatibility decision.

### 9. Pin the supported SDK and make builds reproducible

Problem: Local Release build used a preview .NET SDK (`NETSDK1057`). v1 artifacts
should be produced by a pinned supported SDK, and CI should fail when a preview
SDK is accidentally used.

Evidence:

- Release build output reported a preview SDK.
- No `global.json` exists in the repository root.

Scope:

- Add `global.json` pinning the supported .NET 8 SDK feature band.
- Document supported SDK/runtime matrix.
- Add CI validation for `dotnet --info`.

Done when:

- Local and CI release gates use the same supported SDK family.
- Build output no longer warns about preview SDK usage.

### 10. Align CI with the v1 release gate

Problem: `.github/workflows/ci.yml` runs build/test and separate provider jobs,
but it does not run `eng/v1-release-gate.ps1`, public API snapshot as a named
gate, package-consumer tests, stress loops, pack validation, or benchmark gates.

Evidence:

- `docs/release-gates.md` defines an RC gate that is stricter than CI.
- CI workflow has provider-specific filters, but no RC gate job.

Scope:

- Add `quick`, `live`, and scheduled/RC CI workflows that call the same scripts
  documented for release.
- Upload TRX, packages, benchmark summaries, and live provider configuration
  summaries as artifacts.
- Make release jobs branch/tag protected.

Done when:

- There is one documented command path for local and CI release validation.
- A v1 release cannot be cut without passing the RC gate.

### 11. Add package validation and package-consumer tests

Problem: `dotnet pack` succeeds, but there is no package validation, API
compatibility validation through MSBuild package validation, or smoke test that
installs the produced `.nupkg` into a fresh app.

Evidence:

- No `EnablePackageValidation`, package validation baselines, package icon,
  release notes, or repository commit metadata were found in project files.
- The source generator packaging gap would not be caught by current tests.

Scope:

- Enable NuGet/MSBuild package validation where appropriate.
- Add a consumer app test for runtime package and tool package.
- Verify source link, symbols, README, license, dependencies, analyzers, and
  package metadata.

Done when:

- The package is tested as the user installs it.
- Packaging regressions fail CI.

### 12. Decide package and dependency architecture

Problem: The main runtime package directly references SQL Server, SQLite,
ScriptDom, caching/logging/object-pool dependencies, while PostgreSQL and MySQL
drivers are reflection-based external requirements. The tool package is roughly
20 MB and carries many transitive assemblies.

Evidence:

- `src/nORM.csproj` includes Microsoft.Data.SqlClient, Microsoft.Data.Sqlite,
  ScriptDom, and support libraries.
- `dotnet-norm` package includes many provider and identity/token assemblies.
- PostgreSQL/MySQL provider constructors require external parameter factories.

Scope:

- Decide between one large batteries-included package and split provider
  packages.
- Make dependency rules consistent across providers.
- Document package size, transitive dependencies, and supported deployment
  environments.

Done when:

- A user knows exactly which package(s) to install for each provider.
- The runtime package has no accidental heavy dependencies.

## P1 Blockers

### 13. Produce a real LINQ support matrix

Problem: README advertises "Complete LINQ Support" and "full LINQ", but the code
has explicit unsupported areas: set operations, constrained `ExecuteUpdate` /
`ExecuteDelete`, async streaming restrictions, unsupported method/member
translations, and automatic client-eval fallbacks.

Evidence:

- `BulkCudBuilder.ValidateCudPlan` rejects grouped, ordered, joined, and
  aggregated CUD queries.
- `NormQueryProvider.AsAsyncEnumerable` rejects Include and GroupJoin.
- `QueryTranslator.MethodTranslators` throws for unsupported set operations and
  other LINQ shapes.
- `ExpressionToSqlVisitor` throws for unsupported members/methods.

Scope:

- Create a provider-by-provider LINQ matrix covering where/select/order/group,
  joins, group joins, select many, aggregates, contains, any/all, set ops,
  paging, projections, client-eval, includes, raw SQL composition, and bulk CUD.
- Decide which unsupported cases must be implemented for v1 and which are
  explicitly out of scope.
- Align README wording with the matrix.

Done when:

- "Full LINQ" is either true by the matrix definition or replaced by precise
  wording.
- Unsupported cases fail with stable documented exceptions.

### 14. Make client evaluation explicit and safe

Problem: Query translation can split projections and execute part of the
projection client-side. That can hide performance cliffs, execute arbitrary user
code during materialization, and make "translated to SQL" ambiguous.

Evidence:

- `SelectTranslator` logs `-- CLIENT-EVAL` and stores `ClientProjection`.
- `QueryExecutor` applies `plan.ClientProjection` after materialization.

Scope:

- Add an option controlling client evaluation: throw, warn, or allow.
- Include diagnostics with query shape, projection, and estimated row count.
- Ensure client-eval never runs before server filters/paging that should limit
  rows.

Done when:

- Default behavior for v1 is explicit.
- Tests prove forbidden client-eval throws and allowed client-eval is logged.

### 15. Replace string-based bulk CUD plan rewriting

Problem: `BulkCudBuilder` validates and extracts query semantics from generated
SQL strings using substring scans and alias stripping. This is brittle across
providers, nested queries, comments, literals, aliases, and future SQL shapes.

Evidence:

- `ValidateCudPlan` scans the SQL text for keywords.
- `ExtractWhereClause` parses `FROM`, aliases, and `WHERE` manually.

Scope:

- Build bulk CUD from expression/query plan structure rather than SQL text.
- Preserve tenant filters, global filters, null semantics, and provider paging
  rules.
- Add adversarial SQL-shape tests with comments, string literals, quoted
  identifiers, nested subqueries, and provider-specific quoting.

Done when:

- Bulk CUD no longer depends on ad hoc SQL string parsing.
- Unsupported CUD shapes are rejected from structural query metadata.

### 16. Remove `DynamicInvoke` from user expression handling

Problem: `BulkCudBuilder.BuildSetClause` compiles and `DynamicInvoke`s set
values. The rest of the query pipeline already has safer extraction helpers,
and `DynamicInvoke` is slow and risky with user expressions.

Evidence:

- `BuildSetClause` calls `Expression.Lambda(...).Compile().DynamicInvoke()`.
- `ExpressionValueExtractor` comments explicitly avoid arbitrary
  `Compile().DynamicInvoke()`.

Scope:

- Reuse a safe constant/member extraction path for update values.
- Define what update expressions are allowed: constants, captured values,
  arithmetic, server expressions, or column references.
- Add tests for malicious/throwing expressions and closure values.

Done when:

- Bulk update set values use the same safety model as query parameters.
- Unsupported update expressions fail before executing user code unexpectedly.

### 17. Stabilize async/sync policy

Problem: README says nORM is async-first and has no synchronous `SaveChanges`,
but the public surface includes `ToListSync`, `CountSync`, sync enumerator paths,
and sync-over-async temporal initialization. v1 needs a coherent sync policy.

Evidence:

- `NormAsyncExtensions` exposes sync query helpers.
- `DbContext` sync connection path blocks on temporal bootstrap.
- README warns against sync-over-async for `SaveChangesAsync`.

Scope:

- Decide whether sync query APIs are supported, advanced, or removed before v1.
- Document sync usage and deadlock limitations.
- Add tests under a custom `SynchronizationContext` if sync APIs remain.

Done when:

- README, API names, and implementation agree on sync support.
- Sync APIs do not accidentally bypass async cancellation/timeout semantics.

### 18. Add AOT, trimming, and dynamic-code policy

Problem: nORM uses Reflection.Emit, expression compilation, runtime materializer
generation, reflection-based provider loading, and source generators. v1 needs a
clear policy for NativeAOT/trimming and unsupported deployment modes.

Evidence:

- `DynamicEntityTypeGenerator` uses dynamic type generation.
- materialization and connection factories compile expressions.
- provider loading uses `Type.GetType` for optional drivers.

Scope:

- Decide supported status for trimming and NativeAOT.
- Add annotations, warnings, or explicit unsupported exceptions.
- Create source-generator-first guidance for dynamic-code-restricted apps.

Done when:

- Consumers get deterministic build/runtime behavior under trimming/AOT.
- Package analyzers or docs warn about unsupported combinations.

### 19. Formalize query plan/cache memory limits

Problem: The code has multiple caches: dynamic type cache, compiled materializer
store, query plan cache, prepared command caches, mapping caches, and LRU caches.
v1 needs documented budgets, eviction, and observability so hot multi-tenant apps
do not leak memory over time.

Evidence:

- `DbContext` has static dynamic type cache and per-context prepared command
  caches.
- `CompiledMaterializerStore` is a global public static store.
- `ConcurrentLruCache` is public under `nORM.Internal`.

Scope:

- Inventory all caches and decide static/per-context lifetimes.
- Add size limits and clear/dispose hooks where missing.
- Expose diagnostics counters for hits, misses, evictions, and current size.

Done when:

- Long-running stress tests show bounded memory.
- Users have documented ways to inspect and clear supported caches.

### 20. Finish migration rename and data-loss story

Problem: README warns that property renames generate drop/add and can silently
destroy data. A v1 migration system should not silently generate destructive
operations without an explicit decision.

Evidence:

- README contains a data-loss warning for column renames.
- Schema diff/generation tests cover drift but v1 user tooling still needs a
  safe workflow.

Scope:

- Add rename operations or migration annotations.
- Mark destructive generated diffs and require `--force` or manual confirmation.
- Generate comments/TODOs in migration files for possible rename detection.

Done when:

- The CLI cannot silently generate destructive column/table drop operations as a
  normal happy path.
- Rename workflows are documented and tested.

### 21. Make migrations provider parity explicit

Problem: Migration runners differ by provider, especially around transactional
DDL, advisory locks, history status, and partial failure behavior. Users need a
stable contract for deployment automation.

Evidence:

- SQL Server/PostgreSQL use advisory locks differently.
- MySQL tracks `Partial` status because DDL auto-commits.
- README notes PostgreSQL advisory lock can block indefinitely unless caller
  supplies cancellation.

Scope:

- Define migration atomicity, locking, timeout, cancellation, and recovery
  semantics per provider.
- Add common `MigrationOptions` for lock timeout, history table/schema, and
  failure policy.
- Add live tests for interrupted deployments and retry recovery.

Done when:

- Deployment operators know what happens after process kill, timeout, or failed
  DDL for each provider.

### 22. Complete scaffolding as a real v1 feature or mark it preview

Problem: `DatabaseScaffolder` depends on generic `GetSchemaAsync("Tables")`,
uses a zero-row `SELECT *`, has limited provider-specific behavior, and tests
document that SQLite can throw because `GetSchema("Tables")` is unsupported.

Evidence:

- Scaffolder uses `connection.GetSchemaAsync("Tables")`.
- Tests allow SQLite scaffolding to throw.
- `EscapeIdentifier` falls back to `SqliteProvider` for unknown connections.

Scope:

- Implement provider-specific schema readers for all supported providers.
- Generate keys, nullable reference types, indexes, relationships, schemas,
  composite keys, owned/many-to-many hints, and DbContext sets correctly.
- Add packaged CLI scaffold tests for each provider.

Done when:

- Scaffolding either works across the advertised providers or is explicitly
  labeled preview and removed from v1 marketing.

### 23. Add design-time model discovery for CLI migrations

Problem: `norm migrations add` tries to instantiate a DbContext using
`(DbConnection, DatabaseProvider)` with an in-memory SQLite connection, then
falls back to attribute-only snapshots. Real applications often need DI,
options, tenant providers, or custom constructors.

Evidence:

- `Program.cs` catches context construction failures and logs an attribute-only
  fallback warning.

Scope:

- Add a design-time factory interface similar in spirit to EF design-time
  context creation.
- Allow selecting context type and startup assembly.
- Fail loudly when fluent configuration cannot be loaded unless user explicitly
  chooses attribute-only mode.

Done when:

- CLI migrations include fluent model configuration for real app contexts.
- Attribute-only fallback cannot silently produce an incomplete migration.

### 24. Harden destructive CLI commands

Problem: `norm database drop` drops tables or deletes SQLite files without an
interactive confirmation, dry-run mode, include/exclude filters, or obvious
production safeguards.

Evidence:

- `Program.cs` deletes the SQLite file directly.
- Other providers iterate schema tables and issue `DROP TABLE`.

Scope:

- Add `--yes`, `--dry-run`, environment confirmation, and protected database
  checks.
- Exclude migration history only by explicit option.
- Use dependency-aware drop ordering or provider cascade strategy.

Done when:

- Accidental production drops are difficult.
- Drop behavior is predictable for FK-heavy schemas.

### 25. Define transaction ownership and ambient transaction contract

Problem: The code has substantial transaction handling, ambient transaction
policy, savepoints, external transaction ownership, and bulk transaction logic.
v1 needs one documented contract across direct writes, `SaveChanges`, bulk,
compiled queries, migrations, and raw SQL.

Evidence:

- `DbContext.CurrentTransaction` is internal and used by providers.
- `AmbientTransactionEnlistmentPolicy` has fail-fast/best-effort/ignore modes.
- Tests cover many transaction races and policy cases.

Scope:

- Document transaction ownership rules and nesting behavior.
- Ensure every command creation path uses the same transaction binding pipeline.
- Add a transaction matrix doc and a high-level test suite that maps directly to
  the doc.

Done when:

- Users can predict commit/rollback behavior without reading implementation
  tests.

### 26. Formalize multi-tenancy as a security boundary

Problem: The code has extensive tenant-isolation tests, but v1 needs a written
threat model and a supported/non-supported boundary. Raw SQL, stored procedures,
migrations, scaffolding, dynamic queries, cache keys, bulk operations, and direct
write APIs have different tenant behavior.

Evidence:

- `DbContextOptions.TenantProvider` and `TenantColumnName` drive automatic
  filtering.
- Many adversarial tenant tests exist.
- Raw SQL APIs and stored procedure APIs are present.

Scope:

- Write a tenant security model: protected paths, bypass paths, and user
  responsibilities.
- Ensure every write/read/cache path either enforces tenant isolation or is
  explicitly documented as caller-controlled.
- Add tests for tenant behavior in raw SQL and stored procedures if those are
  intended to be protected.

Done when:

- "Enterprise Ready: Multi-tenancy" is backed by a documented security contract.

### 27. Add safer raw SQL APIs

Problem: `FromSqlRawAsync` and object-array parameters are available, but there
is no obvious `FromSqlInterpolated`/FormattableString path that prevents string
interpolation mistakes. Dynamic `Query(string)` also needs clear identifier
safety rules.

Evidence:

- README advertises raw SQL.
- `DbContext.Query(string)` exists for dynamic table access.
- `ParameterHelper` handles object arrays, but API shape still permits unsafe
  caller composition.

Scope:

- Add interpolated raw SQL APIs or a clear analyzer warning strategy.
- Document parameterization and identifier escaping rules.
- Add tests for unsafe interpolation examples and safe alternatives.

Done when:

- The easiest raw SQL path is also the safe path.

### 28. Make logging and redaction policy complete

Problem: SQL redaction exists, but v1 needs a complete logging contract covering
SQL text, parameters, connection strings, exceptions, interceptors, generated
migration SQL, and provider diagnostics.

Evidence:

- `QueryExecutor.RedactSqlForLogging` redacts string literals.
- CLI connection validation currently conflates execution strings and redacted
  strings.
- Command interceptors can inspect commands and log independently.

Scope:

- Redact parameter values by default in logs.
- Provide opt-in sensitive-data logging.
- Ensure interceptors and exception types follow the same policy.

Done when:

- A secret-scanning test suite proves logs and exceptions do not leak secrets by
  default.

### 29. Stabilize interceptors as a public extensibility point

Problem: Command and SaveChanges interceptors are public enterprise APIs. Their
ordering, mutation rules, exception behavior, sync/async parity, cancellation,
and transaction participation must be fixed before v1.

Evidence:

- `DbContextOptions.CommandInterceptors` and `SaveChangesInterceptors` are public
  mutable lists.
- `CommandInterceptorExtensions` wraps command execution.

Scope:

- Document interceptor order and allowed mutations.
- Define exception and cancellation behavior.
- Add tests for nested interceptors, mutation, suppression, retries, and logging.

Done when:

- Interceptor authors can rely on stable behavior across minor versions.

### 30. Define provider version support and startup validation

Problem: Providers have implicit version requirements and optional capabilities,
but there is no central provider capability matrix enforced at startup.

Evidence:

- `PostgresProvider` has a minimum version field.
- SQLite initialization sets PRAGMAs.
- Savepoints, JSON, temporal triggers, bulk APIs, identity retrieval, and paging
  vary by provider.

Scope:

- Add provider capability descriptors.
- Validate server version where a feature is enabled.
- Document provider-specific limitations and fallback behavior.

Done when:

- Unsupported database versions fail early with actionable messages.

### 31. Finish temporal/versioning as a stable feature

Problem: Temporal versioning is advertised as a modern feature, but it is a
cross-provider trigger/history-table system with bootstrap DDL, tag tables, and
custom column names. That is a large correctness surface.

Evidence:

- `EnableTemporalVersioning`, `CreateTagAsync`, `AsOf`, temporal managers, and
  provider trigger SQL exist.
- Provider trigger SQL is handwritten per provider.

Scope:

- Define temporal schema compatibility and migration interaction.
- Decide whether temporal is v1 stable or preview.
- Add live provider tests for insert/update/delete histories, tag lookups,
  schema drift, custom names, and concurrent bootstrap.

Done when:

- Temporal behavior is documented and verified per provider.

### 32. Stabilize bulk operation semantics

Problem: Bulk operations are a core performance claim and have native/fallback
paths, temp tables, provider-specific APIs, tenant checks, OCC handling, and
transaction ownership. v1 needs a stable semantic contract, not only speed.

Evidence:

- `BulkOperationProvider`, provider-specific bulk files, and fallback batched
  paths exist.
- Bulk operations use temp tables and provider-specific copy APIs.

Scope:

- Define key propagation, generated values, concurrency tokens, triggers,
  tenant filters, transaction behavior, cancellation, and partial failure
  behavior for bulk insert/update/delete.
- Ensure native and fallback paths are semantically equivalent.
- Publish bulk benchmarks and correctness matrix.

Done when:

- Users can swap providers or fallback modes without semantic surprises.

### 33. Finish optimistic concurrency guarantees

Problem: MySQL affected-row semantics leave a documented same-value token gap.
For v1, this must be either closed, made opt-in/unsafe, or prominently
documented as an unsupported guarantee.

Evidence:

- README documents the residual MySQL gap.
- `DbContextOptions.RequireMatchedRowOccSemantics` exists.
- `MySqlProvider.UseAffectedRowsSemantics` is true.

Scope:

- Decide whether the default MySQL path should require matched-row semantics.
- Provide a provider/connection-string path to full guarantees.
- Add docs and tests for rowversion, timestamp, nullable tokens, composite keys,
  direct writes, bulk writes, and same-value conflicts.

Done when:

- The v1 concurrency guarantee is precise and provider-specific gaps are not
  hidden.

### 34. Normalize exception taxonomy

Problem: nORM has custom exception types, but code still throws a mix of
`InvalidOperationException`, `ArgumentException`, `NotSupportedException`,
`DbException`, and custom `Norm*` exceptions across user-facing paths.

Evidence:

- `NormException`, `NormQueryException`, `NormConfigurationException`,
  `NormUnsupportedFeatureException`, and others exist.
- Query/provider/configuration paths still throw framework exceptions directly.

Scope:

- Define which exceptions are user contract.
- Wrap database and translation failures consistently while preserving inner
  exceptions and SQL/parameter redaction.
- Update tests to assert stable exception types for public APIs.

Done when:

- Users can catch predictable nORM exceptions for major failure classes.

## P2 Blockers

### 35. Clean encoding and repository hygiene

Problem: Several files contain non-ASCII box drawing/comments or mojibake-like
characters in comments. `.gitignore` ignores all `*.ps1`, which can hide future
release scripts, while the repository already tracks `eng/v1-release-gate.ps1`.
Local ignored artifacts include large TRX files and old local packages.

Evidence:

- `.github/workflows/ci.yml` comments display non-ASCII separators.
- Code comments contain replacement characters from encoding conversions.
- `.gitignore` has a broad `*.ps1` rule.

Scope:

- Normalize source/docs/workflow files to UTF-8 without mojibake.
- Replace broad script ignore rules with specific generated-file ignores.
- Add a hygiene check for generated artifacts and unexpected encodings.

Done when:

- Fresh clones and PR diffs do not contain encoding noise.
- Release scripts cannot be accidentally hidden by ignore rules.

### 36. Split generated/source-generator dead code paths

Problem: There are multiple source-generation directories, including
`src/nORM/SourceGeneration` excluded from the main compile and a separate
`src/nORM.SourceGenerators` project. This creates confusion about the shipping
generator.

Evidence:

- `src/nORM.csproj` removes `nORM/SourceGeneration/**`.
- `src/nORM.SourceGenerators` is the actual analyzer project.
- `src/SourceGeneration` contains runtime attributes/store.

Scope:

- Delete or relocate excluded legacy generator code.
- Document source-generation project boundaries.
- Ensure docs and tests refer only to shipping generator artifacts.

Done when:

- There is one obvious source generator implementation and one runtime
  attributes/store location.

### 37. Add benchmark governance and publish release baselines

Problem: README says performance decisions should be based on fresh benchmarks,
but v1 needs fixed benchmark governance: hardware notes, provider versions,
seeded schema, fairness rules, raw outputs, and pass/fail thresholds.

Evidence:

- Benchmark projects exist.
- `docs/v1-readiness.md` requires benchmark summaries.
- CI does not currently publish benchmark artifacts.

Scope:

- Define benchmark scenarios that support marketing claims against Dapper, Raw
  ADO.NET, and EF Core.
- Add allocation thresholds and regression budgets.
- Store baseline summaries for v1 RCs.

Done when:

- Performance claims can be backed by reproducible artifacts from the release
  commit.

### 38. Add documentation for production operations

Problem: README is broad, but v1 users need operational docs: DI setup,
connection pooling, retries, timeouts, logging, transactions, migrations,
multi-tenancy, provider setup, troubleshooting, and performance tuning.

Evidence:

- README covers many features, but not full operational runbooks.
- API docs exist under `docs/api`.

Scope:

- Add docs pages for install/provider setup, queries, writes, bulk, migrations,
  transactions, multi-tenancy, raw SQL, source generation, performance, and
  troubleshooting.
- Include provider-specific examples.
- Add docs validation for snippets that should compile.

Done when:

- New users can build a real app without reading tests or implementation.

### 39. Add governance files for a public v1 project

Problem: The repository lacks standard public project governance files in the
root: security policy, changelog/release notes, contributing guide, support
policy, and code of conduct if desired.

Evidence:

- Root check found no `SECURITY.md`, `CHANGELOG.md`, `NuGet.config`, or
  `global.json`.

Scope:

- Add `SECURITY.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, support/versioning
  policy, and release checklist updates.
- Define vulnerability reporting and supported versions.

Done when:

- Consumers know how security fixes, bug reports, and version support work.

### 40. Create a v1 issue map and execution order

Problem: The blockers above are large and interdependent. v1 needs a sequenced
execution plan so API/package decisions happen before implementation locks in
compatibility debt.

Scope:

- Convert this spec into tracked issues or milestones.
- Sequence work in this order:
  1. Package/provider usability and public API freeze.
  2. CLI/migrations safety.
  3. LINQ/client-eval/bulk semantic decisions.
  4. Provider/multi-tenant/security docs and tests.
  5. CI/benchmark/release evidence.
  6. Documentation/governance polish.
- Require every blocker to have an owner, acceptance tests, and release-note
  impact.

Done when:

- The v1 milestone has 41 tracked items with acceptance criteria.
- No blocker is closed without a test, doc, or explicit "not applicable" note.

### 41. Rebuild benchmark credibility with optimized Raw ADO and precise claims

Problem: The benchmark harness is measuring real code, but current public claims
can read stronger than the evidence supports. In particular, Raw ADO.NET rows are
often materialized through convenient name lookups and `Convert.*`, while a
strong manual baseline would use fixed ordinals, typed getters, prepared command
reuse, and identical SQL shape. Some "Dapper prepared" paths also reuse prepared
ADO commands and manual readers, which makes the label ambiguous.

Evidence:

- `benchmarks/ProviderMatrixBenchmarks.cs` Raw ADO entity readers use
  `reader["Name"]`/`Convert.*` style mapping.
- `benchmarks/OrmBenchmarks.cs` contains older SQLite Raw ADO paths with
  repeated ordinal lookup and convenience parameter APIs.
- Provider-matrix BenchmarkDotNet config uses a small public-evidence sample
  (`warmupCount: 1`, `iterationCount: 3`).
- README currently summarizes Raw ADO as a single manual baseline, without
  distinguishing convenience ADO from optimized ADO.

Scope:

- Split benchmark labels into explicit categories such as
  `RawAdo_Convenience`, `RawAdo_Optimized`, `RawAdo_PreparedOptimized`,
  `Dapper`, `Dapper_Prepared` only where Dapper performs materialization, `nORM`,
  and `nORM_Prepared`.
- Add optimized Raw ADO baselines that cache ordinals per reader shape, use typed
  getters where provider values permit them, avoid avoidable `Convert.*`, reuse
  prepared commands fairly, and keep SQL/projection/filter/order/limit parity.
- Rename or remove misleading Dapper-prepared benchmarks that no longer use
  Dapper mapping.
- Increase RC/public benchmark confidence settings and publish the exact
  BenchmarkDotNet config, hardware, SDK, provider versions, connection strings
  with secrets redacted, raw output artifacts, and summary tables.
- Rewrite public performance language so "beats Raw ADO" is only used for the
  specific Raw ADO category and provider where optimized baselines prove it.

Done when:

- Every public performance claim maps to a named benchmark category and raw
  artifact from the release commit.
- Optimized Raw ADO baselines exist for simple, complex, join, count, insert, and
  bulk scenarios across SQLite, SQL Server, PostgreSQL, and MySQL where relevant.
- Benchmark fairness tests reject accidental downgrades such as dynamic Dapper
  mapping, name-lookup-only Raw ADO baselines for optimized categories, or
  mismatched SQL shape.
- README and release notes use qualified performance language that distinguishes
  convenience ADO, optimized ADO, Dapper materialization, EF Core, nORM runtime,
  and nORM compiled/prepared paths.
