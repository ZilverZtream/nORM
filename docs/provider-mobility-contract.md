# Provider Mobility Contract

nORM's portability goal is semantic provider mobility, not a claim that every
database feature is identical on every database.

The v1 provider mobility contract is:

> A supported nORM shape must translate correctly on every supported provider,
> emulate equivalent behavior where a provider lacks a native primitive, or fail
> deterministically before execution with a nORM exception.

This is the difference between "has multiple providers" and "can survive a
provider swap".

The concrete translation decisions are centralized in
`nORM.Configuration.ProviderMobilityTranslator` and documented in
[`provider-mobility-translation-layer.md`](provider-mobility-translation-layer.md).
Runtime strict failures and static certification findings use that same table
for support class, severity, reason, and suggested fix text. Provider target
decisions, including minimum server version, JSON, temporal, bulk, savepoint,
parameter-limit, and concrete SQL translation strategy profiles, are also
classified by that layer.

`DbContextOptions.UseStrictProviderMobility()` turns this contract into runtime
behavior. In strict mode, generated nORM query/write/temporal APIs remain
available and must translate, emulate, or fail deterministically before
execution. Provider-bound escape hatches such as raw SQL, stored procedures,
direct connection access, provider-native tenant policy DDL, provider-native
temporal storage, and silent client evaluation are rejected with nORM exceptions
instead of being certified as portable. Explicit warning-level client projection
tails are allowed only after server filters, ordering and paging have run.

## Support Classes

| Class | Meaning | Release behavior |
| --- | --- | --- |
| Portable | Same application-facing nORM code works across SQLite, SQL Server, PostgreSQL, and MySQL with equivalent visible behavior. | Must have live-provider evidence. |
| Emulated | Provider lacks a native primitive, but nORM supplies generated SQL, trigger/history infrastructure, fallback bulk behavior, or materialization logic to preserve the contract. | Must document precision/performance caveats and have tests. |
| Provider-bound | The feature intentionally uses one database's native capability. | Must be explicit opt-in and not marketed as provider-neutral. |
| Unsupported | nORM cannot preserve the semantics safely. | Must throw deterministically rather than silently drifting or buffering broad data unexpectedly. |

## Provider Swap Certification

The sample store includes a release-style certification gate:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --report ../../artifacts/provider-swap/sample-store.json
```

`certify-provider-swap` is strict. SQLite, SQL Server, PostgreSQL, and MySQL are
requested by default; a missing external provider connection string fails the
gate. Use `--providers sqlite,postgres` to certify a smaller deployment target
set.

The command emits a machine-readable JSON report with contract name, strict
mode, per-provider status, summary, checks that passed, error/warning totals,
recommended fix rows, and optional migration findings. Provider FAIL results,
and SKIP results under strict certification, are counted as report errors. The
report is a release artifact, not marketing copy.

Use `--scan-path` to add a source-level portability inventory to the report:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --scan-path src/MyApp --report ../../artifacts/provider-swap/myapp.json
```

The first scanner is intentionally conservative and scans application `.cs`,
`.sql`, `.csproj`, `.props`, and `.targets` files. It flags provider-bound nORM
usage such as raw SQL APIs, stored procedure APIs, direct `DbContext.Connection`
access, direct `DbContext.Provider` access, raw `DbTransaction`/`DbCommand`
handles, command interceptors, provider-specific connection classes/packages, dynamic `Query(string)` table
queries, custom `[SqlFunction]` SQL fragments, `[CompileTimeQuery]` raw SQL, provider-native tenant/temporal
configuration, client-evaluation opt-ins, `.sql` stored procedure definitions,
obvious provider-specific SQL syntax, and common migration-source risks from EF
Core or Dapper such as `FromSqlRaw`, `ExecuteSqlRaw`, `UseSqlServer`,
`UseNpgsql`, `HasDefaultValueSql`, `HasComputedColumnSql`,
`HasIdentityOptions`,
`UseCollation`, `HasCollation`, `migrationBuilder.Sql`, provider-specific EF migration annotations and value-generation annotations,
`using Dapper`, `SqlMapper` calls, Dapper/EF/provider `PackageReference` rows,
direct provider ADO.NET command/data-adapter objects, and constrained LINQ
shapes such as regex predicates/replacement that are not release-green on every
provider. Findings include a suggested remediation.
The support class/severity comes from the shared translation layer, and concrete
scanner rules append pattern-specific remediation when an EF/Dapper/migration
pattern has a narrower fix. Most findings
should be rewritten to generated nORM APIs or emulated by nORM when the
semantics are clear; findings that encode database-specific business logic need
human review.

The reusable CLI exposes the same certification scan for applications that are
not the sample app:

```powershell
norm portability certify --scan-path src/MyApp --assembly bin/Release/net8.0/MyApp.dll --report artifacts/provider-mobility.json --html artifacts/provider-mobility.html
```

Use `--providers sqlite,postgres,mysql` to narrow the provider target profile
emitted into the report. Without `--providers`, the profile label controls the
target list; `all-four` emits SQLite, SQL Server, PostgreSQL, and MySQL
capability decisions. Unknown provider targets are error-level findings rather
than silently skipped. Error-level provider capability decisions are also
promoted to certification findings instead of being buried in the report.
Provider aliases are normalized for convenience (`mssql` to `sqlserver`,
`postgresql` to `postgres`). `mariadb` currently maps to the MySQL-compatible
profile and must not be described as separate MariaDB certification until nORM
ships a MariaDB-specific provider target profile and live gate.
Add `--sqlite-connection`, `--sqlserver-connection`, `--postgres-connection`,
or `--mysql-connection` to open and validate real targets. The CLI records the
actual server version exposed by the driver and fails certification when the
target cannot be opened, the server version is below the floor, or the version
cannot be determined for an explicitly probed target.
Provider target profiles also show concrete translation decisions for paging,
identifier escaping, parameter binding, boolean predicates, null semantics,
LIKE escaping, string concatenation, DateTime/decimal/TimeSpan normalization,
temporal clock source, generated-key retrieval, bitwise XOR, case-sensitive
string comparison, regex translation, temporal construction/arithmetic,
row-tuple comparison, and ordered string aggregation. Warning
rows do not fail the report, but they must stay visible so reviewers can tell
native provider behavior from nORM-owned emulation. Warning/error target rows
also feed the report's recommended-fix section, so a PASS report with caveats is
still reviewable instead of looking clean by accident.

Use `--assembly` when possible so the CLI can build the nORM schema snapshot
from the application's design-time `DbContext` and inspect provider-mobile
schema metadata. Use `--schema-snapshot Migrations/schema.snapshot.json` when
only the saved migration snapshot is available. Schema inspection fails for
non-portable metadata such as unsupported CLR column types, provider-specific
default SQL (`GETDATE()`, `DATEADD`, `NEWID()`, etc.), invalid FK metadata, and
identity columns on non-integral types. SQL-standard database-evaluated defaults
such as `CURRENT_TIMESTAMP`, `CURRENT_DATE`, and `CURRENT_USER` are warning-level
review items because timezone, precision, and user semantics still vary by
engine. Provider package/connection
bootstrapping is reported as a warning inventory item, not a hard failure, as
long as generated repositories use nORM APIs rather than direct provider
handles.

Future portability-assessment reports should deepen the inventory for
provider-bound app assets such as native temporal tables, RLS policy DDL,
collations and hand-authored migration DDL. Those findings are not certified
nORM portability failures when they sit outside generated nORM paths; they are
migration findings. The expected remediation is to replace them with generated
nORM LINQ/write/temporal APIs when semantics are clear, or flag them for human
design review when no safe automatic rewrite exists.

For local development smoke checks, use:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- verify-providers
```

`verify-providers` skips unconfigured external providers. It is useful while
iterating, but it is not the strict release certification gate.

## Current Sample Checks

The sample certification gate proves the same application scenario on every
requested provider:

| Check | What it proves |
| --- | --- |
| `schema-bootstrap` | The same model can initialize provider-specific schema for the sample. |
| `strict-provider-mobility-mode` | The scenario runs with `UseStrictProviderMobility()` so provider-bound escape hatches cannot be accidentally certified. |
| `tenant-query-boundary` | Generated queries see only the active tenant. |
| `cross-tenant-update-delete-zero-rows` | Generated writes by known foreign IDs affect zero foreign rows. |
| `bulk-insert-visible-state` | Bulk insert produces the same visible state. |
| `linq-where-select-dto-orderby-skip-take` | Representative SQL-backed LINQ keeps the same result shape. |
| `include-as-split-query` | Navigation loading works through the same app code. |
| `groupby-aggregate` | Aggregate grouping returns equivalent totals. |
| `compiled-query` | Compiled query execution preserves provider and tenant behavior. |
| `temporal-tag-asof-current-history` | nORM-managed temporal tags, `AsOf(tag)`, current reads, and history rows agree. |
| `temporal-restore` | Restore-from-tag updates the current row without provider-specific app code. |
| `temporal-tenant-isolation` | Temporal reads/history keep foreign tenants invisible. |

The local strict semantic profile also compares strict-mode nORM results to
LINQ-to-Objects for a generated dataset covering `Where`, `Contains`, boolean
and numeric predicates, DTO projection, `OrderBy`/`ThenBy`/`Skip`/`Take`,
`GroupBy` aggregate DTO projection, and compiled queries. This is pinned by
`ProviderMobilityStrictSemanticFuzzTests` so strict mode cannot quietly shrink
below ordinary application LINQ.

Strict mode is also pinned against common application query composition:
predicate operators, `First`/`Last`/`Single`/`ElementAt`, collection terminals
(`ToArrayAsync`, `ToDictionaryAsync`, `ToHashSetAsync`), `MinBy`/`MaxBy`,
`Reverse`, `Distinct`, set operators, inner joins, query-syntax left joins with
`DefaultIfEmpty`, member-initialized DTO projections, computed projection
members, temporal scalar columns (`DateOnly`, `TimeOnly`, `TimeSpan`), and
`Include`/`ThenInclude` with `AsSplitQuery`. `ProviderMobilityStrictCommonSurfaceTests`
guards the local contract, including evidence-backed constrained shapes such as
identity `Cast<T>`, TPH `OfType<TDerived>`, and documented DateTimeOffset
instant/local-time translations. `LiveProviderJoinSelectManyParityTests` runs
the risky join DTO and left-join null-fallback shapes under
`UseStrictProviderMobility()` across SQLite, SQL Server, PostgreSQL, and MySQL;
`LiveProviderOfTypeParityTests` also runs the TPH `OfType<TDerived>` query under
strict mode on all four providers. `LiveProviderTerminalOpParityTests` runs the
highest-risk terminal operators (`First`, `FirstOrDefault`, `Last`,
`LastOrDefault`, `ElementAt`) under strict mode on all four providers while the
local strict common profile covers the broader terminal set.

Transaction savepoints are provider-mobile when used through the nORM-owned
`DbContextTransaction` wrapper. Raw `DbTransaction` savepoint overloads remain
compatibility APIs because they require a caller-owned provider transaction
handle. `LiveProviderSavepointMigrationTests` covers the strict wrapped
savepoint workflow across SQLite, SQL Server, PostgreSQL, and MySQL.
`Database.CurrentContextTransaction` exposes the same nORM-owned wrapper for
strict-safe transaction inspection; raw `Database.CurrentTransaction` remains a
provider-bound `DbTransaction` escape hatch.

## Required Boundaries

Provider mobility does not mean raw SQL is portable. These remain
caller-controlled or provider-bound:

- raw SQL and stored procedures;
- custom `[SqlFunction]` fragments unless nORM owns provider translations;
- `[CompileTimeQuery]` raw SQL and `CreateCompiledQueryCommandAsync`;
- direct `DbConnection` / `DbCommand` / raw `DbTransaction` usage, including
  raw `Database.CurrentTransaction`; use `Database.CurrentContextTransaction`
  for the strict-safe nORM wrapper;
- raw-transaction savepoint overloads on `DbContext`; use the
  `DbContextTransaction` savepoint wrapper methods in strict mode;
- command interceptors, because they can inspect, rewrite, or suppress generated commands;
- direct `DbContext.Provider` access and dynamic `Query(string)` table queries;
- provider-native RLS policy installation and operational ownership;
- SQL Server system-versioned temporal tables;
- provider-specific DDL outside nORM migrations/bootstrap;
- database collation, isolation-level, and engine configuration choices.

Strict mode does not delete these APIs. It keeps compatibility mode available
for explicit provider-bound code and prevents those paths from passing a
provider-mobility certification by accident.

Unsupported LINQ or write shapes must fail before execution. Constrained shapes
must state their caveat in `docs/linq-support.md` and, when they are part of the
v1 provider contract, in `docs/live-provider-linq-parity.md`.

## Promotion Rule

A feature can be called provider-mobile only when all of the following are true:

- the public docs classify it as Portable or Emulated;
- tests prove behavior on SQLite, SQL Server, PostgreSQL, and MySQL;
- precision, collation, timezone, or provider-storage caveats are documented;
- the strict certification gate includes it or a dedicated live-provider gate
  covers it;
- unsupported variants throw `NormUnsupportedFeatureException` or
  `NormConfigurationException` with an actionable message.

Provider-shape-only tests are not enough for provider mobility claims.
