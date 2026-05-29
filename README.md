# nORM

nORM is an evidence-gated, provider-mobile ORM for .NET.

It is built around a specific goal: keep application data access on generated
nORM APIs so the same model and LINQ surface can move across SQLite, SQL
Server, PostgreSQL, and MySQL with equivalent behavior, provider-specific SQL
translation, explicit tenant boundaries, provider-neutral temporal history, and
benchmark-governed performance.

nORM is not marketed as a complete EF Core replacement, a complete LINQ
implementation, or "always faster than raw ADO.NET". Claims in this repository
are intentionally bounded by docs, tests, live-provider gates, sample evidence,
and BenchmarkDotNet artifacts.

## Product Pillars

### Provider Mobility

nORM's provider mobility contract is:

> A supported nORM shape must translate correctly on every supported provider,
> emulate equivalent behavior where a provider lacks a native primitive, or fail
> deterministically before execution with a nORM exception.

`DbContextOptions.UseStrictProviderMobility()` turns that rule into runtime
behavior for generated nORM paths. Strict mode keeps ordinary generated query,
write, bulk, tenant, transaction-wrapper, and temporal APIs available, while
rejecting provider-bound escape hatches such as raw SQL, stored procedures,
direct `DbConnection` / `DbCommand` / raw `DbTransaction` access, custom SQL
fragments, provider-native tenant/temporal DDL, and silent client-evaluation
opt-ins.

The shared translation decisions live in
`nORM.Configuration.ProviderMobilityTranslator` and are documented in
[Provider Mobility Contract](docs/provider-mobility-contract.md) and
[Provider Mobility Translation Layer](docs/provider-mobility-translation-layer.md).

### Broad SQL-Backed LINQ

nORM supports a broad EF-style LINQ surface for SQL-backed application queries,
including common filtering, projection, ordering, paging, joins, left joins,
aggregates, grouping, terminal operators, compiled queries, and documented
provider-specific translations. The LINQ matrix distinguishes provider-mobile
generated shapes from provider-bound compatibility APIs.

Current hard-query evidence includes live-provider parity tests for complex
commercial analytics and region/category analytics workloads across SQLite,
SQL Server, PostgreSQL, and MySQL. Coverage rows are not treated as parity
evidence unless live-provider evidence exists.

See [LINQ Support](docs/linq-support.md),
[LINQ Support Coverage](docs/linq-support-coverage.md), and
[Live Provider LINQ Parity](docs/live-provider-linq-parity.md). This is the
Documented LINQ Support contract for v1.

### Tenant Boundary

nORM tenant support is a generated-path boundary, not tenant convenience.

When `DbContextOptions.TenantProvider` and `TenantColumnName` are configured,
generated query and write paths inject tenant predicates, tenant-aware write
`WHERE` clauses, tenant-aware cache keys, tenant ID validation/coercion, and
fail-closed configuration errors. Raw SQL, stored procedures, migrations,
scaffolding, and direct connection access remain privileged caller-controlled
paths and do not receive automatic tenant predicates.

SQL Server and PostgreSQL can optionally use provider-native session context and
reviewable RLS policy DDL as defense in depth.

See [Tenant Boundary](docs/tenant-boundary.md),
[Tenant Deployment Patterns](docs/tenant-deployment-patterns.md), and
[Tenant Database-Native RLS](docs/tenant-database-native-rls.md).

### Temporal Versioning

nORM's default temporal model is provider-neutral, nORM-managed history:
history tables, tag storage, and provider-specific triggers/functions managed
by nORM for mapped entities. `AsOf(DateTime)`, `AsOf(tag)`, history reads,
changed-property diffs, restore-from-tag, pruning, and tenant-aware temporal
reads are part of the documented v1 contract.

This is not SQL Server system-versioned temporal tables by default. SQL Server
has an explicit provider-native temporal mode for applications that choose that
provider-bound storage model.

See [Temporal Versioning](docs/temporal-versioning.md) and
[Temporal Precision](docs/temporal-precision.md).

### Benchmark-Governed Performance

nORM performance claims must come from current BenchmarkDotNet artifacts and
the benchmark threshold gate. The provider matrix compares EF Core, Dapper,
nORM, and Raw ADO.NET across SQLite, SQL Server, PostgreSQL, and MySQL with
named baseline categories:

- `RawAdo_Convenience`: straightforward handwritten ADO with conversions/name
  lookups.
- `RawAdo_Optimized`: ordinal-based typed access where provider values permit
  it.
- `RawAdo_TypedNoBox`: typed getter allocation floor.
- `RawAdo_PreparedOptimized` and `RawAdo_PreparedTypedNoBox`: prepared-command
  variants.

SQLite benchmark connections apply the same durability PRAGMAs to every
competitor connection so single-insert comparisons do not measure mismatched
fsync policy. Fast local benchmark suites are smoke tests only and are rejected
as RC/full public evidence.

See [Benchmark Governance](docs/benchmark-governance.md) and
[benchmarks/README.md](benchmarks/README.md).

## Sample Store App

`samples/nORM.Sample.Store` is the product-proof sample. It is a tenant-aware
store web app with a browser frontend, cookie-based tenant login, authenticated
APIs, provider-swappable backend configuration, representative LINQ, bulk
writes, compiled queries, `Include().AsSplitQuery()`, and temporal actions.

Run it locally with SQLite:

```powershell
dotnet run --project samples/nORM.Sample.Store -- --provider sqlite
```

Run the same app against another configured provider:

```powershell
$env:NORM_SAMPLE_SQLSERVER = 'Server=localhost\SQLEXPRESS;Database=normtest;Integrated Security=True;TrustServerCertificate=True;Encrypt=False;Connect Timeout=10'
$env:NORM_SAMPLE_POSTGRES = 'Host=127.0.0.1;Port=5432;Database=normtest;Username=postgres;Password=<password>'
$env:NORM_SAMPLE_MYSQL = 'Server=127.0.0.1;Port=3306;Database=normtest;User ID=root;Password=<password>;AllowPublicKeyRetrieval=True'

dotnet run --project samples/nORM.Sample.Store -- --provider sqlserver
dotnet run --project samples/nORM.Sample.Store -- --provider postgres
dotnet run --project samples/nORM.Sample.Store -- --provider mysql
```

Run the provider verification scenario:

```powershell
dotnet run --project samples/nORM.Sample.Store -- verify-providers
```

Run the strict provider-swap certification gate:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --report ../../artifacts/provider-swap/sample-store.json
```

The certification report records strict mode, per-provider status, provider
capability profiles, live server-version evidence when available, scan status,
source/schema findings, warning/error totals, and recommended fix rows.

See [samples/nORM.Sample.Store/README.md](samples/nORM.Sample.Store/README.md).

## Installation

```powershell
dotnet add package nORM
```

SQL Server and SQLite driver support is included by the runtime package.
PostgreSQL requires `Npgsql`; MySQL requires `MySqlConnector` or `MySql.Data`.
See [Provider Packages](docs/provider-packages.md) and
[Provider Capabilities](docs/provider-capabilities.md).

## Quick Start

```csharp
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;

await using var connection = new SqlConnection(connectionString);
await connection.OpenAsync();

var options = new DbContextOptions()
    .UseStrictProviderMobility();

await using var db = new DbContext(connection, new SqlServerProvider(), options);

var recentOrders = await db.Query<Order>()
    .Where(o => o.Status == "Paid" || o.Status == "Shipped")
    .OrderByDescending(o => o.CreatedAt)
    .Take(50)
    .ToListAsync();
```

Compiled query:

```csharp
var paidOrdersByCustomer = Norm.CompileQuery<DbContext, int, Order>(
    (ctx, customerId) => ctx.Query<Order>()
        .Where(o => o.CustomerId == customerId && o.Status == "Paid")
        .OrderByDescending(o => o.CreatedAt));

var rows = await paidOrdersByCustomer(db, customerId);
```

Tenant configuration:

```csharp
var options = new DbContextOptions
{
    TenantProvider = new HttpContextTenantProvider(),
    TenantColumnName = "TenantId"
}.UseStrictProviderMobility();
```

Temporal configuration:

```csharp
var options = new DbContextOptions();
options.EnableTemporalVersioning();

await db.CreateTagAsync("before-price-update");

var historical = await db.Query<Product>()
    .AsOf("before-price-update")
    .ToListAsync();
```

Bulk insert:

```csharp
await db.BulkInsertAsync(products);
```

## Supported Providers

| Provider | v1 role |
| --- | --- |
| SQLite | Local/dev/test provider and live-provider parity target. |
| SQL Server | Live-provider parity target; optional native tenant/RLS and native temporal mode. |
| PostgreSQL | Live-provider parity target; optional native tenant/RLS support. |
| MySQL | Live-provider parity target with documented concurrency and capability caveats. |

MariaDB is treated as a MySQL-compatible inventory target until nORM ships a
separate MariaDB provider profile and live gate.

## CLI And Certification

The tool package exposes reusable provider-mobility certification for existing
applications:

```powershell
norm portability certify `
  --scan-path src/MyApp `
  --assembly bin/Release/net8.0/MyApp.dll `
  --report artifacts/provider-mobility.json `
  --html artifacts/provider-mobility.html
```

The scanner flags provider-bound assets such as raw SQL APIs, stored procedure
APIs, direct provider handles, provider-specific packages/connection classes,
custom SQL functions, compile-time raw SQL, provider-native temporal/tenant
configuration, client-evaluation opt-ins, `.sql` procedure files, EF Core raw
SQL/migration/provider syntax, Dapper usage, and schema metadata that is not
portable. Findings are remediation work, not hidden failures.

See [dotnet-norm README](src/dotnet-norm/README.md).

## Boundaries

nORM generated APIs are the provider-mobile surface. These paths remain
provider-bound or privileged:

- raw SQL and stored procedures;
- direct `DbConnection`, `DbCommand`, raw `DbTransaction`, and provider access;
- custom SQL fragments and user-authored DDL;
- migrations, scaffolding, and destructive database operations;
- provider-native temporal mode and provider-native tenant/RLS policy DDL;
- database-specific collations, stored procedure bodies, native functions, and
  hand-authored SQL migration scripts.

Where semantics are clear, rewrite provider-bound assets to generated nORM
LINQ/write/temporal APIs. Where the database code encodes behavior that cannot
be inferred safely, certification should flag it for human design review.

## Migrations And Operations

nORM includes migration runners, design-time snapshot generation, guarded
database drop, advisory migration locks, transaction/savepoint contracts,
retry/interceptor policies, logging redaction, cache bounds, and source
generation support.

`norm migrations add` uses `INormDesignTimeDbContextFactory<TContext>` when one
is present so fluent mapping is included in generated migration snapshots. Use
`--attribute-only` only when the migration should intentionally ignore fluent
model configuration.

Scaffolding is preview in v1: table and column reverse engineering is supported,
while relationship and index generation remain explicit post-processing.

nORM is async-first for writes. There is no synchronous `SaveChanges` API;
legacy synchronous query helpers such as `ToListSync` and `CountSync` remain
documented compatibility APIs.

MySQL optimistic concurrency has a strict default: nORM refuses
timestamp-tracked MySQL updates by default when affected-row semantics cannot
prove matched-row concurrency behavior. Use `UseAffectedRows=false` with
`new MySqlProvider(useAffectedRowsSemantics: false)` for the strongest MySQL
contract.

For reviewers: nORM refuses timestamp-tracked MySQL updates by default unless
the configured provider/driver path can prove the documented concurrency
contract.

Important docs:

- [Migration Provider Contract](docs/migration-provider-contract.md)
- [Design-Time Migrations](docs/design-time-migrations.md)
- [Scaffolding Preview](docs/scaffolding.md)
- [Transactions](docs/transactions.md)
- [Sync and Async Policy](docs/sync-policy.md)
- [Bulk Operations](docs/bulk-operations.md)
- [Cache Policy](docs/cache-policy.md)
- [Multi-Tenancy Security](docs/multi-tenancy-security.md)
- [Raw SQL Security](docs/raw-sql-security.md)
- [Stored Procedure Security](docs/stored-procedure-security.md)
- [Interceptors](docs/interceptors.md)
- [Logging and Redaction](docs/logging-redaction.md)
- [Exception Taxonomy](docs/exception-taxonomy.md)
- [Optimistic Concurrency](docs/optimistic-concurrency.md)
- [Source Generation](docs/source-generation.md)
- [AOT and Trimming](docs/aot-trimming.md)
- [Production Operations](docs/production-operations.md)
- [Repository Hygiene](docs/repository-hygiene.md)

## Migration From EF Core Or Dapper

nORM uses familiar EF-style concepts, but migration should be treated as a port,
not a blind package swap.

Recommended workflow:

1. Run source/schema certification to inventory provider-bound assets.
2. Move ordinary data access to generated nORM LINQ/write/bulk/temporal APIs.
3. Run strict provider mobility mode in tests.
4. Run live-provider parity for the target provider set.
5. Run benchmark slices for the paths that matter to the application.

Do not claim an application migration is safe unless that specific application
workflow is tested and proven.

## Development

```powershell
dotnet restore
dotnet build nORM.sln -c Release --nologo
dotnet test tests/nORM.Tests.csproj -c Release --no-build
```

Focused release-style checks:

```powershell
dotnet run --project samples/nORM.Sample.Store -- verify-providers
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --report ../../artifacts/provider-swap/sample-store.json
eng/run-benchmark-isolated.ps1 -- --provider-matrix --provider Sqlite --filter "*ProviderMatrixBenchmarks.Insert_Single*"
```

The full release gate is intentionally heavier and should not be run after every
small edit. Use focused tests, live-provider slices, and provider-specific
benchmark slices first.

## Status

The v1/RC work is tracked in [v1 Issue Map](docs/v1-issue-map.md),
[Release Gates](docs/release-gates.md), and
[Benchmark Governance](docs/benchmark-governance.md). Release process and
community/security expectations are tracked in [Release Checklist](docs/release-checklist.md),
[CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md),
[SUPPORT.md](SUPPORT.md), and [CHANGELOG.md](CHANGELOG.md). Public release
claims should point to those artifacts, the sample app, live-provider evidence,
and fresh benchmark output rather than this README alone.

## License

MIT. See [LICENSE](LICENSE).
