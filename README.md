# nORM (The Norm) - The Provider-Mobile ORM for .NET

nORM is a modern Object-Relational Mapping (ORM) library for .NET built around one
idea: **the same application code should run, unchanged, on SQLite, SQL Server,
PostgreSQL, and MySQL.** You write normal LINQ; nORM acts as a translation layer
between your code and whichever provider you point it at. Switching databases is
meant to be as routine as changing a connection string - not a rewrite.

It does this without giving up the ergonomics you expect from an EF Core-style
ORM: LINQ queries, change tracking, migrations, multi-tenancy, temporal history,
compiled queries, and provider-native bulk operations - and it stays competitive
with hand-written ADO.NET on the hot path (see [Performance](#performance)).

## Why Choose nORM?

### 1. Provider mobility is the product, not a footnote

- **Swap providers like changing a connection string.** Supported LINQ and API
  shapes must translate, emulate, or fail *deterministically and identically*
  across all four providers - that is the enforced
  [Provider Mobility Contract](docs/provider-mobility-contract.md), not a
  best-effort hope.
- **Strict Provider Mobility Mode.** Turn on `UseStrictProviderMobility()` during
  development and nORM blocks exactly the things that *can't* move between
  databases - raw SQL, stored procedures, direct connection/command access,
  provider-native DDL, command interceptors, and client-eval escape hatches -
  while admitting every portable, nORM-translated feature. You find out you've
  painted yourself into a provider-specific corner at dev time, not in
  production. See the [translation layer](docs/provider-mobility-translation-layer.md).
- **Certification tooling.** `dotnet norm portability certify` scans an existing
  codebase and schema and reports which assets are portable and which are
  provider-bound migration findings, with concrete translation strategies and
  optional live server-version/feature probes.
- **A translation layer that keeps growing.** Hard, real-world LINQ shapes
  (multi-join analytics, `GroupBy` with element selectors and projection tails,
  left joins, correlated aggregates, `DistinctBy`, `SequenceEqual`,
  `Take/SkipWhile`, regex predicates, `DateTimeOffset` arithmetic) are covered by
  live cross-provider parity tests so "portable" means "verified on all four,"
  not "probably fine."

### 2. Fast where it counts

- **Fastest method on every provider in our matrix is a nORM path** (corrected,
  threshold-gated RC2 evidence; see [Performance](#performance)).
- Compiled/prepared query paths, IL-generated materializers, low-allocation
  execution, and database-native bulk operations - benchmarked against EF Core,
  Dapper, and optimized Raw ADO.NET with [explicit baseline rules](docs/benchmark-governance.md).

### 3. Everything you expect from a real ORM

- **Familiar API**: EF Core-style context, configuration, and change tracking
- **Documented LINQ Support** with explicit limits in [the LINQ support matrix](docs/linq-support.md)
- **Bulk Operations**: provider-specific bulk insert/update/delete with documented semantics
- **Multi-tenancy** enforced on every generated query and write path
- **Temporal queries & versioning** with nORM-managed history and `AsOf(tag)`
- **Migrations** with provider-correct DDL, advisory-locked concurrent deploys, and safe rename detection
- **Operational features**: caching, retry policies, interceptors, JSON querying, window functions
- **Explicit deployment boundaries**: JIT-first with source-generation support and documented [AOT/trimming limits](docs/aot-trimming.md)
- **Bounded cache policy** with documented lifetimes, limits, and diagnostics ([cache policy](docs/cache-policy.md))
- **Multi-database support**: SQL Server, PostgreSQL, SQLite, and MySQL
- **Product-Proof Sample**: `samples/nORM.Sample.Store` is a provider-swappable tenant + temporal web app with a browser frontend, authenticated tenant flow, and a verification mode.

## Sample Store App

Run the RC3 product-proof sample locally with SQLite:

```bash
dotnet run --project samples/nORM.Sample.Store -- --provider sqlite
```

The same app can target SQL Server, PostgreSQL, or MySQL by setting
`NORM_SAMPLE_*` or `NORM_TEST_*` connection strings and changing only
`--provider`. The sample demonstrates generated-path tenant boundaries,
representative LINQ, bulk insert, compiled query, `Include().AsSplitQuery()`,
and nORM-managed temporal `AsOf(tag)`. See
[samples/nORM.Sample.Store/README.md](samples/nORM.Sample.Store/README.md),
[Tenant Boundary](docs/tenant-boundary.md), and
[Temporal Versioning](docs/temporal-versioning.md).

For the strict provider-swap release gate, see
[Provider Mobility Contract](docs/provider-mobility-contract.md). The sample
can emit a certification artifact:

```powershell
dotnet run --project samples/nORM.Sample.Store -c Release --no-build -- certify-provider-swap --report ../../artifacts/provider-swap/sample-store.json
```

Provider-bound assets in existing applications, such as SQL Server stored
procedures or provider-specific raw SQL, should be inventoried as migration
findings. nORM should translate or emulate generated nORM features; arbitrary
caller-authored database language needs a generated nORM rewrite or an explicit
human-reviewed remediation.

Existing applications can run the reusable certification scanner through the
tool package:

```powershell
norm portability certify --scan-path src/MyApp --assembly bin/Release/net8.0/MyApp.dll --report artifacts/provider-mobility.json --html artifacts/provider-mobility.html
```

The report includes source findings, schema metadata findings, provider target
profiles, live server-version evidence when connection strings are supplied,
feature probes such as JSON availability, and concrete translation-strategy
rows for dialect differences.

<a name="performance"></a>
## Performance

nORM ships BenchmarkDotNet suites that compare nORM against EF Core, Dapper, and
Raw ADO.NET across all four providers, using the same seeded schema, equivalent
SQL shape, typed materialization, and matched compiled/prepared modes. The
numbers below are from the threshold-gated RC2 provider matrix
(commit `a4c3017`, `launchCount/warmupCount/iterationCount` per
[governance](docs/benchmark-governance.md), Windows x64, .NET 8.0.417). They are
representative, not a substitute for re-running on your own hardware before a
release decision.

### nORM runtime latency, full provider matrix (mean, lower is better)

| Operation (runtime nORM)   | SQLite | PostgreSQL | SQL Server | MySQL |
|----------------------------|-------:|-----------:|-----------:|------:|
| Simple query               | 23.2 µs | 46.8 µs | 57.1 µs | 202.3 µs |
| Complex query (filter/order/skip/take) | 64.1 µs | 154.0 µs | 242.9 µs | 568.4 µs |
| Join query                 | 49.5 µs | 106.6 µs | 160.5 µs | 314.2 µs |
| Count                      | 15.2 µs | 131.7 µs | 85.7 µs | 253.4 µs |
| Single insert †            | 54.2 µs | 129.0 µs | 110.9 µs | 1,692.1 µs |
| Bulk insert (idiomatic `BulkInsertAsync`) | 657 µs | 963 µs | 4,353 µs | 6,065 µs |

### How that compares (baseline category named per governance)

- **Fastest read path on every provider is a nORM method.** Simple query beats the
  fastest optimized Raw ADO baseline on SQLite (23.2 vs 29.5 µs `RawAdo_Optimized`),
  PostgreSQL (46.8 vs 112.7 µs), and SQL Server (57.1 vs 82.5 µs), and stays within
  range on MySQL (202.3 vs 212.7 µs `RawAdo_PreparedOptimized`).
- **Joins and complex reads** match or beat typed Dapper and optimized Raw ADO on
  every provider (e.g. PostgreSQL join 106.6 µs vs 211.2 µs `RawAdo_Optimized`;
  SQL Server complex 242.9 µs ≈ Dapper 243.6 µs).
- **Single insert** is the fastest measured path on every provider - SQLite
  (54.2 µs vs Raw ADO 59.1, Dapper 60.0, EF 76.8, all under *equalized* durability
  settings), PostgreSQL (129.0 µs vs Raw ADO 212.4), SQL Server (110.9 µs vs
  Raw ADO 135.0), and MySQL (1,692 µs vs Raw ADO 1,757, Dapper 1,797, EF 2,925).
- **Idiomatic bulk insert** is **2.4×–4.6× faster** than EF `AddRange` / Dapper-in-
  transaction across providers (PostgreSQL 963 µs vs EF 4,483 µs; MySQL 6,065 µs vs
  Dapper 20,226 µs).

> † **SQLite single-insert is measured under equalized durability.** Every
> compared connection (nORM, EF Core, Dapper, Raw ADO) uses identical
> `journal_mode = WAL` / `synchronous = NORMAL` / `busy_timeout` settings, per
> [benchmark governance](docs/benchmark-governance.md), so the row reflects
> data-layer overhead rather than mismatched fsync policy. (Earlier runs showed a
> misleading ~7× SQLite insert advantage purely from durability mismatch; that
> artifact is gone.) MySQL's high absolute latency is the local server/connector
> round-trip and fsync cost - Raw ADO.NET is equally slow there - not nORM
> overhead.

Raw ADO.NET baselines are labeled precisely: `Convenience` (name lookup /
conversion helpers), `Optimized` (ordinal-based typed getters), and
`PreparedOptimized` (prepared command reuse plus the optimized reader). Any public
performance claim must name the exact category, provider, and benchmark artifact,
and must come from generated BenchmarkDotNet reports - run
`eng/run-benchmark-isolated.ps1 -- --provider-matrix` (see
[benchmark governance](docs/benchmark-governance.md)) rather than hand-copying
numbers.

## Installation

```bash
dotnet add package nORM
```

## Quick Start

### Familiar EF Core-Style Setup

```csharp
using nORM.Core;
using nORM.Providers;

var provider = new SqlServerProvider();
var context = new DbContext("Server=.;Database=MyApp;Trusted_Connection=true", provider);

// Define entities exactly like EF Core
public class User
{
    [Key]
    public int Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public DateTime CreatedAt { get; set; }

    // Navigation properties are supported for documented relationship shapes
    public virtual ICollection<Order> Orders { get; set; }
}
```

### LINQ Queries

```csharp
// Familiar EF Core-style syntax on provider-tested query paths
var users = await context.Query<User>()
    .Where(u => u.Name.StartsWith("John"))
    .Include(u => u.Orders)
    .OrderBy(u => u.CreatedAt)
    .ToListAsync();

// Complex queries remain composable and benchmarked across providers
var userStats = await context.Query<User>()
    .Where(u => u.CreatedAt > DateTime.Now.AddDays(-30))
    .GroupBy(u => u.CreatedAt.Date)
    .Select(g => new { Date = g.Key, Count = g.Count() })
    .ToListAsync();

// Compiled queries for warm-path execution
var getActiveUsers = Norm.CompileQuery<MyContext, DateTime, User>(
    (ctx, since) => ctx.Query<User>()
        .Where(u => u.CreatedAt > since)
);
```

### CRUD Operations

> **Note:** nORM is async-first for writes. Only `SaveChangesAsync()` is provided; there is no synchronous `SaveChanges()`. Use `await ctx.SaveChangesAsync()` in all contexts, including console apps: `ctx.SaveChangesAsync().GetAwaiter().GetResult()` can cause deadlocks in some synchronization contexts. Synchronous query helpers such as `ToListSync()` and `CountSync()` remain supported for legacy synchronous callers; see [Sync and Async Policy](docs/sync-policy.md).

```csharp
// Same familiar API as EF Core
var user = new User { Name = "John Doe", Email = "john@example.com" };
await context.InsertAsync(user); // Auto-populates Id

user.Email = "newemail@example.com";
await context.UpdateAsync(user);

await context.DeleteAsync(user);
```

### Bulk Operations

```csharp
// Process thousands of records efficiently
var users = GenerateUsers(10000);
await context.BulkInsertAsync(users);

await context.BulkUpdateAsync(modifiedUsers);
await context.BulkDeleteAsync(usersToDelete);
```

Bulk operation semantics, fallback/native provider paths, transactions, tenant
checks, and cache invalidation are part of the v1 contract. See
[Bulk Operation Contract](docs/bulk-operations.md).

## Advanced Features

### Zero-Configuration Database Discovery

```csharp
// Query any table without defining entity classes
var users = await context.Query("Users").ToListAsync();

// Preview: scaffold tables and columns from an existing database
await DatabaseScaffolder.ScaffoldAsync(connection, provider, outputDir, "MyApp.Entities");
```

Scaffolding is preview in v1: table and column reverse engineering is supported,
but relationship/index generation remains explicit post-processing. See
[Scaffolding Preview Contract](docs/scaffolding.md).

### Modern SQL Features

```csharp
// JSON querying
var products = await context.Query<Product>()
    .Where(p => Json.Value<string>(p.Metadata, "$.category") == "electronics")
    .ToListAsync();

// Window functions
var ranked = await context.Query<Sale>()
    .WithRowNumber((sale, rowNum) => new { sale.Amount, RowNumber = rowNum })
    .WithRank((sale, rank) => new { sale.Amount, Rank = rank })
    .ToListAsync();
```

### Enterprise Features

```csharp
// Multi-tenancy with automatic filtering
var options = new DbContextOptions
{
    TenantProvider = new HttpContextTenantProvider(),
    TenantColumnName = "TenantId"
};

// Advanced caching with invalidation
var products = await context.Query<Product>()
    .Where(p => p.Category == "Electronics")
    .Cacheable(TimeSpan.FromMinutes(30))
    .ToListAsync();

// Retry policies for resilience
options.RetryPolicy = new RetryPolicy
{
    MaxRetries = 3,
    BaseDelay = TimeSpan.FromSeconds(1)
};
```

Multi-tenancy is enforced on ORM-generated query and write paths. Raw SQL,
stored procedures, migrations, scaffolding, and direct connection access are
caller-controlled privileged paths. See the
[Multi-Tenancy Security Contract](docs/multi-tenancy-security.md),
[Tenant Boundary](docs/tenant-boundary.md),
[Tenant Deployment Patterns](docs/tenant-deployment-patterns.md), and
[Tenant Database-Native RLS](docs/tenant-database-native-rls.md).

### Temporal Queries & Versioning

```csharp
// Query historical data
var historical = await context.Query<Product>()
    .AsOf(DateTime.Parse("2023-01-01"))
    .ToListAsync();

// Tag-based versioning
await context.CreateTagAsync("release-v1.0");
var releaseData = await context.Query<Product>()
    .AsOf("release-v1.0")
    .ToListAsync();
```

Temporal versioning is implemented with nORM-managed history tables and
provider-specific triggers. See [Temporal Versioning](docs/temporal-versioning.md)
and [Temporal Precision](docs/temporal-precision.md) for the stable v1
contract.

## Database Providers

Supported database engines (see [Provider Capabilities](docs/provider-capabilities.md) for version requirements and feature-level differences):

```csharp
// SQL Server
var provider = new SqlServerProvider();

// PostgreSQL
var provider = new PostgresProvider();

// SQLite
var provider = new SqliteProvider();

// MySQL
var provider = new MySqlProvider();
```

Install `nORM` for all providers. SQL Server and SQLite drivers are included by
the runtime package; PostgreSQL requires `Npgsql`, and MySQL requires either
`MySqlConnector` or `MySql.Data`. See [Provider Packages](docs/provider-packages.md)
for the locked v1 package/dependency contract and
[Provider Capabilities](docs/provider-capabilities.md) for version and feature
support.

## Raw SQL & Stored Procedures

```csharp
// Prefer interpolated raw SQL so values are parameterized automatically
var users = await context.FromSqlInterpolatedAsync<User>(
    $"SELECT * FROM Users WHERE CreatedAt > {DateTime.Now.AddDays(-7)}");

// Raw SQL is still available when parameter names need to be explicit
var recent = await context.FromSqlRawAsync<User>(
    "SELECT * FROM Users WHERE CreatedAt > @p0",
    DateTime.Now.AddDays(-7));

// Stored procedures
var results = await context.ExecuteStoredProcedureAsync<UserStats>(
    "sp_GetUserStatistics",
    new { StartDate = DateTime.Now.AddMonths(-1) });
```

Raw query APIs are read-only `SELECT`/CTE APIs with provider-aware validation.
Stored procedures and direct connection access are privileged paths. See
[Raw SQL Security](docs/raw-sql-security.md) and
[Stored Procedure Security](docs/stored-procedure-security.md). SQL diagnostics
redact string literals and parameter values by default; see
[Logging and Redaction](docs/logging-redaction.md).
Stable exception categories for query, provider, timeout, configuration, usage,
unsupported-feature, and concurrency failures are documented in
[Exception Taxonomy](docs/exception-taxonomy.md).

## Database Migrations

```csharp
public class CreateUsersTable : Migration
{
    public CreateUsersTable() : base(20240101001, "CreateUsersTable") { }

    public override void Up(DbConnection connection, DbTransaction transaction)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = @"
            CREATE TABLE Users (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                Name NVARCHAR(255) NOT NULL,
                Email NVARCHAR(255) NOT NULL,
                CreatedAt DATETIME2 NOT NULL
            )";
        cmd.ExecuteNonQuery();
    }
}

// Apply migrations
var runner = new SqlServerMigrationRunner(connection, Assembly.GetExecutingAssembly());
await runner.ApplyMigrationsAsync();
```

### Migration Notes

> **Column renames - annotate with `[RenameColumn]`.** nORM cannot infer that a dropped column and a
> new column are a rename from the diff alone, so without help it would emit a destructive DROP + ADD
> pair. The supported v1 workflow is to annotate the renamed property with `[RenameColumn("OldName")]`
> before the next `norm migrations add` run. The schema differ then matches the new property to the
> old column and emits a provider-correct rename entry in `SchemaDiff.RenamedColumns` instead of
> dropping data.
>
> ```csharp
> using nORM.Mapping;
>
> // Property was: public decimal TotalCost { get; set; }
> // Renaming the property without [RenameColumn] -> DROP TotalCost + ADD TotalAmount -> DATA LOSS.
>
> // Correct: annotate the renamed property with its previous column name.
> [RenameColumn("TotalCost")]
> public decimal TotalAmount { get; set; }
> ```
>
> The generated migration uses each provider's native rename syntax automatically:
> - SQL Server: `EXEC sp_rename N'[Orders].[TotalCost]', N'TotalAmount', 'COLUMN'`
> - PostgreSQL / SQLite 3.25+ / MySQL 8.0+: `ALTER TABLE Orders RENAME COLUMN TotalCost TO TotalAmount`
>
> **Unannotated renames still produce DROP + ADD.** `norm migrations add` refuses to write destructive
> column or table drops unless `--force` is supplied, and forced migrations include TODO warnings so an
> accidental rename is hard to miss in review.
>
> **Table renames are not yet auto-detected.** Rename the entity in code and write a manual migration
> that issues the provider's table-rename statement (`sp_rename` on SQL Server,
> `ALTER TABLE ... RENAME TO ...` elsewhere).
>
> nORM also detects **migration class name drift** - renaming the C# migration class after it has
> already been applied - and throws at startup. Combined with `[RenameColumn]`, the only remaining
> rename shape that requires a manual migration is the table rename above.

**Concurrent deployments (SQL Server / MySQL / Postgres).** The runners acquire a database-level advisory
lock before reading the pending list, serializing concurrent deployments automatically:

| Provider | Mechanism |
|---|---|
| SQLite | `BEGIN EXCLUSIVE` transaction |
| SQL Server | `sp_getapplock` (Session scope, 30 s timeout) |
| MySQL | `GET_LOCK('__NormMigrationsLock', 30)` |
| PostgreSQL | `pg_try_advisory_lock(key)` retry loop (session level, 30 s timeout) |

No application-level coordination is required. On SQL Server, DDL is transactional. If the second process
somehow races past the lock, the PK constraint on the history table prevents double-recording and the whole
transaction rolls back cleanly. On MySQL, DDL auto-commits per step; the advisory lock prevents the race
entirely. For the full provider contract, cancellation behavior, and custom migration options, see
[Migration Provider Contract](docs/migration-provider-contract.md).

`norm migrations add` uses `INormDesignTimeDbContextFactory<TContext>` when one
is present, so fluent mapping is included in generated migration snapshots. Use
`--attribute-only` only when you intentionally want to ignore fluent model
configuration. See [Design-Time Migrations](docs/design-time-migrations.md).

`norm database drop` is guarded: destructive execution requires `--yes`, and
`--dry-run` previews the files or tables that would be removed. The tool refuses
to drop known system databases such as `master`, `postgres`, `mysql`, and
`information_schema`.

Transaction ownership, ambient `TransactionScope` policy, savepoints, and
commit/rollback cancellation behavior are documented in
[Transaction Contract](docs/transactions.md).

## v1 Features

### Thread Safety

**`DbContext` is not thread-safe.** Use one context per request, operation, or unit of work. For ASP.NET Core, register it as `Scoped` so each HTTP request gets its own instance:

```csharp
// ASP.NET Core - correct DI registration
builder.Services.AddScoped(sp =>
    new DbContext(connectionString, new SqlServerProvider()));

// Incorrect - do NOT share a single context across requests
builder.Services.AddSingleton<DbContext>(...);  // data races
```

For long-running background jobs that process many entities in a loop, call `ctx.ChangeTracker.Clear()` periodically to release tracked entity memory (approximately 300 bytes per tracked entity):

```csharp
foreach (var batch in items.Chunk(500))
{
    foreach (var item in batch) { /* process */ }
    await ctx.SaveChangesAsync();
    ctx.ChangeTracker.Clear();  // release snapshots before next batch
}
```

### MySQL Optimistic Concurrency

`MySqlProvider` defaults to affected-row semantics, which is required by most MySQL connectors. MySQL's `affected-rows` count does not distinguish "row matched but value unchanged" from "no row matched". For v1, nORM refuses timestamp-tracked MySQL updates by default when affected-row semantics are active. This fail-fast gate is controlled by `DbContextOptions.RequireMatchedRowOccSemantics`, which defaults to `true`.

For the full MySQL optimistic-concurrency guarantee, configure the driver for matched-row semantics with `UseAffectedRows=false` and construct `new MySqlProvider(useAffectedRowsSemantics: false)`. Applications may instead set `RequireMatchedRowOccSemantics=false` to accept the weaker affected-row mode; in that opt-in mode nORM uses a **SELECT-then-verify** fallback for zero-row updates.

**Residual gap**: if a concurrent writer sets the concurrency token to the *same* new value (same-value token conflict), neither the UPDATE rowcount nor the SELECT verification can detect the conflict because the WHERE clause still matches. This edge case requires application-level versioning (e.g. monotonically increasing versions) to close. To eliminate the gap entirely, use `UseAffectedRows=false` in the connection string with a connector that supports it and construct `new MySqlProvider(useAffectedRowsSemantics: false)`.

**DELETE path**: DELETE rowcount is always checked regardless of `UseAffectedRowsSemantics`, because deleting a row always counts it as affected. There is no same-value ambiguity for deletes.

The full provider-specific contract is documented in
[Optimistic Concurrency](docs/optimistic-concurrency.md).
Source generator package boundaries and runtime attribute ownership are
documented in [Source Generation](docs/source-generation.md).
Production setup, connection pooling, retries, transactions, migrations,
multi-tenancy, logging, and troubleshooting are covered in
[Production Operations](docs/production-operations.md).

### Connection Management & Pooling

```csharp
var builder = new SqlConnectionStringBuilder(connectionString)
{
    Pooling = true,
    MaxPoolSize = 50,
    MinPoolSize = 5
};

await using var connection = new SqlConnection(builder.ConnectionString);
await connection.OpenAsync(cancellationToken);

await using var context = new DbContext(
    connection,
    new SqlServerProvider(),
    new DbContextOptions());
```

### Interceptors & Extensibility

```csharp
public sealed class LoggingInterceptor : BaseDbCommandInterceptor
{
    public LoggingInterceptor(ILogger<LoggingInterceptor> logger) : base(logger) { }

    public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command,
        DbContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("Command executing");
        return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
    }
}

options.CommandInterceptors.Add(new LoggingInterceptor(NullLogger<LoggingInterceptor>.Instance));
```

Interceptor ordering, suppression, failure, cancellation, and redaction behavior
are part of the v1 contract. See [Interceptor Contract](docs/interceptors.md).

### Global Query Filters

```csharp
// Automatically apply filters like soft deletes
options.AddGlobalFilter<ISoftDeletable>(e => !e.IsDeleted);
```

## Migration from Entity Framework Core

nORM intentionally uses familiar EF Core-style concepts, but it is not a
binary-compatible EF Core provider and it does not implement every EF Core LINQ,
tracking, lazy-loading, or migrations behavior. Treat migration as a deliberate
port:

1. **DbContext-style patterns** - Context and query entry points are familiar, but constructors and options differ.
2. **Documented LINQ subset** - Query shapes must be checked against [the LINQ support matrix](docs/linq-support.md).
3. **Compatible common attributes** - `[Key]`, `[Table]`, and `[Column]` are supported for standard mapping cases.
4. **Fluent API with nORM semantics** - Configuration is similar in shape, not a one-for-one EF Core clone.
5. **DI integration** - Register contexts and providers explicitly for the target database.

### Simple Migration Example

```csharp
// Before (EF Core)
public class MyDbContext : DbContext
{
    public DbSet<User> Users { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseSqlServer(connectionString);
}

// After (nORM)
public class MyDbContext : nORM.Core.DbContext
{
    public MyDbContext() : base(connectionString, new SqlServerProvider()) { }

    // Same entity access patterns
    public IQueryable<User> Users => Query<User>();
}
```

## Performance Design

- **Advanced IL Materialization**: IL-generated materializers avoid reflection on tuned paths
- **Low-Allocation Query Execution**: Memory-conscious query processing reduces avoidable GC pressure
- **Intelligent Caching**: Multi-layered caching strategy for query plans and metadata
- **Driver Pooling**: Uses ADO.NET provider-native pooling instead of a custom public pool
- **Native Bulk Operations**: Database-specific bulk operation implementations
- **Compiled Query Support**: Pre-compile frequently used queries for warm-path reuse

## Performance Targets

nORM is being tuned toward these release goals:

- **Query latency**: keep simple, joined, and complex read paths inside the versioned benchmark budgets
- **Compiled queries**: keep warm-path execution inside the compiled-query benchmark budgets
- **Memory usage**: keep read-heavy allocations inside the versioned benchmark budgets
- **Bulk operations**: keep idiomatic `BulkInsertAsync` inside the provider-specific benchmark budgets

Benchmark claims must follow the reproducibility and baseline rules in
[Benchmark Governance](docs/benchmark-governance.md).

## Contributing

Before public release, use focused issues or pull requests with a clear reproduction, test coverage, and benchmark data when performance is involved.
Repository artifact and encoding rules are documented in
[Repository Hygiene](docs/repository-hygiene.md).
Project process is documented in [CONTRIBUTING.md](CONTRIBUTING.md),
[SECURITY.md](SECURITY.md), [SUPPORT.md](SUPPORT.md), [CHANGELOG.md](CHANGELOG.md),
and the [Release Checklist](docs/release-checklist.md).
The v1 blocker execution map is tracked in [v1 Issue Map](docs/v1-issue-map.md).

### Development Setup

1. Clone the repository
2. Install the .NET SDK pinned by `global.json` (`8.0.417` for the current v1 gate)
3. Run `dotnet restore`
4. Run `dotnet build`
5. Run tests: `dotnet test`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with performance lessons learned from Entity Framework Core and Dapper
- Optimized for the modern .NET ecosystem
- Designed for production workloads; see [benchmark governance](docs/benchmark-governance.md) for performance evidence requirements

## Support

-  [Documentation](https://github.com/zilverztream/nORM/wiki)
-  [Issues](https://github.com/zilverztream/nORM/issues)
-  [Discussions](https://github.com/zilverztream/nORM/discussions)

---

*nORM - familiar ORM ergonomics with provider-tested LINQ and benchmark-governed performance*
