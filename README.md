# nORM (The Norm) - High-Performance ORM for .NET

nORM is a modern Object-Relational Mapping (ORM) library for .NET that is being tuned for low-overhead hot paths while keeping familiar ORM features such as LINQ queries, change tracking, migrations, multi-tenancy, and provider-specific bulk operations.

## Why Choose nORM?

- **Performance-focused**: compiled queries, native bulk operations, and BenchmarkDotNet suites for comparing tuned paths
- **Documented LINQ Support**: Provider-tested LINQ support for common query shapes, with explicit limits documented in [the LINQ support matrix](docs/linq-support.md)
- **Explicit Deployment Boundaries**: JIT-first runtime with source-generation support and documented [AOT/trimming limits](docs/aot-trimming.md)
- **Bounded Cache Policy**: Process-wide and per-context caches have documented lifetimes, limits, and diagnostics in [the cache policy](docs/cache-policy.md)
- **Familiar API**: EF Core-style context, configuration, and change-tracking patterns
- **Bulk Operations**: provider-specific bulk insert, update, and delete operations with documented semantics
- **Advanced Query Capabilities**: Raw SQL, stored procedures, and compiled queries
- **Connection Management**: context-level connection ownership plus database-driver pooling
- **Multi-Database Support**: SQL Server, PostgreSQL, SQLite, and MySQL
- **Smart Relationship Handling**: Automatic relationship discovery and lazy loading
- **Flexible Configuration**: Fluent API and attribute-based configuration
- **Developer Tools**: Preview database scaffolding and reverse engineering
- **Modern Features**: JSON querying, window functions, temporal queries
- **Operational Features**: Multi-tenancy, caching, retry policies, and interceptors with explicit support contracts

## Performance Validation

nORM ships with BenchmarkDotNet suites that compare EF Core, Dapper, Raw ADO.NET, and nORM across SQLite, SQL Server, PostgreSQL, and MySQL. Release decisions should be based on fresh local or CI benchmark output, not hard-coded numbers in this README. All benchmark paths are expected to use the same seeded schema, equivalent SQL shape, typed result materialization, and comparable compiled/prepared modes. Raw ADO.NET results are split into explicit convenience and optimized categories so public claims do not blur easy handwritten ADO with ordinal-based, typed-getter ADO.

### Read Queries

| Operation                        | Compared modes |
|----------------------------------|----------------|
| Simple query                     | runtime, compiled/prepared, Dapper, Raw ADO convenience/optimized/prepared-optimized |
| Complex query                    | runtime, compiled/prepared, Dapper, Raw ADO convenience/optimized/prepared-optimized |
| JOIN query                       | runtime, compiled, typed Dapper, Raw ADO optimized |
| Count                            | runtime, compiled/prepared, Dapper, Raw ADO optimized |

### Write Operations

| Operation                        | Compared modes |
|----------------------------------|----------------|
| Single insert                    | EF Core, Dapper, Raw ADO, nORM |
| Bulk insert - naive              | EF Core, Dapper, Raw ADO, nORM |
| Bulk insert - batched/prepared   | EF Core, Dapper, Raw ADO, nORM |
| Bulk insert - idiomatic API      | provider-native nORM path |

> nORM's compiled query cache is intended to deliver the largest gains on warm paths. Performance-sensitive work should run `benchmarks/nORM.Benchmarks.csproj` before release decisions; generated BenchmarkDotNet output is intentionally not tracked in source control.

Raw ADO.NET labels are precise: `Convenience` means straightforward handwritten ADO using name lookup/conversions, `Optimized` means ordinal-based typed getters where provider values permit them, and `PreparedOptimized` means prepared command reuse plus the optimized reader path. Any public performance comparison against Raw ADO.NET must name the exact category, provider, and benchmark artifact.

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

### High-Performance LINQ Queries

```csharp
// Familiar EF Core-style syntax with lower runtime overhead on tuned paths
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

// Compiled queries for maximum performance
var getActiveUsers = Norm.CompileQuery<MyContext, DateTime, User>(
    (ctx, since) => ctx.Query<User>()
        .Where(u => u.CreatedAt > since)
);
```

### Lightning-Fast CRUD Operations

> **Note:** nORM is async-first for writes. Only `SaveChangesAsync()` is provided; there is no synchronous `SaveChanges()`. Use `await ctx.SaveChangesAsync()` in all contexts, including console apps: `ctx.SaveChangesAsync().GetAwaiter().GetResult()` can cause deadlocks in some synchronization contexts. Synchronous query helpers such as `ToListSync()` and `CountSync()` remain supported for legacy synchronous callers; see [Sync and Async Policy](docs/sync-policy.md).

```csharp
// Same familiar API as EF Core
var user = new User { Name = "John Doe", Email = "john@example.com" };
await context.InsertAsync(user); // Auto-populates Id

user.Email = "newemail@example.com";
await context.UpdateAsync(user);

await context.DeleteAsync(user);
```

### Superior Bulk Operations

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
[Multi-Tenancy Security Contract](docs/multi-tenancy-security.md).

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
for the stable v1 contract.

## Database Providers

Full support for major database engines:

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

> **DATA LOSS WARNING - Column renames.** nORM cannot prove that a dropped column and a new column are
> a rename. Renaming `Order.TotalCost` to `Order.TotalAmount` produces a migration diff that **drops
> `TotalCost` and adds `TotalAmount`**. `norm migrations add` refuses to write table/column-drop
> migrations unless `--force` is supplied, and forced migrations include TODO warnings. Always replace
> rename-like drops/adds with a manual rename operation before applying the migration:
>
> ```csharp
> // WRONG - renames the C# property directly -> DROP + ADD -> DATA LOSS
> // public decimal TotalAmount { get; set; }   // was TotalCost
>
> // CORRECT - write a manual migration that renames the column
> public class RenameTotalCostToTotalAmount : Migration
> {
>     public RenameTotalCostToTotalAmount() : base(20240201001, "RenameTotalCostToTotalAmount") { }
>
>     public override void Up(DbConnection connection, DbTransaction transaction)
>     {
>         using var cmd = connection.CreateCommand();
>         cmd.Transaction = transaction;
>         // SQL Server:
>         cmd.CommandText = "EXEC sp_rename 'Orders.TotalCost', 'TotalAmount', 'COLUMN'";
>         // PostgreSQL / SQLite 3.25+:
>         // cmd.CommandText = "ALTER TABLE Orders RENAME COLUMN TotalCost TO TotalAmount";
>         // MySQL:
>         // cmd.CommandText = "ALTER TABLE Orders RENAME COLUMN TotalCost TO TotalAmount";
>         cmd.ExecuteNonQuery();
>     }
> }
> ```
>
> nORM *does* detect **migration class name drift** (renaming the C# migration class after it has already been
> applied) and throws at startup. Only property-to-column renames are undetected.

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

## Production-Ready Features

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

- **Advanced IL Materialization**: Hand-optimized IL generation eliminates reflection overhead
- **Zero-Allocation Query Execution**: Memory-efficient query processing reduces GC pressure  
- **Intelligent Caching**: Multi-layered caching strategy for query plans and metadata
- **Driver Pooling**: Uses ADO.NET provider-native pooling instead of a custom public pool
- **Native Bulk Operations**: Database-specific bulk operation implementations
- **Compiled Query Support**: Pre-compile frequently used queries for maximum speed

## Performance Targets

nORM is being tuned toward these release goals:

- **Query latency**: stay competitive with Dapper on simple and joined reads
- **Compiled queries**: make warm-path execution consistently faster than uncompiled LINQ
- **Memory usage**: keep allocations well below EF Core on read-heavy paths
- **Bulk operations**: remain substantially faster than EF Core and competitive with Dapper transaction-based inserts

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
- Designed for enterprise production workloads

## Support

-  [Documentation](https://github.com/zilverztream/nORM/wiki)
-  [Issues](https://github.com/zilverztream/nORM/issues)  
-  [Discussions](https://github.com/zilverztream/nORM/discussions)

---

*nORM - Entity Framework performance, without the Entity Framework overhead* 
