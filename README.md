# nORM (The Norm) - High-Performance ORM for .NET

nORM is a modern, high-performance Object-Relational Mapping (ORM) library for .NET that targets Dapper-competitive hot paths and substantially lower overhead than Entity Framework Core while maintaining the developer-friendly features expected from a modern ORM. Built for enterprise applications that demand both developer productivity and performance.

## Why Choose nORM?

- **Fast by design**: Dapper-competitive read paths, compiled queries, and native bulk operations
- **Documented LINQ Support**: Provider-tested LINQ support for common query shapes, with explicit limits documented in [the LINQ support matrix](docs/linq-support.md)
- **Explicit Deployment Boundaries**: JIT-first runtime with source-generation support and documented [AOT/trimming limits](docs/aot-trimming.md)
- **Bounded Cache Policy**: Process-wide and per-context caches have documented lifetimes, limits, and diagnostics in [the cache policy](docs/cache-policy.md)
- **Familiar API**: EF Core-style context, configuration, and change-tracking patterns
- **Enterprise-Grade Bulk Operations**: High-performance bulk insert, update, and delete operations
- **Advanced Query Capabilities**: Raw SQL, stored procedures, and compiled queries
- **Intelligent Connection Management**: Built-in pooling and connection optimization
- **Multi-Database Support**: SQL Server, PostgreSQL, SQLite, and MySQL
- **Smart Relationship Handling**: Automatic relationship discovery and lazy loading
- **Flexible Configuration**: Fluent API and attribute-based configuration
- **Developer Tools**: Database scaffolding and reverse engineering
- **Modern Features**: JSON querying, window functions, temporal queries
- **Enterprise Ready**: Multi-tenancy, caching, retry policies, and interceptors

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

Raw ADO.NET labels are precise: `Convenience` means straightforward handwritten ADO using name lookup/conversions, `Optimized` means ordinal-based typed getters where provider values permit them, and `PreparedOptimized` means prepared command reuse plus the optimized reader path. Any public "beats Raw ADO" claim must name the exact category, provider, and benchmark artifact.

## Installation

```bash
dotnet add package nORM
```

## Quick Start

### Familiar EF Core-Style Setup

```csharp
using nORM.Core;
using nORM.Providers;

// Drop-in replacement for EF Core setup
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
    
    // Navigation properties work identically
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

## Advanced Features

### Zero-Configuration Database Discovery

```csharp
// Query any table without defining entity classes
var users = await context.Query("Users").ToListAsync();

// Scaffold entire database automatically
await DatabaseScaffolder.ScaffoldAsync(connection, provider, outputDir, "MyApp.Entities");
```

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
for the locked v1 package/dependency contract.

## Raw SQL & Stored Procedures

```csharp
// Raw SQL with type safety
var users = await context.FromSqlRawAsync<User>(
    "SELECT * FROM Users WHERE CreatedAt > @date", 
    DateTime.Now.AddDays(-7));

// Stored procedures
var results = await context.ExecuteStoredProcedureAsync<UserStats>(
    "sp_GetUserStatistics",
    new { StartDate = DateTime.Now.AddMonths(-1) });
```

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

> **DATA LOSS WARNING - Column renames.** nORM cannot detect when you rename a C# property. Renaming
> `Order.TotalCost` to `Order.TotalAmount` generates a migration diff that **drops `TotalCost` and adds
> `TotalAmount`**, destroying all column data silently. Always create a manual migration instead:
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
| PostgreSQL | `pg_advisory_lock(key)` (session level, blocks until available) |

No application-level coordination is required. On SQL Server, DDL is transactional. If the second process
somehow races past the lock, the PK constraint on the history table prevents double-recording and the whole
transaction rolls back cleanly. On MySQL, DDL auto-commits per step; the advisory lock prevents the race
entirely. If you need a bounded wait on PostgreSQL (which blocks indefinitely), wrap `ApplyMigrationsAsync`
in a `CancellationTokenSource` with a timeout.

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

`MySqlProvider` defaults to `UseAffectedRowsSemantics = true`, which is required by most MySQL connectors. MySQL's `affected-rows` count does not distinguish "row matched but value unchanged" from "no row matched". nORM handles this with a **SELECT-then-verify** fallback: when an UPDATE returns fewer rows than expected, nORM queries the database to check whether the original concurrency tokens still exist. If they do, the update was a same-value no-op and no conflict is raised. If any token has changed, a `DbConcurrencyException` is thrown as expected.

**Residual gap**: if a concurrent writer sets the concurrency token to the *same* new value (same-value token conflict), neither the UPDATE rowcount nor the SELECT verification can detect the conflict because the WHERE clause still matches. This edge case requires application-level versioning (e.g. monotonically increasing versions) to close. To eliminate the gap entirely, use `useAffectedRows=false` in the connection string with a connector that supports it, or subclass `MySqlProvider` and override `UseAffectedRowsSemantics` to `false`.

**DELETE path**: DELETE rowcount is always checked regardless of `UseAffectedRowsSemantics`, because deleting a row always counts it as affected. There is no same-value ambiguity for deletes.

### Connection Management & Pooling

```csharp
var pool = new ConnectionPool(
    () => new SqlConnection("Server=.;Database=MyApp;Trusted_Connection=true"),
    new ConnectionPoolOptions
    {
        MaxPoolSize = 50,
        MinPoolSize = 5,
        ConnectionIdleLifetime = TimeSpan.FromMinutes(5)
    });
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
        Console.WriteLine(command.CommandText);
        return Task.FromResult(InterceptionResult<DbDataReader>.Continue());
    }
}

options.CommandInterceptors.Add(new LoggingInterceptor(NullLogger<LoggingInterceptor>.Instance));
```

### Global Query Filters

```csharp
// Automatically apply filters like soft deletes
options.AddGlobalFilter<ISoftDeletable>(e => !e.IsDeleted);
```

## Migration from Entity Framework Core

nORM is designed as a drop-in replacement for EF Core:

1. **Same DbContext patterns** - Minimal code changes required
2. **Identical LINQ syntax** - Your existing queries work unchanged  
3. **Compatible attribute model** - `[Key]`, `[Table]`, `[Column]` attributes work identically
4. **Familiar fluent API** - Model configuration uses the same patterns
5. **Same dependency injection** - Integrate with your existing DI container

### Simple Migration Example

```csharp
// Before (EF Core)
public class MyDbContext : DbContext
{
    public DbSet<User> Users { get; set; }
    
    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseSqlServer(connectionString);
}

// After (nORM) - Almost identical!
public class MyDbContext : nORM.Core.DbContext
{
    public MyDbContext() : base(connectionString, new SqlServerProvider()) { }
    
    // Same entity access patterns
    public IQueryable<User> Users => Query<User>();
}
```

## Why nORM Outperforms EF Core

- **Advanced IL Materialization**: Hand-optimized IL generation eliminates reflection overhead
- **Zero-Allocation Query Execution**: Memory-efficient query processing reduces GC pressure  
- **Intelligent Caching**: Multi-layered caching strategy for query plans and metadata
- **Optimized Connection Management**: Efficient connection pooling and reuse
- **Native Bulk Operations**: Database-specific bulk operation implementations
- **Compiled Query Support**: Pre-compile frequently used queries for maximum speed

## Performance Targets

nORM is being tuned toward these release goals:

- **Query latency**: stay competitive with Dapper on simple and joined reads
- **Compiled queries**: make warm-path execution consistently faster than uncompiled LINQ
- **Memory usage**: keep allocations well below EF Core on read-heavy paths
- **Bulk operations**: remain substantially faster than EF Core and competitive with Dapper transaction-based inserts

## Contributing

Before public release, use focused issues or pull requests with a clear reproduction, test coverage, and benchmark data when performance is involved.

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
