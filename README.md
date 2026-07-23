# nORM - The Provider-Mobile ORM for .NET

> **Naming.** nORM is meant to be *the norm* - the standard ORM you reach for. It
> publishes on NuGet as the **`TheNorm`** package because the `nORM` id is held by
> an unrelated 2011 project; the API namespace and `dotnet norm` CLI keep the `nORM`
> name. So you `dotnet add package TheNorm` and write `using nORM;`.

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
  threshold-gated provider-matrix evidence; see [Performance](#performance)).
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
- **NativeAOT-ready source-generated path**: `[GenerateMaterializer]` entities with `[CompileTimeQuery]` methods read *and* write on a self-contained native binary with no trimmer rooting — the generator emits the metadata-preservation itself. The reflection path stays JIT-first with documented [AOT/trimming boundaries](docs/aot-trimming.md).
- **Bounded cache policy** with documented lifetimes, limits, and diagnostics ([cache policy](docs/cache-policy.md))
- **Multi-database support**: SQL Server, PostgreSQL, SQLite, and MySQL
- **Product-Proof Sample**: `samples/nORM.Sample.Store` is a provider-swappable tenant + temporal web app with a browser frontend, authenticated tenant flow, and a verification mode.

## Sample Store App

Run the product-proof sample locally with SQLite:

```bash
dotnet run --project samples/nORM.Sample.Store -- --provider sqlite
```

The same app can target SQL Server, PostgreSQL, or MySQL by setting
`NORM_SAMPLE_*` or `NORM_TEST_*` connection strings and changing only
`--provider`. The sample demonstrates generated-path tenant boundaries,
representative LINQ, bulk insert, compiled query, `Include()` eager loading
(`AsSplitQuery()` is accepted for EF compatibility), and nORM-managed temporal
`AsOf(tag)`. See
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
numbers below are an evidence snapshot from an earlier threshold-gated provider
matrix run plus the follow-up SQLite single-insert fairness slice after
durability settings were equalized. They are representative, not final release
evidence. Release claims must come from a fresh provider-matrix run on the
release commit, using the job settings and claim rules in
[benchmark governance](docs/benchmark-governance.md).

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

- **Hot read paths are competitive with the fastest measured baselines.** Simple
  query beats the fastest optimized Raw ADO baseline on SQLite (23.2 vs 29.5 µs
  `RawAdo_Optimized`), PostgreSQL (46.8 vs 112.7 µs), and SQL Server (57.1 vs
  82.5 µs), and is within the same local-server band on MySQL (202.3 vs 212.7 µs
  `RawAdo_PreparedOptimized`).
- **Joins and complex reads** match or beat typed Dapper and optimized Raw ADO on
  every provider (e.g. PostgreSQL join 106.6 µs vs 211.2 µs `RawAdo_Optimized`;
  SQL Server complex 242.9 µs ≈ Dapper 243.6 µs).
- **Single insert** is the fastest measured path on every provider - SQLite
  (54.2 µs vs Raw ADO 59.1, Dapper 60.0, EF 76.8, all under *equalized* durability
  settings), PostgreSQL (129.0 µs vs Raw ADO 212.4), SQL Server (110.9 µs vs
  Raw ADO 135.0), and MySQL (1,692 µs vs Raw ADO 1,757, Dapper 1,797, EF 2,925).
- **Single full-column update** exposes nORM's two write models, and each beats its
  fair peer (SQLite, current OrmBenchmarks run under equalized settings). The
  change-**tracked** path (`Update` + `SaveChanges`, the same model EF uses) is
  12.3 KB / 57.0 µs vs EF Core's tracked update 15.9 KB / 72.1 µs. The **direct**
  path (`UpdateAsync`, the same shape Dapper issues) is 5.0 KB / 51.1 µs vs Dapper
  5.4 KB / 49.7 µs - lower allocation, tied latency. The comparison to avoid is
  nORM's tracked path against Dapper's direct one: that is tracked-vs-untracked,
  not a like-for-like write.
- **Idiomatic bulk insert** is **2.4×–4.6× faster** than EF `AddRange` / Dapper-in-
  transaction across providers (PostgreSQL 963 µs vs EF 4,483 µs; MySQL 6,065 µs vs
  Dapper 20,226 µs).
- **Allocation is where the gap is widest, and it is machine-independent** (unlike
  latency, allocation does not move with machine load, so these figures are solid
  regardless of the run environment). Single insert allocates **424 B (PostgreSQL),
  643 B (MySQL), 1.1 KB (SQL Server), 1.6 KB (SQLite)** — roughly **4–10× less than
  Dapper** (4.4–6.2 KB) and **14–44× less than EF Core** (16–29 KB) on every
  provider. Reads are similarly lean (SQLite join: nORM 11–17 KB vs EF Core 60 KB).

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
conversion helpers), `Optimized` (ordinal-based typed getters with portable
conversion helpers when needed), `TypedNoBox` (typed getter allocation floor),
`PreparedOptimized`, and `PreparedTypedNoBox`. Any public performance claim must
name the exact category, provider, and benchmark artifact, and must come from
generated BenchmarkDotNet reports - run
`eng/run-benchmark-isolated.ps1 -- --provider-matrix` (see
[benchmark governance](docs/benchmark-governance.md)) rather than hand-copying
numbers.

## Installation

nORM publishes on NuGet as the **`TheNorm`** package (the `nORM` id is held by an
unrelated 2011 project); the API namespace and `dotnet norm` CLI keep the `nORM` name.

```bash
dotnet add package TheNorm
```

```csharp
using nORM;
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

Primary keys follow the EF Core convention: when an entity has no explicit
`[Key]` attribute or fluent `HasKey`, a property named `Id` or `<TypeName>Id`
becomes the primary key automatically. Explicit configuration always wins.

#### Store-generated keys by convention

Also matching EF Core, a **single-column integer primary key with no explicit
value-generation configuration is store-generated by convention**: the database
generates it when the value is left at its default (`0`), and an explicitly
supplied non-default value is honored (an upsert/seed still works). So on SQLite,
PostgreSQL, and MySQL the `User` above needs no `[DatabaseGenerated]` annotation —
inserting one with `Id` unset produces a fresh key; inserting one with `Id = 42`
keeps `42`. This is realized with each provider's native mechanism: a SQLite
`INTEGER PRIMARY KEY` (rowid alias), PostgreSQL `GENERATED BY DEFAULT AS IDENTITY`,
and MySQL `AUTO_INCREMENT`. **On SQL Server the convention is opt-in** — annotate the
key with `[DatabaseGenerated(DatabaseGeneratedOption.Identity)]` to request a
store-generated `IDENTITY` key — because SQL Server's `SET IDENTITY_INSERT`
requirement for honoring an explicit value would break an existing plain-`INT`
(non-identity) table.

The convention applies only to a single-column integer key with **no** explicit
configuration. To keep a key entirely client-set — for example an existing table
whose key column is *not* an identity/auto-increment column, or a natural key you
assign yourself — opt out in one of two ways:

```csharp
// Per entity: declare the key is not store-generated.
public class LedgerEntry
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.None)]
    public int Id { get; set; }   // client-set; nothing is auto-generated
}

// Globally: turn the convention off for the whole context.
var options = new DbContextOptions { UseStoreGeneratedKeyConvention = false };
```

`[DatabaseGenerated(DatabaseGeneratedOption.Identity)]` and the fluent
`Property(e => e.Id).ValueGeneratedOnAdd()` remain the explicit way to request a
store-generated key and always take precedence over the convention. Composite keys
and non-integer keys (for example `Guid`) are never store-generated by convention.

### LINQ Queries

`context.Query<T>()` opens a queryable. `context.Set<T>()` returns a `DbSet<T>` for
Entity Framework Core muscle memory — an `IQueryable<T>` that *also* exposes the
change-tracking verbs (`Add`/`Remove`/`Update`/`Attach`/`Find`). Expose it as a context
property for EF-style `context.Users` access — a computed property, so no reflection or
auto-population is involved (trimming/NativeAOT-safe):

```csharp
public sealed class AppContext : DbContext
{
    public AppContext(DbConnection cn, DatabaseProvider p) : base(cn, p) { }
    public DbSet<User> Users => this.Set<User>();
}

// then, EF-style:
context.Users.Add(new User { Name = "Ada" });
await context.SaveChangesAsync();
var admins = await context.Users.Where(u => u.IsAdmin).ToListAsync();
```

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

// Compiled terminal queries return the terminal result directly
// (First/Single/Count/Any/Sum/... with LINQ-identical semantics)
var getUserById = Norm.CompileTerminalQuery(
    (MyContext ctx, int id) => ctx.Query<User>().First(u => u.Id == id));
var countSince = Norm.CompileTerminalQuery(
    (MyContext ctx, DateTime since) => ctx.Query<User>().Count(u => u.CreatedAt > since));
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

nORM supports both direct writes (above) and EF Core-style change tracking
(`Add`/`Remove` + `SaveChangesAsync`). See the [write model guide](docs/write-model.md)
for when to use which, and how the two interact.

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

## Dependency Injection (ASP.NET Core)

nORM registers on `IServiceCollection`, so ASP.NET Core and generic-host apps get a
scoped `DbContext` with a container-managed lifetime - one context (and connection) per
request, disposed automatically when the scope ends:

```csharp
using Microsoft.Extensions.DependencyInjection;
using nORM.Core;
using nORM.Providers;

var builder = WebApplication.CreateBuilder(args);

// Registers DbContext as Scoped; a fresh provider + options are built per context.
builder.Services.AddNorm(
    builder.Configuration.GetConnectionString("Default")!,
    () => new SqlServerProvider(),
    options => options.UseInMemoryCache());
```

Inject `DbContext` (or your own derived context) straight into controllers,
minimal-API handlers, or services:

```csharp
app.MapGet("/users/{id:int}", async (int id, DbContext db) =>
    await db.Query<User>().Where(u => u.Id == id).FirstOrDefaultAsync());
```

### Custom context classes

Define your own context deriving from `DbContext`, register it with the generic overload, and inject your type:

```csharp
public sealed class AppDbContext : DbContext
{
    public AppDbContext(string connectionString, DatabaseProvider provider)
        : base(connectionString, provider) { }
}

builder.Services.AddNorm(sp => new AppDbContext(
    builder.Configuration.GetConnectionString("Default")!, new SqlServerProvider()));
```

### Contexts outside a request scope

For singletons, background services, or parallel work, register a factory and create
short-lived, caller-owned contexts on demand:

```csharp
builder.Services.AddNormFactory(sp => new AppDbContext(
    builder.Configuration.GetConnectionString("Default")!, new SqlServerProvider()));

public sealed class Worker(INormDbContextFactory<AppDbContext> factory) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var db = factory.CreateDbContext(); // caller owns and disposes it
        // ... use db ...
    }
}
```

### Context pooling

For high-throughput request pipelines, `AddNormPool<TContext>` reuses a bounded set of
warm contexts instead of building a fresh one per request - the nORM analogue of EF Core's
`AddDbContextPool`:

```csharp
builder.Services.AddNormPool<AppDbContext>(sp => new AppDbContext(
    builder.Configuration.GetConnectionString("Default")!, new SqlServerProvider()),
    poolSize: 1024); // poolSize defaults to 1024; must be > 0
```

The pool is registered as a singleton and the context is injected `Scoped`: each scope
**rents** a warm context and **returns** it when the scope disposes. Renting keeps the
context's warm per-instance caches (entity mappings, prepared commands, fast-path SQL, the
query provider) and its connection, which is where the win comes from. On return nORM resets
all per-request state: it clears the change tracker and identity map, pending relationship
key fixups, and query-scoped resources. Two safety rules are enforced by construction:

- **Tenant isolation:** the applied native tenant-session key is cleared on return, so the
  next lease re-applies its *own* tenant session - a pooled context never inherits the
  previous tenant's row-level-security scope.
- **Transactions are never pooled:** a context holding a live explicit, context, or ambient
  `System.Transactions` transaction is disposed instead of returned, so an open transaction
  can never leak into the next lease.

`AddNorm` and `AddNorm<TContext>` default to `ServiceLifetime.Scoped`; pass a different
`ServiceLifetime` if you need one. Because nORM builds mappings and materializers by
reflection, the connection-string `AddNorm` overload is annotated
`RequiresUnreferencedCode`/`RequiresDynamicCode`; see [AOT & trimming](docs/aot-trimming.md).

## Advanced Features

### Fluent model configuration

Override `OnModelCreating` on a derived context to configure entities with an EF Core-style
fluent builder. Property-level verbs mirror EF Core and feed both the runtime model and the
schema snapshot used by migrations:

```csharp
public sealed class AppDbContext : DbContext
{
    public AppDbContext(string cs, DatabaseProvider p) : base(cs, p) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>(e =>
        {
            e.Property(p => p.Name).IsRequired().HasMaxLength(200);
            e.Property(p => p.Sku).HasColumnType("char(12)");          // explicit store type
            e.Property(p => p.Price).HasPrecision(18, 2);
            e.Property(p => p.CreatedUtc).HasDefaultValueSql("CURRENT_TIMESTAMP");
            e.Property(p => p.Status).HasDefaultValue(ProductStatus.Draft); // literal default
            e.Property(p => p.Slug).HasComment("URL-safe product identifier");
            e.Property(p => p.RowVersion).IsRowVersion();               // concurrency token
            e.Property(p => p.Id).ValueGeneratedOnAdd();               // store-generated on insert
        });
    }
}
```

| Verb | Effect |
| --- | --- |
| `IsRequired(bool = true)` | Marks the column `NOT NULL` (or nullable when `false`). |
| `HasColumnType(string)` | Pins the exact provider store type, bypassing nORM's default CLR-to-store mapping. |
| `HasDefaultValue(object)` | Emits a provider-formatted literal `DEFAULT` at snapshot time (value converters are applied). |
| `HasDefaultValueSql(string)` / `HasDefaultValueSql(string, string)` | Emits a raw SQL `DEFAULT` expression (the two-arg overload supplies a provider-specific variant). |
| `HasComment(string)` | Emits a native column comment per provider (SQL Server extended property, PostgreSQL/MySQL `COMMENT`, SQLite inline `/* */`). |
| `IsRowVersion()` | Declares an 8-byte client-managed concurrency token included in `UPDATE`/`DELETE` predicates. |
| `ValueGeneratedOnAdd()` / `ValueGeneratedNever()` / `ValueGeneratedOnAddOrUpdate()` | Declares whether the store generates the value on insert, never, or on insert and update. |

Reusable, testable configuration classes are supported through
`IEntityTypeConfiguration<TEntity>`, applied individually or by assembly scan:

```csharp
public sealed class ProductConfig : IEntityTypeConfiguration<Product>
{
    public void Configure(EntityTypeBuilder<Product> builder)
    {
        builder.Property(p => p.Name).IsRequired().HasMaxLength(200);
    }
}

protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.ApplyConfiguration(new ProductConfig());
    // or discover every IEntityTypeConfiguration<T> in an assembly:
    modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
}
```

### Zero-Configuration Database Discovery

```csharp
// Query any table without defining entity classes
var users = await context.Query("Users").ToListAsync();

// Scaffold tables, columns, and supported single-column relationships
await DatabaseScaffolder.ScaffoldAsync(connection, provider, outputDir, "MyApp.Entities");
```

Scaffolding is a bounded v1 bootstrap tool: table/column reverse engineering,
schema-preserving table mapping, identifier cleanup, table filtering,
deterministic repeated output, CLI preflighted overwrite protection that
refuses output conflicts by default, project-aware nullable output, provider
metadata-backed identity columns, computed/generated column metadata, SQL
Server rowversion/timestamp metadata, safe SQL default metadata via generated
`HasDefaultValueSql(...)` configuration, single-column/composite FK
navigation generation with supported delete/update referential actions,
one-to-one reference navigations for exact unique dependent FKs, role-based
self-referencing FK and self-join names, pure many-to-many join mapping including
schema-qualified and composite-key junction tables, and
single-column/composite index metadata including descending key order, SQL
Server/PostgreSQL/SQLite filtered index predicates, and SQL Server/PostgreSQL
included-column index metadata are supported, including columns that
participate in multiple indexes. Table CHECK constraints are emitted into
fluent model configuration with `HasCheckConstraint` and migration snapshots.
Generated contexts expose both `DbConnection` and connection-string
constructors that require an explicit `DatabaseProvider`, apply generated model
configuration, and never embed the scaffold connection string.
Computed/generated column expressions are emitted with
`HasComputedColumnSql` and marked as database-generated for runtime writes.
Column collations are emitted with `HasCollation` so DB-first string
comparison/order semantics survive the scaffold into migration snapshots.
SQL Server `IDENTITY(seed, increment)` metadata is emitted with
`HasIdentityOptions(...)` and preserved in SQL Server migration create/rebuild
DDL.
SQLite expression indexes, ordinary PostgreSQL B-tree expression indexes, and
MySQL expression indexes exposed by `SHOW INDEX` are emitted with
`HasExpressionIndex`, including representable PostgreSQL expression-index
`INCLUDE` and null-semantics metadata; SQL Server expression-index shapes stay
on the safer generated-column path.
Opt-in routine stubs include SQL Server scalar/table-valued functions plus
PostgreSQL and MySQL functions as provider-bound `SELECT` wrappers instead of
stored-procedure calls. PostgreSQL extension-owned routines are suppressed so
installed helper extensions do not generate application routine wrappers.
Stored-procedure stubs include buffered and streaming
result wrappers; stubs with discovered output metadata include a convenience
overload that uses the scaffolded `OutputParameter` definitions plus an
explicit-output overload for reviewed signature changes, including INOUT and
return-value directions where provider metadata exposes them.
Opt-in sequence stubs generate provider-bound next-value wrappers for SQL Server
and PostgreSQL standalone sequences.
Duplicate routine or sequence names across schemas use schema-prefixed generated
member names instead of opaque numeric suffixes.
SQL Server
and PostgreSQL schemas are preserved, SQLite attached database schemas are
preserved, and MySQL discovery does not bake the current database/catalog name
into the model.
SQLite rowid integer primary keys are generated as non-null `long` properties,
SQLite `UUID` columns scaffold as `Guid`, and SQLite declared `JSON`/`XML`
columns scaffold as string storage. SQL Server `xml`, PostgreSQL
`citext`/`json`/`jsonb`/`xml`/`uuid`, and MySQL `json`/`year` columns also scaffold as
safe scalar CLR storage while native JSON/XML operator semantics stay provider-bound,
and PostgreSQL arrays over safe scalar elements, including numeric, text/citext,
UUID, binary, date/time including `time with time zone`, interval, and timestamp arrays, scaffold as CLR arrays
while remaining provider-specific schema for provider-mobility review,
and dynamic `Query(string)` scaffolding mirrors static required/generated
metadata for supported shapes.

Unsupported composite foreign keys that do not target generated primary keys or
exact ordered unfiltered unique indexes, payload join tables,
complex/provider-specific defaults that fail the migration default allowlist,
provider column types,
unparsed provider-specific identity strategies that make the generated entity read-only,
unrecognized FK referential actions, triggers, SQL Server provider-native
temporal tables, SQLite virtual tables and shadow tables,
routines, sequences, synonyms, and events are reported in
`nORM.ScaffoldWarnings.md` and
`nORM.ScaffoldWarnings.json` instead of being silently converted into invalid
model code. The JSON report includes stable diagnostic codes, categories,
section counts, and suggested actions so CI can route scaffold follow-up without
parsing prose.
Tables without primary keys and generated query artifacts are emitted as
`[ReadOnlyEntity]` types: they can be queried, but nORM rejects generated
insert/update/delete and tracked `SaveChanges` writes before SQL generation.
Tables with provider-owned triggers are also emitted as `[ReadOnlyEntity]`
until trigger side effects are hand-modeled.
SQL Server provider-native temporal base and history tables are also emitted as
`[ReadOnlyEntity]` because native period/history behavior is provider-owned
until explicitly modeled.
Tables with unsafe provider-specific columns such as SQL Server/SQLite spatial
types, PostgreSQL `inet`, or MySQL spatial columns, plus unsafe MySQL `set(...)` declarations, are
also generated as `[ReadOnlyEntity]` so provider-owned type handling cannot be
written accidentally; safe scalar promotions and MySQL unsigned numeric widths
remain ordinary generated properties, including unsigned decimal/numeric
precision and optional-scale metadata.
SQL Server alias types over scaffoldable scalar/binary bases remain diagnostics
for provider-mobility review, but generated writes stay enabled because nORM
binds the safe base CLR type and the database enforces the alias type. SQL
Server text/binary facets such as non-Unicode `varchar`, fixed-length `char`,
and fixed-length `binary` are emitted into generated fluent configuration so
schema snapshots and migrations can round-trip the provider shape.
PostgreSQL domains over safe scalar/array/enum base types remain diagnostics for
provider-mobility review, but generated writes stay enabled because nORM binds
the safe base CLR type, preserves bounded string/numeric facets where provider
metadata exposes them, and the database enforces the domain constraint.
Safe SQL defaults, including vetted hex/binary literals, literal-only
`LOWER`/`UPPER` string normalization defaults with provider-normalized
PostgreSQL casts or MySQL character-set literals, and PostgreSQL typed-cast
defaults, are emitted as `HasDefaultValueSql(...)`; SQL Server
explicit non-system default-constraint names are preserved with
`HasDefaultValueSql(..., constraintName: ...)`. Only unmodeled
complex/provider-specific defaults make the generated entity read-only. MySQL
`ON UPDATE` timestamp defaults remain provider-specific diagnostics rather than
plain default metadata because the database mutates them during updates.
Foreign keys from keyless dependent tables are reported as relationship
diagnostics instead of generating navigations that cannot be safely included or
tracked.
Foreign keys with unknown provider-specific referential actions also suppress
generated navigations/fluent relationships instead of treating those actions as
`NO ACTION`.
Ordinary views and PostgreSQL materialized views scaffold as read-only query
artifacts by default. Opt-in query artifacts can include SQLite virtual tables
and SQL Server synonyms whose local base object resolves as a table or view;
non-query, remote, or unresolved synonyms remain provider-owned diagnostics.
Use `--fail-on-warnings` or `ScaffoldOptions.FailOnWarnings` to make lossy
scaffolds fail in CI after the warning report is written.
Use `--dry-run` or `ScaffoldOptions.DryRun` to validate scaffold output without
creating, deleting, or overwriting files. The CLI dry run also prints warning
summaries while leaving the requested output path untouched.
Clean later scaffold runs remove stale warning reports when overwrite is
explicitly allowed, or fail clearly when overwrite protection would leave stale
reports in place.
Repeated scaffolds can keep reviewed custom code in partial entity/context
classes, with generated contexts applying scaffolded model configuration before
caller-supplied `DbContextOptions.OnModelCreating` and
`OnModelCreatingPartial(ModelBuilder)` customization hooks. Table filters
(`--tables`/`--table`) and schema filters (`--schemas`/`--schema`) can be combined; EF-style
multi-value `--table First Second` and `--schema Accounting Sales` tokens are
accepted, and table filters can use `schema.table` or `schema.view`. MySQL
catalog-qualified table and query-artifact filters are accepted when the
catalog matches the current database, while generated model metadata remains
unqualified because MySQL catalogs are not emitted as nORM schemas.
Object-kind selectors such as `table:dbo.Report`, `view:dbo.Report`,
`query:dbo.Report`, `routine:dbo.RebuildCache`, and
`sequence:dbo.InvoiceNumber` disambiguate same-schema database objects that
share a name. Literal-name selectors such as `name:aux.orders` and
`table:name:aux.orders` select literal dotted object names that would otherwise
look schema-qualified.
Blank CLI table/schema filters are rejected so an empty option cannot broaden
the run to every table.
Schema filters select all discovered user tables and supported query
artifacts in matching schemas. By default, nORM's scaffold pluralizer singularizes
plural database object names for entity classes and emits collection-style
`IQueryable<T>` context property names; `--no-pluralize` or
`ScaffoldOptions.UsePluralizer = false` preserves generated names without
singularizing or pluralizing them. `--use-database-names` or
`ScaffoldOptions.UseDatabaseNames` preserves legal database table, view,
sequence, routine, column, and routine result-column names as generated CLR
names while still escaping invalid C# identifiers; synthetic navigation members
remain C#-style names derived from FK roles.
When routine or sequence stubs are enabled, table filters can select matching provider routines or sequences
without generating every discovered provider object.
Ambiguous explicit filters that match more than one selectable table, query artifact,
routine, or sequence fail closed; use schema-qualified filters for cross-schema
matches, object-kind selectors for same-schema object-kind collisions, and
literal-name selectors for filtered literal dotted-name collisions.
The CLI accepts EF's `--no-onconfiguring` switch as a no-op because nORM never
emits generated `OnConfiguring` connection-string code.
It also accepts EF-style aliases including `--output-dir`/`-o`, `-n`, `-c`,
`-t`, `--data-annotations`/`-d`, and `--force`/`-f`; the data-annotations
switch is a no-op because nORM already emits supported annotation metadata.
CLI scaffolding refuses existing output conflicts by default; `--force` opts
into overwriting generated files and `--no-overwrite` is accepted as an explicit
guard.
EF-style positional arguments are accepted as
`norm scaffold <connection> <provider> ...`; explicit `--connection` and
`--provider` options take precedence when both forms are supplied. A single
positional value after `--connection` is treated as the provider.
`norm dbcontext scaffold <connection> <provider> ...` is also accepted as an
EF-style alias for the same bounded nORM scaffold command, and `norm --help`
advertises `dbcontext` as an EF-style alias group.
The provider argument accepts EF Core package names and normalizes them to nORM
providers, including `Microsoft.EntityFrameworkCore.SqlServer`,
`Microsoft.EntityFrameworkCore.Sqlite`, `Npgsql.EntityFrameworkCore.PostgreSQL`,
`Pomelo.EntityFrameworkCore.MySql`, and `MySql.EntityFrameworkCore`.
Named connection references such as `Name=ConnectionStrings:AppDb`,
`name=ConnectionStrings:AppDb`, or shorthand `Name=AppDb` resolve from
environment variables first (for example `ConnectionStrings__AppDb`), then
startup-project and target-project user secrets declared through `UserSecretsId`, then `appsettings.json` / `appsettings.{Environment}.json` under the startup
project, target project, or current directory without executing startup code.
`--project`/`-p` targets a `.csproj` or single-project directory; relative
output paths resolve under that project, and the namespace defaults to its
`RootNamespace`, `AssemblyName`, or sanitized project file name plus sanitized
output directory segments when `--namespace` is omitted. If `--project` is
omitted and the command working directory contains exactly one `.csproj`, nORM
uses that project for the same defaults, including nullable-reference output.
`Nullable` values `enable` and `annotations` emit `#nullable enable`; `disable`,
`warnings`, or an omitted property emit `#nullable disable`. The nearest
`Directory.Build.props` is read before project overrides for `RootNamespace`,
`AssemblyName`, `UserSecretsId`, and `Nullable`. When
`--context-dir` is supplied, relative paths resolve under the target project
directory, or under the command working directory when `--project` is omitted. Without
`--context-namespace`, a qualified `--context`, or an explicit `--namespace`,
the context namespace defaults to the project's root namespace plus sanitized
context directory segments. `--context` also accepts namespace-qualified names
such as `MyApp.Data.AppDbContext`; nORM generates class `AppDbContext` in
namespace `MyApp.Data`, unless `--context-namespace` supplies an explicit
context namespace. `--namespace`, `--context-namespace`, and qualified
`--context` namespace portions are validated as C# namespaces before generation.
Explicit `--context` class-name segments are validated as C# type identifiers
rather than silently corrected. Blank explicit CLI string values such as
`--namespace " "`, `--context " "`, and `--output " "` are rejected instead of
being treated as omitted options.
When `--context` is omitted, the CLI derives the context
class name from the database name, SQLite file name, or final `Name=...`
configuration-key segment and appends `Context`, falling back to `AppDbContext`
when no stable name is available. EF common switches
`--startup-project`/`-s`, `--framework`/`--target-framework`,
`--configuration`, `--runtime`, and `--no-build` are accepted for command-line
compatibility, but nORM scaffolding connects directly to the database and does
not build or load a startup app for schema discovery. Legacy EF-style
`--msbuildprojectextensionspath` is also accepted as a no-op because nORM
scaffold does not invoke MSBuild.
EF-style application arguments after `--` are accepted for command
compatibility. `-- --environment Production` is used only to include
`appsettings.Production.json` in named-connection lookup, with startup-project
environment files searched before target-project environment files when
`--startup-project` is supplied; other application arguments are ignored
because nORM does not execute startup code. When no
pass-through environment is supplied, `ASPNETCORE_ENVIRONMENT` and then
`DOTNET_ENVIRONMENT` select the matching `appsettings.{Environment}.json` file.
An explicit blank `--environment` value is rejected. Unmatched scaffold tokens
before `--` still fail fast.
EF-style `.config/dotnet-ef.json` defaults are read for `project`,
`startupProject`, `outputDir`/`output`, `namespace`, `context`, `contextDir`,
`contextNamespace`, `schema`/`schemas`, `table`/`tables`,
`framework`/`targetFramework`, `configuration`, `runtime`,
`msbuildProjectExtensionsPath`, `verbose`, `noColor`, `prefixOutput`,
`noPluralize`, `useDatabaseNames`, `noRelationships`, `force`, `noOverwrite`, `dryRun`,
`failOnWarnings`, `emitRoutineStubs`,
`emitSequenceStubs`, `emitViewEntities`, and `emitQueryArtifacts`; relative project paths are resolved relative to the
parent of `.config`, comma-separated or array table/schema defaults are
accepted, present string properties must be non-blank, and explicit CLI
options override config values. When any CLI table/schema filter is supplied,
config table/schema defaults are ignored so they cannot expand the explicit
selection. Explicit CLI `--force` and `--no-overwrite` values also override the
opposite config default instead of conflicting with it.
`--json` emits a machine-readable scaffold result summary for successful runs
and scaffold failures; `--verbose`/`-v`,
`--no-color`, and `--prefix-output` are accepted for EF command-line
compatibility because nORM scaffold output is already plain and explicit.
Unfiltered ordinary views and PostgreSQL materialized views are scaffolded by
default as read-oriented generated types; explicit `--table`/`--schema` filters
also include matching supported query artifacts. SQLite virtual tables and
SQL Server local table/view synonyms remain opt-in through explicit filters or
`--emit-query-artifacts` (or the compatibility alias `--emit-view-entities`).
`--context-dir` can place the generated context outside the entity output
directory using EF-style project-relative paths; the CLI rejects absolute
context paths. API callers can use
`ScaffoldOptions.ContextDirectory` for output-relative context placement or
`ScaffoldOptions.ContextOutputDirectory` for an absolute context directory.
`--context-namespace`/`ScaffoldOptions.ContextNamespace` can separate the context
namespace while generated context code imports the entity namespace.
Opt-in scaffold switches can emit provider-bound routine wrappers
(`--emit-routine-stubs`), provider-bound sequence wrappers
(`--emit-sequence-stubs`), and optional provider query artifacts
(`--emit-query-artifacts`, or the compatibility alias `--emit-view-entities`, used for SQLite virtual-table and SQL Server synonym query artifacts);
both remain explicitly bounded and are not provider mobility proof by
themselves. Routine wrappers preserve discovered input CLR types, output
`DbType` values, INOUT direction, and string/binary output sizes where
providers expose them; PostgreSQL set-returning functions scaffold as
table-valued `SELECT * FROM function(...)` wrappers. Payload bridge tables scaffold as explicit join
entities with payload columns and FK navigations.
Discriminator-looking columns and owned-type naming conventions scaffold as
ordinary scalar columns; owned-type inference, inheritance inference, view
write semantics, and provider-specific schema semantics remain explicit
modeling work after scaffolding. See [Scaffolding Contract](docs/scaffolding.md).

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
configuration. The command can resolve project outputs with `--project` or
`--startup-project` plus `--configuration`, `--runtime`, and
`--target-framework`/`--framework`; `--environment` is passed to design-time
factories and exposed through the standard environment variables while the
snapshot is built. Explicit `--deps` and `--runtimeconfig` files are validated
and managed/native runtime assets listed in `--deps` are used for design-time
dependency probing, including nested deps-file-directory, runtimeconfig
`additionalProbingPaths`, and NuGet package cache assets. See
[Design-Time Migrations](docs/design-time-migrations.md).

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
