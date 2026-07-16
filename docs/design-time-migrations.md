# Design-Time Migrations

`norm migrations add` must build the same model that the application uses at
runtime. For v1, the preferred path is an explicit design-time context factory.

## Factory API

Implement `INormDesignTimeDbContextFactory<TContext>` in the assembly passed to
`norm migrations add --assembly` or resolved from `--project` /
`--startup-project`:

```csharp
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;

public sealed class AppDesignTimeFactory : INormDesignTimeDbContextFactory<AppDbContext>
{
    public AppDbContext CreateDbContext(string[] args)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<Order>()
                  .ToTable("Orders")
                  .Property(o => o.TotalAmount)
                  .HasColumnName("total_amount")
        };

        return new AppDbContext(connection, new SqliteProvider(), options);
    }
}
```

The CLI disposes the returned context after snapshot generation. When
`--environment <name>` is supplied, the factory receives
`["--environment", "<name>"]` and the CLI temporarily sets both
`ASPNETCORE_ENVIRONMENT` and `DOTNET_ENVIRONMENT` while the model snapshot is
built. If the factory constructor or `CreateDbContext` fails, the CLI reports
the failing factory type and the failure reason while redacting connection
string secrets such as `Password=...`, `Pwd=...`, `Token=...`, and
`Secret=...`.

## Project Resolution

`norm migrations add` can resolve the design-time assembly from a project file:

```bash
norm migrations add InitialCreate --provider sqlite --project src/App/App.csproj --configuration Release --target-framework net8.0
```

`--startup-project` takes precedence over `--project` for the design-time host
assembly, matching the EF tooling convention for multi-project solutions.
`--configuration`, `--runtime`, and `--target-framework`/`--framework` are
passed to `dotnet msbuild -getProperty:TargetPath` so the CLI loads the same
build output a real application layout uses. Multi-targeted projects must pass
`--target-framework` or `--framework` so the design-time assembly is resolved
from the intended target.

Explicit `--deps` and `--runtimeconfig` paths are validated before the assembly
is loaded. The assembly directory remains the primary dependency resolver root.
When `--deps` is supplied, managed and native runtime assets listed in the deps
file are also used as dependency candidates, including nested paths such as
`lib/net8.0/Dependency.dll` or `runtimes/win-x64/native/Native.dll` under the
deps-file directory and matching assets in the configured/global NuGet package
cache.

## Applying migrations: `norm database update`

```bash
norm database update --connection "..." --provider sqlserver --assembly ./bin/Debug/net8.0/App.Migrations.dll
```

Applies all pending migrations from the assembly to the target database.
`--connection`, `--provider` (`sqlserver`, `sqlite`, `postgres`, `mysql`, or the
matching EF Core provider package name), and `--assembly` are all required.
Concurrent deploys are safe: every runner serializes behind a provider advisory
lock (`sp_getapplock` / `pg_try_advisory_lock` / `GET_LOCK`), re-reads the
pending list after acquiring it, and records applied versions in the
`__NormMigrationsHistory` table, so two deployers applying the same migration
run it exactly once. When nothing is pending the command reports that and
exits successfully.

## Resetting a database: `norm database drop`

```bash
norm database drop --connection "..." --provider postgres --yes
norm database drop --connection "..." --provider postgres --dry-run
```

Destructive, built for resetting TEST databases. The command refuses to run
without `--yes`; `--dry-run` prints what would be dropped without deleting
anything. Provider-protected system database names (e.g. `master`, `postgres`,
`mysql`) refuse the drop outright, and system schemas are excluded from table
enumeration. On SQLite the database file itself is deleted.
`runtimeOptions.additionalProbingPaths` from the explicit runtimeconfig file,
and from the matching `.runtimeconfig.dev.json` sibling when present, are used
as package roots before the configured/global NuGet package cache.

## Fallbacks

If no factory exists, the CLI attempts the simple
`(DbConnection, DatabaseProvider)` constructor path. This supports minimal
contexts and preserves older projects.

If neither path works, migration generation fails with an actionable error. The
CLI no longer silently falls back to attribute-only scanning because that can
drop fluent mappings such as `ToTable`, `HasColumnName`, `HasKey`,
relationships, and shadow properties.

Use `--attribute-only` only when the model intentionally consists of data
annotations and no fluent configuration.
