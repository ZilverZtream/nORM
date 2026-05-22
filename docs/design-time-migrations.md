# Design-Time Migrations

`norm migrations add` must build the same model that the application uses at
runtime. For v1, the preferred path is an explicit design-time context factory.

## Factory API

Implement `INormDesignTimeDbContextFactory<TContext>` in the assembly passed to
`norm migrations add --assembly`:

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

The CLI disposes the returned context after snapshot generation.

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
