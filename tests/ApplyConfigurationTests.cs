using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// Public top-level entity + configuration so ApplyConfigurationsFromAssembly's reflection scan can
// discover the type and Activator can construct the configuration.
public class AcWidget
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

public sealed class AcWidgetConfiguration : IEntityTypeConfiguration<AcWidget>
{
    // ToTable rename means the query only resolves if this configuration was actually applied.
    public void Configure(EntityTypeBuilder<AcWidget> builder)
        => builder.ToTable("AcWidgetTable").HasKey(w => w.Id);
}

/// <summary>
/// EF Core parity: per-entity configuration can live in a dedicated
/// <see cref="IEntityTypeConfiguration{TEntity}"/> class applied via
/// <see cref="ModelBuilder.ApplyConfiguration{TEntity}"/> or
/// <see cref="ModelBuilder.ApplyConfigurationsFromAssembly"/>. Each config renames the table, so a query
/// resolves only when the configuration ran.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ApplyConfigurationTests
{
    private static SqliteConnection NewDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE AcWidgetTable (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); INSERT INTO AcWidgetTable VALUES (1, 'a');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public void ApplyConfiguration_runs_the_config_class()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.ApplyConfiguration(new AcWidgetConfiguration())
        }, ownsConnection: false);

        var w = ctx.Query<AcWidget>().First(x => x.Id == 1);
        Assert.Equal("a", w.Name);
    }

    [Fact]
    public void ApplyConfigurationsFromAssembly_discovers_and_applies_config_classes()
    {
        using var cn = NewDb();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.ApplyConfigurationsFromAssembly(typeof(AcWidgetConfiguration).Assembly)
        }, ownsConnection: false);

        // AcWidgetConfiguration was discovered by the scan and renamed the table to AcWidgetTable.
        var w = ctx.Query<AcWidget>().First(x => x.Id == 1);
        Assert.Equal("a", w.Name);
    }

    [Fact]
    public void ApplyConfiguration_null_throws()
    {
        var mb = new ModelBuilder();
        Assert.Throws<ArgumentNullException>(() => mb.ApplyConfiguration<AcWidget>(null!));
    }

    [Fact]
    public void ApplyConfigurationsFromAssembly_null_throws()
    {
        var mb = new ModelBuilder();
        Assert.Throws<ArgumentNullException>(() => mb.ApplyConfigurationsFromAssembly(null!));
    }
}
