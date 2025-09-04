using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class CompositeKeyTests
{
    public class CompositeEntity
    {
        public int KeyPart1 { get; set; }
        public int KeyPart2 { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    [Fact]
    public void BuildUpdate_and_Delete_use_all_key_columns()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<CompositeEntity>().HasKey(e => new { e.KeyPart1, e.KeyPart2 })
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var mapping = ctx.GetMapping(typeof(CompositeEntity));

        var updateSql = ctx.Provider.BuildUpdate(mapping);
        var deleteSql = ctx.Provider.BuildDelete(mapping);

        Assert.Contains("KeyPart1", updateSql);
        Assert.Contains("KeyPart2", updateSql);
        Assert.Contains(" AND ", updateSql);

        Assert.Contains("KeyPart1", deleteSql);
        Assert.Contains("KeyPart2", deleteSql);
        Assert.Contains(" AND ", deleteSql);
    }

    [Fact]
    public void ChangeTracker_uses_composite_key_for_identity_map()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
                mb.Entity<CompositeEntity>().HasKey(e => new { e.KeyPart1, e.KeyPart2 })
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var first = new CompositeEntity { KeyPart1 = 1, KeyPart2 = 2, Value = "A" };
        var second = new CompositeEntity { KeyPart1 = 1, KeyPart2 = 2, Value = "B" };

        ctx.Attach(first);
        var entry = ctx.Attach(second);

        Assert.Same(first, entry.Entity);
        Assert.Single(ctx.ChangeTracker.Entries);
    }
}

