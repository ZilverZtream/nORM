using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A WHERE predicate comparing a column that has a value converter must bind the CONVERTED
/// (provider) value, not the raw model value. Uses a negating converter (model 42 stored as -42) so
/// that failing to convert produces a demonstrably wrong result set — unlike an enum->int converter,
/// which coincides with nORM's default enum handling and hides the bug. Compares the fast-path
/// shapes (simple equality, equality+Take) against a full-translator shape (multi-predicate) that is
/// known to convert correctly.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class FastPathConverterPredicateTests
{
    private class Item
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private static DbContext Create(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Score INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Item>().Property<int>(p => p.Score).HasConversion(new NegatingConverter())
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static async Task Seed(DbContext ctx)
    {
        ctx.Add(new Item { Name = "a", Score = 42 });   // stored -42
        ctx.Add(new Item { Name = "b", Score = 100 });  // stored -100
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task FullTranslator_multipredicate_converts_where_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        // Multi-predicate (AndAlso) does not match any fast-path shape -> full translator.
        var rows = await ctx.Query<Item>().Where(p => p.Score == 42 && p.Id > 0).ToListAsync();
        Assert.Single(rows);
        Assert.Equal("a", rows[0].Name);
    }

    [Fact]
    public async Task FastPath_simple_equality_converts_where_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        // Single equality on a converted column -> simple-where fast path.
        var rows = await ctx.Query<Item>().Where(p => p.Score == 42).ToListAsync();
        Assert.Single(rows);          // BUG if 0: fast path bound raw 42 vs stored -42
        Assert.Equal("a", rows[0].Name);
    }

    [Fact]
    public async Task FastPath_equality_with_take_converts_where_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var rows = await ctx.Query<Item>().Where(p => p.Score == 42).Take(10).ToListAsync();
        Assert.Single(rows);
        Assert.Equal("a", rows[0].Name);
    }
}
