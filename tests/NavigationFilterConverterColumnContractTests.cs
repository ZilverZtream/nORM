using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins comparisons against a value-converter column in the navigation-filter grammar (filtered Include,
/// shaped-collection filters, global filters). The opposing operand is converted to the provider
/// representation, so `l.Score == 5` (model) queries the STORED value — previously it compared the column to
/// the raw model value and silently matched nothing. Covers constant, closure (with rebind), ordering, and a
/// global filter, using an order-preserving offset converter (stored = model + 1000).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationFilterConverterColumnContractTests
{
    [Table("NccOrder")]
    public class Order { [Key] public int Id { get; set; } public List<Line> Lines { get; set; } = new(); }

    [Table("NccLine")]
    public class Line { [Key] public int Id { get; set; } public int OrderId { get; set; } public int Score { get; set; } }

    // Order-preserving: model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private static DbContext Bootstrap(SqliteConnection cn, bool minScoreGlobalFilter = false)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NccOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE NccLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Score INTEGER NOT NULL);
                INSERT INTO NccOrder VALUES (1);
                -- Stored = model + 1000: model scores 5, 7, 9.
                INSERT INTO NccLine VALUES (1,1,1005),(2,1,1007),(3,1,1009);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Line>().Property<int>(l => l.Score).HasConversion(new OffsetConverter());
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            if (minScoreGlobalFilter)
                mb.Entity<Line>().Property<int>(l => l.Score); // ensure converter registered before filter below
        }};
        if (minScoreGlobalFilter)
            opts.AddGlobalFilter<Line>(l => l.Score >= 7);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static int[] Load(DbContext ctx, Func<INormQueryable<Order>, IQueryable<Order>> shape)
        => shape((INormQueryable<Order>)ctx.Query<Order>()).ToList().Single()
            .Lines.OrderBy(l => l.Id).Select(l => l.Id).ToArray();

    [Fact]
    public void Equality_against_a_converter_column_matches_the_stored_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 1 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Score == 5))));
    }

    [Fact]
    public void Ordering_against_a_converter_column_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // model Score > 6 → 7, 9 (stored 1007, 1009); the bound 6 is converted to 1006.
        Assert.Equal(new[] { 2, 3 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Score > 6))));
    }

    [Fact]
    public void Closure_against_a_converter_column_binds_and_rebinds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        int[] RunFor(int s) => Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Score == s)));
        Assert.Equal(new[] { 2 }, RunFor(7));   // captured value converted to 1007
        Assert.Equal(new[] { 3 }, RunFor(9));   // re-bind → 1009
    }

    [Fact]
    public void Global_filter_on_a_converter_column_applies()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn, minScoreGlobalFilter: true);
        // Global filter Score >= 7 → 7, 9 (bound converted to 1007).
        Assert.Equal(new[] { 2, 3 }, Load(ctx, q => q.Include(o => o.Lines)));
    }
}
