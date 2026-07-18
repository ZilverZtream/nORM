using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins string-method predicates (StartsWith/EndsWith/Contains, with a constant pattern) in the navigation-
/// filter grammar shared by filtered Include and global filters. They share the projection/Where path's
/// case-sensitivity handling: ordinal by default, LOWER-folded for the OrdinalIgnoreCase overload. Negation
/// composes, and a global filter using a string method applies to the eager-loaded child too.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationFilterStringMethodContractTests
{
    [Table("NfsOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("NfsLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
    }

    private static DbContext Bootstrap(SqliteConnection cn, bool globalFilterHidesXy = false)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NfsOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE NfsLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);
                INSERT INTO NfsOrder VALUES (1);
                INSERT INTO NfsLine VALUES (1,1,'AB1'),(2,1,'AB2'),(3,1,'XY9'),(4,1,'abZ');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
        }};
        if (globalFilterHidesXy)
            opts.AddGlobalFilter<Line>(l => !l.Sku.StartsWith("XY"));
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static string[] Load(DbContext ctx, Func<INormQueryable<Order>, IQueryable<Order>> shape)
    {
        var order = shape((INormQueryable<Order>)ctx.Query<Order>()).ToList().Single();
        return order.Lines.OrderBy(l => l.Id).Select(l => l.Sku).ToArray();
    }

    [Fact]
    public void StartsWith_ordinal_filters_case_sensitively()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { "AB1", "AB2" }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Sku.StartsWith("AB")))));
    }

    [Fact]
    public void Contains_filters_the_children()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { "XY9" }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Sku.Contains("Y")))));
    }

    [Fact]
    public void EndsWith_filters_the_children()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { "abZ" }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Sku.EndsWith("Z")))));
    }

    [Fact]
    public void Negated_string_method_composes()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { "XY9", "abZ" }, Load(ctx, q => q.Include(o => o.Lines.Where(l => !l.Sku.StartsWith("AB")))));
    }

    [Fact]
    public void Case_insensitive_overload_folds_case()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { "AB1", "AB2", "abZ" },
            Load(ctx, q => q.Include(o => o.Lines.Where(l => l.Sku.StartsWith("ab", StringComparison.OrdinalIgnoreCase)))));
    }

    [Fact]
    public void Global_filter_using_a_string_method_hides_the_matching_child()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn, globalFilterHidesXy: true);
        // Global filter !Sku.StartsWith("XY") drops XY9 from the eager-loaded collection.
        Assert.Equal(new[] { "AB1", "AB2", "abZ" }, Load(ctx, q => q.Include(o => o.Lines)));
    }
}
