using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins shaped collection projections (Stage 1: a filtered, same-element-type collection):
/// Select(o => new Dto { Lines = o.Lines.Where(pred).ToList() }) fetches the collection via a
/// split query with the predicate ANDed onto the child fetch — previously this threw under the
/// default client-eval policy. A bare o.Lines.ToList() (no filter) still returns the whole set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionProjectionContractTests
{
    [Table("ScpOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("ScpLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public bool Active { get; set; }
        public string Sku { get; set; } = "";
    }

    public class OrderDto
    {
        public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    private static async Task<(DbContext ctx, List<Order> oracle)> BootstrapAsync(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ScpOrder (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
                CREATE TABLE ScpLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Active INTEGER NOT NULL, Sku TEXT NOT NULL);
                INSERT INTO ScpOrder VALUES (1, 'o1'), (2, 'o2');
                INSERT INTO ScpLine VALUES (1, 1, 1, 'a'), (2, 1, 0, 'b'), (3, 1, 1, 'c');
                INSERT INTO ScpLine VALUES (4, 2, 0, 'd');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        // In-memory oracle mirroring the seed data.
        var lines = new List<Line>
        {
            new() { Id = 1, OrderId = 1, Active = true, Sku = "a" },
            new() { Id = 2, OrderId = 1, Active = false, Sku = "b" },
            new() { Id = 3, OrderId = 1, Active = true, Sku = "c" },
            new() { Id = 4, OrderId = 2, Active = false, Sku = "d" },
        };
        var oracle = new List<Order>
        {
            new() { Id = 1, Code = "o1", Lines = lines.Where(l => l.OrderId == 1).ToList() },
            new() { Id = 2, Code = "o2", Lines = lines.Where(l => l.OrderId == 2).ToList() },
        };
        await Task.CompletedTask;
        return (ctx, oracle);
    }

    [Fact]
    public async Task Filtered_collection_projection_applies_the_predicate()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, oracle) = await BootstrapAsync(cn);
        await using var _ctx = ctx;

        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderDto { Id = o.Id, Code = o.Code, Lines = o.Lines.Where(l => l.Active).ToList() })
            .ToListAsync();

        var expected = oracle.OrderBy(o => o.Id)
            .Select(o => $"{o.Id}:[{string.Join(",", o.Lines.Where(l => l.Active).OrderBy(l => l.Id).Select(l => l.Sku))}]")
            .ToArray();
        var actual = dtos.OrderBy(d => d.Id)
            .Select(d => $"{d.Id}:[{string.Join(",", d.Lines.OrderBy(l => l.Id).Select(l => l.Sku))}]")
            .ToArray();

        // Order 1 → only active lines a,c; Order 2 → none active.
        Assert.Equal(new[] { "1:[a,c]", "2:[]" }, actual);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Filtered_collection_projection_binds_scalar_and_filter_closures_in_order()
    {
        // A scalar closure in one binding and a filter closure in another must each bind their own
        // value — a document-order mismatch would cross the two and silently return the wrong rows.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, _) = await BootstrapAsync(cn);
        await using var _ctx = ctx;

        var codeSuffix = "!";
        var wantedSku = "c";

        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderDto
            {
                Id = o.Id,
                Code = o.Code + codeSuffix,
                Lines = o.Lines.Where(l => l.Sku == wantedSku).ToList()
            })
            .ToListAsync();

        var order1 = dtos.Single(d => d.Id == 1);
        Assert.Equal("o1!", order1.Code);
        Assert.Equal(new[] { "c" }, order1.Lines.Select(l => l.Sku).ToArray());
    }

    [Fact]
    public async Task Filtered_collection_projection_rebinds_closure_values_across_cache_hits()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, _) = await BootstrapAsync(cn);
        await using var _ctx = ctx;

        async Task<string[]> RunFor(string sku)
        {
            var dtos = await ctx.Query<Order>()
                .Select(o => new OrderDto { Id = o.Id, Code = o.Code, Lines = o.Lines.Where(l => l.Sku == sku).ToList() })
                .ToListAsync();
            return dtos.OrderBy(d => d.Id)
                .Select(d => $"{d.Id}:[{string.Join(",", d.Lines.OrderBy(l => l.Id).Select(l => l.Sku))}]")
                .ToArray();
        }

        // Same query shape twice with different closure values — the second execution reuses the
        // cached plan and must re-bind the new value, not replay a baked-in stale one.
        var first = await RunFor("a");
        var second = await RunFor("c");

        Assert.Equal(new[] { "1:[a]", "2:[]" }, first);
        Assert.Equal(new[] { "1:[c]", "2:[]" }, second);
    }

    [Fact]
    public async Task Bare_collection_projection_returns_the_whole_set()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        var (ctx, _) = await BootstrapAsync(cn);
        await using var _ctx = ctx;

        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderDto { Id = o.Id, Code = o.Code, Lines = o.Lines.ToList() })
            .ToListAsync();

        var order1 = dtos.Single(d => d.Id == 1);
        Assert.Equal(new[] { "a", "b", "c" }, order1.Lines.OrderBy(l => l.Id).Select(l => l.Sku).ToArray());
    }
}
