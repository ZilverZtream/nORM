using System;
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
/// Pins shaped collection projections Stage 2 — an element PROJECTION inside the collection binding:
/// Select(o => new Dto { Lines = o.Lines.Select(l => new LineDto{...}).ToList() }). The collection is
/// fetched via the same split query as Stage 1 but each child entity is shaped into the projected element
/// type client-side. Composes with a Where filter, supports scalar and computed projections, stitches
/// per-parent, and leaves empty matches empty. A closure- or outer-referencing element projection is NOT
/// admitted (it stays fail-loud, since the child materializer only has the child entity).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionProjectionStage2ContractTests
{
    [Table("Sp2Order")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("Sp2Line")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
    }

    public class LineDto { public string Sku { get; set; } = ""; public int Qty { get; set; } }
    // NOTE: the collection property name must match the nav name ("Lines") — the split-query stitch
    // resolves the target by the navigation property name (a Stage 1 convention inherited here).
    public class OrderDto { public int Id { get; set; } public List<LineDto> Lines { get; set; } = new(); }
    public class OrderScalarDto { public int Id { get; set; } public List<string> Lines { get; set; } = new(); }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Sp2Order (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
                CREATE TABLE Sp2Line (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL, Qty INTEGER NOT NULL);
                INSERT INTO Sp2Order VALUES (1, 'o1'), (2, 'o2'), (3, 'o3');
                INSERT INTO Sp2Line VALUES (1,1,'a',3),(2,1,'b',8),(3,1,'c',5);
                INSERT INTO Sp2Line VALUES (4,2,'d',10);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public async Task Dto_element_projection_shapes_and_stitches_per_parent()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderDto { Id = o.Id, Lines = o.Lines.Select(l => new LineDto { Sku = l.Sku, Qty = l.Qty }).ToList() })
            .ToListAsync();

        var o1 = dtos.Single(d => d.Id == 1);
        Assert.Equal(new[] { "a:3", "b:8", "c:5" }, o1.Lines.OrderBy(l => l.Sku).Select(l => $"{l.Sku}:{l.Qty}").ToArray());
        var o2 = dtos.Single(d => d.Id == 2);
        Assert.Equal(new[] { "d:10" }, o2.Lines.Select(l => $"{l.Sku}:{l.Qty}").ToArray());
        Assert.Empty(dtos.Single(d => d.Id == 3).Lines);   // no children → empty projected list
    }

    [Fact]
    public async Task Scalar_element_projection_yields_a_scalar_collection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderScalarDto { Id = o.Id, Lines = o.Lines.Select(l => l.Sku).ToList() })
            .ToListAsync();

        Assert.Equal(new[] { "a", "b", "c" }, dtos.Single(d => d.Id == 1).Lines.OrderBy(s => s).ToArray());
        Assert.Equal(new[] { "d" }, dtos.Single(d => d.Id == 2).Lines.ToArray());
    }

    [Fact]
    public async Task Filtered_then_projected_applies_both()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderDto { Id = o.Id, Lines = o.Lines.Where(l => l.Qty >= 5).Select(l => new LineDto { Sku = l.Sku, Qty = l.Qty }).ToList() })
            .ToListAsync();

        // Order 1: only b(8), c(5) pass Qty>=5, projected.
        Assert.Equal(new[] { "b:8", "c:5" }, dtos.Single(d => d.Id == 1).Lines.OrderBy(l => l.Sku).Select(l => $"{l.Sku}:{l.Qty}").ToArray());
    }

    [Fact]
    public async Task Computed_element_projection_runs_client_side()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderDto { Id = o.Id, Lines = o.Lines.Select(l => new LineDto { Sku = l.Sku.ToUpper(), Qty = l.Qty * 2 }).ToList() })
            .ToListAsync();

        Assert.Equal(new[] { "A:6", "B:16", "C:10" }, dtos.Single(d => d.Id == 1).Lines.OrderBy(l => l.Sku).Select(l => $"{l.Sku}:{l.Qty}").ToArray());
    }

    [Fact]
    public async Task Closure_capturing_element_projection_works_and_rebinds_across_executions()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        async Task<int[]> RunFor(int factor)
        {
            var dtos = await ctx.Query<Order>()
                .Select(o => new OrderDto { Id = o.Id, Lines = o.Lines.Select(l => new LineDto { Sku = l.Sku, Qty = l.Qty * factor }).ToList() })
                .ToListAsync();
            return dtos.Single(d => d.Id == 1).Lines.OrderBy(l => l.Sku).Select(l => l.Qty).ToArray();
        }

        // The element projection captures `factor`. The plan is marked non-cacheable so a second execution
        // with a different factor re-binds it — a frozen cached delegate would replay the first run's values.
        Assert.Equal(new[] { 30, 80, 50 }, await RunFor(10));   // 3,8,5 * 10
        Assert.Equal(new[] { 6, 16, 10 }, await RunFor(2));     // 3,8,5 * 2
    }

    [Fact]
    public async Task Outer_row_referencing_element_projection_is_not_admitted()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        // The element projection reads the OUTER row (o.Id), which the child materializer cannot supply —
        // this stays fail-loud rather than being mis-shaped.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await ctx.Query<Order>()
                .Select(o => new OrderOuterRefDto { Id = o.Id, Lines = o.Lines.Select(l => new LineWithOrderDto { Sku = l.Sku, OrderId = o.Id }).ToList() })
                .ToListAsync());
    }

    public class LineWithOrderDto { public string Sku { get; set; } = ""; public int OrderId { get; set; } }
    public class OrderOuterRefDto { public int Id { get; set; } public List<LineWithOrderDto> Lines { get; set; } = new(); }

    [Fact]
    public async Task Bare_collection_projection_still_returns_entities()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        // Stage 1 regression: no .Select → the collection holds the entity type.
        var dtos = await ctx.Query<Order>()
            .Select(o => new OrderEntityDto { Id = o.Id, Lines = o.Lines.ToList() })
            .ToListAsync();
        Assert.Equal(new[] { "a", "b", "c" }, dtos.Single(d => d.Id == 1).Lines.OrderBy(l => l.Sku).Select(l => l.Sku).ToArray());
    }

    public class OrderEntityDto { public int Id { get; set; } public List<Line> Lines { get; set; } = new(); }
}
