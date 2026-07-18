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
/// Hardens shaped-collection Stage 2 (element projections) against the interactions a reviewer would probe:
/// the element projection is applied AFTER the child fetch's global filter (a soft-deleted child never
/// reaches the projection), two projected collections on one query each shape and stitch independently, and
/// a closure predicate on the filter coexists with the closure-free projection and re-binds across cache hits.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionProjectionStage2InteractionTests
{
    [Table("Sp2iOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
        public List<Note> Notes { get; set; } = new();
    }

    [Table("Sp2iLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public int Qty { get; set; }
        public bool IsDeleted { get; set; }
    }

    [Table("Sp2iNote")]
    public class Note
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Text { get; set; } = "";
    }

    public class LineDto { public string Sku { get; set; } = ""; public int Qty { get; set; } }
    public class NoteDto { public string Text { get; set; } = ""; }
    public class OrderDto { public int Id { get; set; } public List<LineDto> Lines { get; set; } = new(); public List<NoteDto> Notes { get; set; } = new(); }

    private static DbContext Bootstrap(SqliteConnection cn, bool softDelete)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE Sp2iOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE Sp2iLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL, Qty INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                CREATE TABLE Sp2iNote (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Text TEXT NOT NULL);
                INSERT INTO Sp2iOrder VALUES (1);
                INSERT INTO Sp2iLine VALUES (1,1,'a',3,0),(2,1,'b',8,1),(3,1,'c',5,0);
                INSERT INTO Sp2iNote VALUES (1,1,'x'),(2,1,'y');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Note>().HasKey(n => n.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            mb.Entity<Order>().HasMany(o => o.Notes).WithOne().HasForeignKey(n => n.OrderId, o => o.Id);
        }};
        if (softDelete)
            opts.AddGlobalFilter<Line>(l => !l.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public async Task Element_projection_runs_after_the_soft_delete_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn, softDelete: true);

        var dto = (await ctx.Query<Order>()
            .Select(o => new OrderDto { Id = o.Id, Lines = o.Lines.Select(l => new LineDto { Sku = l.Sku, Qty = l.Qty }).ToList() })
            .ToListAsync()).Single();

        // Line b is soft-deleted → filtered at the child fetch, never projected. a and c remain.
        Assert.Equal(new[] { "a:3", "c:5" }, dto.Lines.OrderBy(l => l.Sku).Select(l => $"{l.Sku}:{l.Qty}").ToArray());
    }

    [Fact]
    public async Task Two_projected_collections_each_shape_and_stitch_independently()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn, softDelete: false);

        var dto = (await ctx.Query<Order>()
            .Select(o => new OrderDto
            {
                Id = o.Id,
                Lines = o.Lines.Select(l => new LineDto { Sku = l.Sku, Qty = l.Qty }).ToList(),
                Notes = o.Notes.Select(n => new NoteDto { Text = n.Text }).ToList()
            })
            .ToListAsync()).Single();

        Assert.Equal(new[] { "a", "b", "c" }, dto.Lines.OrderBy(l => l.Sku).Select(l => l.Sku).ToArray());
        Assert.Equal(new[] { "x", "y" }, dto.Notes.OrderBy(n => n.Text).Select(n => n.Text).ToArray());
    }

    [Fact]
    public async Task Closure_filter_coexists_with_the_projection_and_rebinds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn, softDelete: false);

        async Task<string[]> RunFor(int min)
        {
            var dto = (await ctx.Query<Order>()
                .Select(o => new OrderDto { Id = o.Id, Lines = o.Lines.Where(l => l.Qty >= min).Select(l => new LineDto { Sku = l.Sku, Qty = l.Qty }).ToList() })
                .ToListAsync()).Single();
            return dto.Lines.OrderBy(l => l.Sku).Select(l => l.Sku).ToArray();
        }

        // The FILTER captures `min` (bound via @cp); the PROJECTION is closure-free. Same plan shape twice
        // with different thresholds must re-bind the filter, not replay the first run's value.
        Assert.Equal(new[] { "b", "c" }, await RunFor(5));   // Qty>=5 → b(8), c(5)
        Assert.Equal(new[] { "b" }, await RunFor(8));        // Qty>=8 → b only
    }
}
