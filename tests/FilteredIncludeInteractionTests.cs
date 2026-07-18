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
/// Hardens filtered Include against the interactions a reviewer would ask about: it must AND on top of a
/// registered global filter (a soft-deleted child stays hidden even if it matches the Include predicate),
/// and two filtered includes on one query must each bind their OWN closure value — the per-filter compiled
/// parameters must never cross-bind.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FilteredIncludeInteractionTests
{
    [Table("FiiOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
        public List<Note> Notes { get; set; } = new();
    }

    [Table("FiiLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public bool Active { get; set; }
        public bool IsDeleted { get; set; }
        public string Sku { get; set; } = "";
    }

    [Table("FiiNote")]
    public class Note
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Text { get; set; } = "";
    }

    private static DbContext Bootstrap(SqliteConnection cn, bool softDelete)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FiiOrder (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
                CREATE TABLE FiiLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Active INTEGER NOT NULL, IsDeleted INTEGER NOT NULL, Sku TEXT NOT NULL);
                CREATE TABLE FiiNote (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Text TEXT NOT NULL);
                INSERT INTO FiiOrder VALUES (1, 'o1');
                INSERT INTO FiiLine VALUES (1, 1, 1, 0, 'a'), (2, 1, 1, 1, 'b'), (3, 1, 0, 0, 'c');
                INSERT INTO FiiNote VALUES (1, 1, 'x'), (2, 1, 'y');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Note>().HasKey(n => n.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
                mb.Entity<Order>().HasMany(o => o.Notes).WithOne().HasForeignKey(n => n.OrderId, o => o.Id);
            }
        };
        if (softDelete)
            opts.AddGlobalFilter<Line>(l => !l.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Filtered_include_ands_on_top_of_a_soft_delete_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn, softDelete: true);

        var order = ctx.Query<Order>()
            .Include(o => o.Lines.Where(l => l.Active))
            .Single();

        // Line 1: active + not deleted → kept. Line 2: active but soft-deleted → hidden by the global
        // filter. Line 3: not deleted but not active → removed by the Include predicate.
        Assert.Equal(new[] { "a" }, order.Lines.OrderBy(l => l.Id).Select(l => l.Sku).ToArray());
    }

    [Fact]
    public void Two_filtered_includes_each_bind_their_own_closure()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn, softDelete: false);

        var wantedSku = "a";
        var wantedNote = "y";

        var order = ctx.Query<Order>()
            .Include(o => o.Lines.Where(l => l.Sku == wantedSku))
            .Include(o => o.Notes.Where(n => n.Text == wantedNote))
            .Single();

        // Each collection filtered by its OWN closure — a crossed @cp binding would swap or empty these.
        Assert.Equal(new[] { "a" }, order.Lines.Select(l => l.Sku).ToArray());
        Assert.Equal(new[] { "y" }, order.Notes.Select(n => n.Text).ToArray());
    }

    [Fact]
    public void Two_filtered_includes_rebind_on_cache_hit_with_swapped_values()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn, softDelete: false);

        (string[] lines, string[] notes) Run(string sku, string note)
        {
            var o = ctx.Query<Order>()
                .Include(x => x.Lines.Where(l => l.Sku == sku))
                .Include(x => x.Notes.Where(n => n.Text == note))
                .Single();
            return (o.Lines.OrderBy(l => l.Id).Select(l => l.Sku).ToArray(),
                    o.Notes.OrderBy(n => n.Id).Select(n => n.Text).ToArray());
        }

        var first = Run("a", "x");
        var second = Run("c", "y");   // same plan shape, different closures — must re-bind, not replay

        Assert.Equal(new[] { "a" }, first.lines);
        Assert.Equal(new[] { "x" }, first.notes);
        Assert.Equal(new[] { "c" }, second.lines);
        Assert.Equal(new[] { "y" }, second.notes);
    }
}
