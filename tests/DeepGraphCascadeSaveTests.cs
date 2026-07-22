using System;
using System.Collections.Generic;
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
/// A 3-level object graph (Order -> Lines -> Allocs) saved via Add + SaveChanges cascades inserts to every
/// level with correct foreign keys, and a deep modification of the loaded graph — editing a grandchild,
/// adding a grandchild, adding a child (with its own grandchild), and removing a child (which cascades to
/// its grandchildren) — all persist correctly. Keys are store-generated ([DatabaseGenerated(Identity)]) so
/// unset default-0 keys don't collide in the change tracker. Regression guard for deep-graph write integrity.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class DeepGraphCascadeSaveTests
{
    [Table("DgcOrder")]
    public sealed class Order
    {
        [System.ComponentModel.DataAnnotations.Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("DgcLine")]
    public sealed class Line
    {
        [System.ComponentModel.DataAnnotations.Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public Order Order { get; set; } = default!;
        public List<Alloc> Allocs { get; set; } = new();
    }

    [Table("DgcAlloc")]
    public sealed class Alloc
    {
        [System.ComponentModel.DataAnnotations.Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int LineId { get; set; }
        public int Qty { get; set; }
        public Line Line { get; set; } = default!;
    }

    private static SqliteConnection CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE DgcOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE DgcLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);" +
            "CREATE TABLE DgcAlloc (Id INTEGER PRIMARY KEY AUTOINCREMENT, LineId INTEGER NOT NULL, Qty INTEGER NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext Ctx(SqliteConnection cn) => new DbContext(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Alloc>().HasKey(a => a.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne(l => l.Order).HasForeignKey(l => l.OrderId, o => o.Id);
            mb.Entity<Line>().HasMany(l => l.Allocs).WithOne(a => a.Line).HasForeignKey(a => a.LineId, l => l.Id);
        }
    });

    private static string Fmt(Order o) =>
        o.Name + "[" + string.Join(",", o.Lines.OrderBy(l => l.Sku).Select(l =>
            l.Sku + "(" + string.Join("+", l.Allocs.OrderBy(a => a.Qty).Select(a => a.Qty)) + ")")) + "]";

    [Fact]
    public async Task Three_level_graph_cascade_inserts_all_levels()
    {
        using var cn = CreateDb();
        var order = new Order
        {
            Name = "O1",
            Lines =
            {
                new Line { Sku = "A", Allocs = { new Alloc { Qty = 1 }, new Alloc { Qty = 2 } } },
                new Line { Sku = "B", Allocs = { new Alloc { Qty = 3 } } },
            }
        };
        var ctx = Ctx(cn);
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        var reloaded = Ctx(cn).Query<Order>().Include(o => o.Lines).ThenInclude((Line l) => l.Allocs).Single();
        Assert.Equal("O1[A(1+2),B(3)]", Fmt(reloaded));
    }

    [Fact]
    public async Task Deep_modification_persists_edits_adds_and_cascade_removes()
    {
        using var cn = CreateDb();
        var ctx0 = Ctx(cn);
        ctx0.Add(new Order
        {
            Name = "O1",
            Lines =
            {
                new Line { Sku = "A", Allocs = { new Alloc { Qty = 1 }, new Alloc { Qty = 2 } } },
                new Line { Sku = "B", Allocs = { new Alloc { Qty = 3 } } },
            }
        });
        await ctx0.SaveChangesAsync();

        var ctx = Ctx(cn);
        var o = ctx.Query<Order>().Include(x => x.Lines).ThenInclude((Line l) => l.Allocs).Single();
        var lineA = o.Lines.Single(l => l.Sku == "A");
        lineA.Allocs.Single(a => a.Qty == 1).Qty = 99;              // edit grandchild
        lineA.Allocs.Add(new Alloc { Qty = 7 });                     // add grandchild
        o.Lines.Add(new Line { Sku = "C", Allocs = { new Alloc { Qty = 5 } } }); // add child + grandchild
        o.Lines.Remove(o.Lines.Single(l => l.Sku == "B"));           // remove child, cascade its grandchildren
        await ctx.SaveChangesAsync();

        var reloaded = Ctx(cn).Query<Order>().Include(x => x.Lines).ThenInclude((Line l) => l.Allocs).Single();
        Assert.Equal("O1[A(2+7+99),C(5)]", Fmt(reloaded));

        // The removed line's grandchildren must be gone (no orphaned allocs).
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM DgcAlloc WHERE Qty = 3";
        Assert.Equal(0L, check.ExecuteScalar());
    }
}
